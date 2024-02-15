use crate::config::FSStorageEngineConfig;
use crate::error::{Error, Result};
use crate::file_shard::FileShard;
use crate::hash_ring::HashRing;
use crate::utils::{bytes_to_key, key_to_bytes};
use crate::IrohDoc;
use futures::StreamExt;
use iroh::bytes::Hash;
use iroh::client::{Entry, LiveEvent};
use iroh::sync::store::Query;
use iroh::sync::{AuthorId, ContentStatus};
use lru::LruCache;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncRead;
use tokio_task_pool::Pool;
use tokio_util::bytes;
use tracing::{error, info, info_span, warn, Instrument};

#[derive(Clone)]
pub struct FSStorageEngine {
    author_id: AuthorId,
    iroh_doc: IrohDoc,
    hash_ring: HashRing,
    fs_shards: HashMap<String, FileShard>,
    replicas: u8,
}

impl FSStorageEngine {
    pub async fn new(
        author_id: AuthorId,
        iroh_doc: IrohDoc,
        fs_storage_config: FSStorageEngineConfig,
    ) -> Result<Self> {
        let mut fs_shards = HashMap::new();
        for fs_shard in &fs_storage_config.fs_shards {
            fs_shards.insert(fs_shard.name.clone(), FileShard::new(&fs_shard.path).await?);
        }
        let fs_storage = FSStorageEngine {
            author_id,
            iroh_doc: iroh_doc.clone(),
            hash_ring: HashRing::with_hasher(fs_storage_config.fs_shards.iter()),
            fs_shards,
            replicas: fs_storage_config.replicas,
        };
        let fs_storage_clone = fs_storage.clone();
        tokio::spawn({
            async move {
                let mut stream = fs_storage_clone.iroh_doc().subscribe().await.unwrap();
                let mut wait_list = LruCache::new(NonZeroUsize::new(1024).expect("not possible"));
                info!("started");
                while let Some(event) = stream.next().await {
                    let event = event.unwrap();
                    match &event {
                        LiveEvent::InsertRemote {
                            entry,
                            content_status,
                            ..
                        } => {
                            info!(event = ?event);
                            match content_status {
                                ContentStatus::Complete => {
                                    fs_storage_clone.process_remote_entry(entry).await?;
                                }
                                ContentStatus::Missing => {
                                    if entry.content_len() > 0 {
                                        wait_list.put(entry.content_hash(), entry.clone());
                                    } else {
                                        fs_storage_clone.process_remote_entry(entry).await?;
                                    }
                                }
                                ContentStatus::Incomplete => {
                                    wait_list.put(entry.content_hash(), entry.clone());
                                }
                            };
                        }
                        LiveEvent::ContentReady { hash } => {
                            info!(event = ?event);
                            let Some(hash) = &wait_list.pop(hash) else {
                                warn!(action = "skipped_absent_hash", hash = ?hash);
                                continue;
                            };
                            fs_storage_clone.process_remote_entry(hash).await?;
                        }
                        _ => {}
                    };
                }
                Ok::<(), Error>(())
            }
            .instrument(info_span!(parent: None, "fs_sync", table_id = iroh_doc.id().to_string()))
        });

        let fs_storage_clone = fs_storage.clone();

        if fs_storage_config.is_import_missing_enabled {
            tokio::spawn(async move {
                let all_keys: Arc<HashSet<_>> = Arc::new(
                    fs_storage_clone
                        .iroh_doc()
                        .get_many(Query::all())
                        .await
                        .map_err(Error::doc)?
                        .map(|x| bytes::Bytes::copy_from_slice(x.unwrap().key()))
                        .collect()
                        .await,
                );
                let pool = Arc::new(Pool::bounded(16));

                for fs_shard in fs_storage_clone.fs_shards.values() {
                    let fs_storage_clone = fs_storage_clone.clone();
                    let fs_shard = fs_shard.clone();
                    let all_keys = all_keys.clone();
                    let pool = pool.clone();
                    tokio::spawn(async move {
                        let base_path = fs_shard.path().to_path_buf();
                        let mut read_dir_stream = tokio::fs::read_dir(&base_path)
                            .await
                            .map_err(Error::io_error)?;
                        while let Some(entry) = read_dir_stream
                            .next_entry()
                            .await
                            .map_err(Error::io_error)?
                        {
                            let key = key_to_bytes(&entry.file_name().to_string_lossy());
                            if all_keys.contains(&key) || key.starts_with(&[b'~']) {
                                continue;
                            }
                            pool.spawn({
                                let iroh_doc = fs_storage_clone.iroh_doc().clone();
                                async move {
                                    let import_progress = iroh_doc
                                        .import_file(
                                            fs_storage_clone.author_id,
                                            key,
                                            &entry.path(),
                                            true,
                                        )
                                        .await
                                        .map_err(Error::doc)
                                        .unwrap();
                                    match import_progress.finish().await.map_err(Error::hash) {
                                        Err(error) => error!(error = ?error, path = ?entry.path(), key = ?entry.file_name(), "import_progress_error"),
                                        _ => {}
                                    };
                                    info!(action = "imported", key = ?entry.file_name())
                                }
                                    .instrument(info_span!(parent: None, "restore"))
                            })
                                .await
                                .unwrap();
                        }
                        Ok::<(), Error>(())
                    });
                }
                Ok::<(), Error>(())
            });
        }
        Ok(fs_storage)
    }

    fn get_path(&self, key: &str) -> Result<PathBuf> {
        if let Some(file_shard_config) = self.hash_ring.range(key, 1).into_iter().next() {
            let file_shard = &self.fs_shards[&file_shard_config.name];
            return Ok(file_shard.get_path_for(key));
        }
        Err(Error::storage("missing file shards"))
    }

    async fn process_remote_entry(&self, entry: &Entry) -> Result<()> {
        let key = std::str::from_utf8(bytes_to_key(entry.key())).unwrap();
        if entry.content_len() == 0 {
            self.delete_from_fs(key).await?;
        } else {
            let file_shard_path = self.get_path(key).unwrap();
            self.iroh_doc()
                .export_file(entry.clone(), file_shard_path)
                .await
                .map_err(Error::storage)?
                .finish()
                .await
                .map_err(Error::storage)?;
        }
        Ok(())
    }

    pub async fn delete_from_fs(&self, key: &str) -> Result<()> {
        info!("delete_from_fs {:?} {:?}", self.iroh_doc().id(), key);
        if let Some(file_shard_config) = self.hash_ring.range(key, 1).into_iter().next() {
            self.fs_shards[&file_shard_config.name]
                .delete(key)
                .await
                .map_err(Error::io_error)?;
            return Ok(());
        }
        Err(Error::storage("no file shards"))
    }

    pub async fn delete(&self, key: &str) -> Result<usize> {
        info!("delete {:?} {:?}", self.iroh_doc().id(), key);
        let removed_items = self
            .iroh_doc
            .del(self.author_id, key_to_bytes(key))
            .await
            .map_err(Error::missing_key)?;
        Ok(removed_items)
    }

    pub async fn insert<S: AsyncRead + Send + Unpin>(&self, key: &str, value: S) -> Result<Hash> {
        if let Some(file_shard_config) = self.hash_ring.range(key, 1).into_iter().next() {
            let file_shard = &self.fs_shards[&file_shard_config.name];
            let data_path = file_shard
                .insert(key, value)
                .await
                .map_err(Error::io_error)?;
            let import_progress = self
                .iroh_doc
                .import_file(self.author_id, key_to_bytes(key), &data_path, true)
                .await
                .map_err(Error::doc)?;
            return Ok(import_progress.finish().await.map_err(Error::hash)?.hash);
        }
        Err(Error::FileShard {
            description: "missing file shards".to_string(),
        })
    }

    pub async fn exists(&self, key: &str) -> Result<Option<PathBuf>> {
        for file_shard_config in self.hash_ring.range(key, 1) {
            let file_shard = &self.fs_shards[&file_shard_config.name];
            if file_shard
                .exists(key)
                .await
                .map_err(Error::io_error)?
                .is_some()
            {
                return Ok(Some(file_shard.get_path_for(key)));
            }
        }
        Ok(None)
    }

    pub fn iroh_doc(&self) -> &IrohDoc {
        &self.iroh_doc
    }

    pub async fn get(&self, key: &str) -> Result<Box<dyn AsyncRead + Unpin + Send>> {
        for file_shard_config in self.hash_ring.range(key, 1) {
            let file_shard = &self.fs_shards[&file_shard_config.name];
            match file_shard.open_store(key).await {
                Ok(Some(file)) => return Ok(Box::new(file)),
                Ok(None) => return Err(Error::io_error("missing file")),
                Err(e) => return Err(Error::io_error(e)),
            }
        }
        Err(Error::io_error("missing shard"))
    }
}
