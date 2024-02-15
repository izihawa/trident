use crate::config::StorageEngineConfig;
use crate::error::{Error, Result};
use crate::file_shard::FileShard;
use crate::hash_ring::HashRing;
use crate::sinks::Sink;
use crate::utils::{bytes_to_key, key_to_bytes};
use crate::{IrohClient, IrohDoc};
use async_stream::stream;
use futures::{Stream, StreamExt};
use iroh::bytes::Hash;
use iroh::client::{Entry, LiveEvent};
use iroh::rpc_protocol::ShareMode;
use iroh::sync::store::Query;
use iroh::sync::{AuthorId, ContentStatus};
use iroh::ticket::DocTicket;
use lru::LruCache;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncRead;
use tokio_task_pool::Pool;
use tokio_util::bytes;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, info_span, warn, Instrument};

#[derive(Clone)]
pub struct Storage {
    author_id: AuthorId,
    iroh_doc: IrohDoc,
    sync_client: IrohClient,
    hash_ring: HashRing,
    fs_shards: HashMap<String, FileShard>,
    replicas: u8,
    sinks: Vec<Arc<dyn Sink>>,
    keep_blob: bool,
}

impl Storage {
    pub async fn new(
        table_name: &str,
        author_id: AuthorId,
        iroh_doc: IrohDoc,
        sync_client: IrohClient,
        storage_config: StorageEngineConfig,
        sinks: Vec<Arc<dyn Sink>>,
        keep_blob: bool,
        cancellation_token: CancellationToken,
        task_tracker: TaskTracker,
    ) -> Result<Self> {
        let mut fs_shards = HashMap::new();
        for fs_shard in &storage_config.fs_shards {
            fs_shards.insert(
                fs_shard.name.clone(),
                FileShard::new(&fs_shard.path.join(table_name)).await?,
            );
        }
        let storage = Storage {
            author_id,
            iroh_doc: iroh_doc.clone(),
            sync_client,
            hash_ring: HashRing::with_hasher(storage_config.fs_shards.iter()),
            fs_shards,
            replicas: storage_config.replicas,
            sinks,
            keep_blob,
        };
        let storage_clone = storage.clone();

        task_tracker.spawn({
            let cancellation_token = cancellation_token.clone();
            async move {
                let mut stream = storage_clone.iroh_doc().subscribe().await.unwrap();
                let mut wait_list = LruCache::new(NonZeroUsize::new(4096).expect("not possible"));
                info!("started");
                loop {
                    tokio::select! {
                        _ = cancellation_token.cancelled() => return Ok::<(), Error>(()),
                        event = stream.next() => {
                            if let Some(event) = event {
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
                                                storage_clone.process_remote_entry(entry).await?;
                                                storage_clone.process_sinks(entry).await;
                                            }
                                            ContentStatus::Missing => {
                                                if entry.content_len() > 0 {
                                                    wait_list.put(entry.content_hash(), entry.clone());
                                                } else {
                                                    storage_clone.process_remote_entry(entry).await?;
                                                }
                                            }
                                            ContentStatus::Incomplete => {
                                                wait_list.put(entry.content_hash(), entry.clone());
                                            }
                                        };
                                    }
                                    LiveEvent::InsertLocal { entry } => {
                                        storage_clone.process_sinks(entry).await;
                                    }
                                    LiveEvent::ContentReady { hash } => {
                                        info!(event = ?event);
                                        let Some(entry) = &wait_list.pop(hash) else {
                                            warn!(action = "skipped_absent_hash", hash = ?hash);
                                            continue;
                                        };
                                        storage_clone.process_remote_entry(entry).await?;
                                        storage_clone.process_sinks(entry).await;
                                        storage_clone.retain_blob_if_needed(entry).await;
                                    }
                                    _ => {}
                                };
                            } else {
                                return Ok::<(), Error>(())
                            }
                        }
                    }
                }
            }
            .instrument(info_span!(parent: None, "fs_sync", table_id = iroh_doc.id().to_string()))
        });

        let fs_storage_clone = storage.clone();

        if storage_config.is_import_missing_enabled {
            task_tracker.spawn(async move {
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
                    let cancellation_token = cancellation_token.clone();
                    tokio::spawn(async move {
                        let base_path = fs_shard.path().to_path_buf();
                        let mut read_dir_stream = tokio::fs::read_dir(&base_path)
                            .await
                            .map_err(Error::io_error)?;

                        loop {
                            tokio::select! {
                                _ = cancellation_token.cancelled() => return Ok::<(), Error>(()),
                                entry = read_dir_stream.next_entry() => {
                                    let entry = entry.map_err(Error::io_error)?;
                                    if let Some(entry) = entry {
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
                                                if let Err(error) = import_progress.finish().await.map_err(Error::hash) {
                                                    error!(error = ?error, path = ?entry.path(), key = ?entry.file_name(), "import_progress_error");
                                                }
                                                info!(action = "imported", key = ?entry.file_name())
                                            }
                                                .instrument(info_span!(parent: None, "restore"))
                                        })
                                        .await
                                        .unwrap();
                                    } else {
                                        return Ok::<(), Error>(())
                                    }
                                }
                            }
                        }
                    });
                }
                Ok::<(), Error>(())
            });
        }
        Ok(storage)
    }

    fn get_path(&self, key: &str) -> Result<PathBuf> {
        if let Some(file_shard_config) = self.hash_ring.range(key, 1).into_iter().next() {
            let file_shard = &self.fs_shards[&file_shard_config.name];
            return Ok(file_shard.get_path_for(key));
        }
        Err(Error::storage("missing file shards"))
    }

    async fn process_sinks(&self, entry: &Entry) {
        let key = std::str::from_utf8(bytes_to_key(entry.key())).unwrap();
        let file_shard_path = self.get_path(key).unwrap();
        for sink in &self.sinks {
            if let Err(error) = sink.send(entry.key(), &file_shard_path).await {
                warn!(error = ?error);
                continue;
            }
            info!(action = "send", sink = sink.name(), key = ?key);
        }
    }

    async fn process_remote_entry(&self, entry: &Entry) -> Result<Option<PathBuf>> {
        let key = std::str::from_utf8(bytes_to_key(entry.key())).unwrap();
        if entry.content_len() == 0 {
            self.delete_from_fs(key).await?;
            Ok(None)
        } else {
            let file_shard_path = self.get_path(key).unwrap();
            self.iroh_doc()
                .export_file(entry.clone(), file_shard_path.clone())
                .await
                .map_err(Error::storage)?
                .finish()
                .await
                .map_err(Error::storage)?;
            Ok(Some(file_shard_path))
        }
    }

    async fn retain_blob_if_needed(&self, entry: &Entry) {
        if !self.keep_blob {
            if let Err(error) = self
                .sync_client
                .blobs
                .delete_blob(entry.content_hash())
                .await
            {
                warn!(error = ?error);
            }
        }
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

    pub async fn get(&self, key: &str) -> Result<Option<Box<dyn AsyncRead + Unpin + Send>>> {
        match self
            .iroh_doc
            .get_one(Query::key_exact(key_to_bytes(key)))
            .await
            .map_err(Error::doc)?
        {
            Some(entry) => {
                return Ok(Some(Box::new(
                    entry
                        .content_reader(self.iroh_doc())
                        .await
                        .map_err(Error::doc)?,
                )))
            }
            None => {
                for file_shard_config in self.hash_ring.range(key, 1) {
                    let file_shard = &self.fs_shards[&file_shard_config.name];
                    return match file_shard.open_store(key).await {
                        Ok(Some(file)) => Ok(Some(Box::new(file))),
                        Ok(None) => Ok(None),
                        Err(e) => Err(Error::io_error(e)),
                    };
                }
            }
        }
        Err(Error::io_error("missing shard"))
    }

    pub fn get_all(&self) -> impl Stream<Item = Result<Entry>> {
        let iroh_doc = self.iroh_doc.clone();
        stream! {
            for await entry in iroh_doc.get_many(Query::all()).await.map_err(Error::table)? {
                yield entry.map_err(Error::entry)
            }
        }
    }
    pub async fn share(&self, mode: ShareMode) -> Result<DocTicket> {
        self.iroh_doc().share(mode).await.map_err(Error::storage)
    }

    pub async fn get_hash(&self, key: &str) -> Result<Option<(Hash, u64)>> {
        Ok(self
            .iroh_doc()
            .get_one(Query::key_exact(key_to_bytes(key)))
            .await
            .map_err(Error::missing_key)?
            .map(|entry| (entry.content_hash(), entry.content_len())))
    }

    pub async fn insert_hash(&self, key: &str, hash: Hash, size: u64) -> Result<()> {
        self.iroh_doc
            .set_hash(self.author_id, key_to_bytes(key), hash, size)
            .await
            .map_err(Error::hash)
    }
}
