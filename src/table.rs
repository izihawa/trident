use crate::config::{StorageEngineConfig, TableConfig};
use crate::error::{Error, Result};
use crate::file_shard::FileShard;
use crate::hash_ring::HashRing;
use crate::utils::key_to_bytes;
use crate::IrohDoc;
use async_stream::stream;
use futures::{Stream, StreamExt};
use iroh::bytes::store::fs::Store;
use iroh::bytes::store::{ExportMode, Map};
use iroh::bytes::Hash;
use iroh::client::{Entry, LiveEvent};
use iroh::net::key::PublicKey;
use iroh::net::NodeAddr;
use iroh::node::Node;
use iroh::rpc_protocol::{BlobDownloadRequest, SetTagOption, ShareMode};
use iroh::sync::store::{DownloadPolicy, Query, SortBy, SortDirection};
use iroh::sync::{AuthorId, ContentStatus, PeerIdBytes};
use iroh::ticket::DocTicket;
use iroh_base::hash::BlobFormat;
use lru::LruCache;
use rand::seq::SliceRandom;
use rand::thread_rng;
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
    hash_ring: HashRing,
    shards: HashMap<String, FileShard>,
    import_threads: u32,
}

impl Storage {
    pub async fn from_config(
        table_name: &str,
        storage_config: Option<StorageEngineConfig>,
    ) -> Result<Option<Storage>> {
        if let Some(storage_config) = storage_config {
            let mut shards = HashMap::new();
            for shard in &storage_config.shards {
                shards.insert(
                    shard.name.clone(),
                    FileShard::new(&shard.path.join(table_name)).await?,
                );
            }
            let hash_ring = HashRing::with_hasher(storage_config.shards.iter());
            return Ok(Some(Storage {
                hash_ring,
                shards,
                import_threads: storage_config.import_threads,
            }));
        }
        Ok(None)
    }

    fn get_path(&self, key: &str) -> Result<PathBuf> {
        if let Some(file_shard_config) = self.hash_ring.range(key, 1).into_iter().next() {
            let file_shard = &self.shards[&file_shard_config.name];
            return file_shard.get_path_for(key).map_err(Error::io_error);
        }
        Err(Error::FileShard {
            description: "missing file shards".to_string(),
        })
    }

    pub async fn insert<S: AsyncRead + Send + Unpin>(
        &self,
        key: &str,
        value: S,
    ) -> Result<PathBuf> {
        if let Some(file_shard_config) = self.hash_ring.range(key, 1).into_iter().next() {
            let file_shard = &self.shards[&file_shard_config.name];
            let data_path = file_shard
                .insert(key, value)
                .await
                .map_err(Error::io_error)?;
            return Ok(data_path);
        }
        Err(Error::FileShard {
            description: "missing file shards".to_string(),
        })
    }
}

#[derive(Clone)]
pub struct Table {
    author_id: AuthorId,
    iroh_doc: IrohDoc,
    table_config: TableConfig,
    cancellation_token: CancellationToken,
    task_tracker: TaskTracker,
    node: Node<Store>,
    storage: Option<Storage>,
}

impl Table {
    pub async fn new(
        table_name: &str,
        author_id: AuthorId,
        node: Node<Store>,
        iroh_doc: IrohDoc,
        storage_config: Option<StorageEngineConfig>,
        table_config: TableConfig,
        cancellation_token: CancellationToken,
        task_tracker: TaskTracker,
    ) -> Result<Self> {
        let storage = Storage::from_config(table_name, storage_config).await?;
        let table = Table {
            author_id,
            node,
            iroh_doc: iroh_doc.clone(),
            storage: storage.clone(),
            table_config,
            cancellation_token: cancellation_token.clone(),
            task_tracker: task_tracker.clone(),
        };
        let table0 = table.clone();
        let mut stream = table0
            .iroh_doc()
            .subscribe()
            .await
            .map_err(Error::storage)?;

        if let Some(storage) = storage {
            task_tracker.spawn({
                let cancellation_token = cancellation_token.clone();
                async move {
                    let mut wait_list = LruCache::new(NonZeroUsize::new(4 * 1024 * 1024).expect("not possible"));
                    info!("started");
                    loop {
                        tokio::select! {
                        biased;
                        _ = cancellation_token.cancelled() => {
                            info!("cancel");
                            return Ok::<(), Error>(())
                        },
                        event = stream.next() => {
                            let event = match event {
                                Some(Ok(event)) => event,
                                Some(Err(error)) => {
                                    warn!(error = ?error);
                                    continue;
                                }
                                None => return Ok::<(), Error>(()),
                            };
                            match &event {
                                LiveEvent::InsertRemote {
                                    entry,
                                    content_status,
                                    ..
                                } => {
                                    let key = std::str::from_utf8(entry.key()).map_err(Error::incorrect_key)?;
                                    info!(event = ?event);
                                    match content_status {
                                        ContentStatus::Complete => {
                                            table0.process_remote_entry(key, entry).await?;
                                        }
                                        ContentStatus::Missing | ContentStatus::Incomplete => {
                                            wait_list.put(entry.content_hash(), entry.clone());
                                        }
                                    };
                                }
                                LiveEvent::ContentReady { hash } => {
                                    info!(event = ?event);
                                    let Some(entry) = &wait_list.pop(hash) else {
                                        warn!(action = "skipped_absent_hash", hash = ?hash);
                                        continue;
                                    };
                                    let key = std::str::from_utf8(entry.key()).map_err(Error::incorrect_key)?;
                                    table0.process_remote_entry(key, entry).await?;
                                }
                                _ => {}
                            };
                        }
                    }
                    }
                }
                    .instrument(info_span!(parent: None, "fs_sync", table_id = iroh_doc.id().to_string()))
            });
            if storage.import_threads > 0 {
                let import_threads_task_tracker = TaskTracker::new();
                let task_tracker0 = task_tracker.clone();
                task_tracker.spawn(async move {
                    let all_keys: Arc<HashSet<_>> = Arc::new(
                        iroh_doc
                            .get_many(Query::all())
                            .await
                            .map_err(Error::doc)?
                            .map(|x| bytes::Bytes::copy_from_slice(x.unwrap().key()))
                            .collect()
                            .await,
                    );
                    let pool = Arc::new(Pool::bounded(storage.import_threads as usize));

                    for shard in storage.shards.values() {
                        let shard = shard.clone();
                        let shard_path = shard.path().to_path_buf();
                        let all_keys = all_keys.clone();
                        let pool = pool.clone();
                        let cancellation_token = cancellation_token.clone();
                        let import_threads_task_tracker0 = import_threads_task_tracker.clone();
                        let iroh_doc = iroh_doc.clone();
                        task_tracker0.spawn(async move {
                            let base_path = shard.path().to_path_buf();
                            let mut read_dir_stream = tokio::fs::read_dir(&base_path)
                                .await
                                .map_err(Error::io_error)?;

                            loop {
                                tokio::select! {
                                biased;
                                _ = cancellation_token.cancelled() => {
                                    info!("cancel");
                                    import_threads_task_tracker0.close();
                                    import_threads_task_tracker0.wait().await;
                                    return Ok::<(), Error>(())
                                },
                                entry = read_dir_stream.next_entry() => {
                                    let entry = entry.map_err(Error::io_error)?;
                                    if let Some(entry) = entry {
                                        let key = key_to_bytes(&entry.file_name().to_string_lossy());
                                        if all_keys.contains(&key) || key.starts_with(&[b'~']) {
                                            continue;
                                        }
                                        let import_threads_task_tracker0 = import_threads_task_tracker0.clone();
                                        let base_path = base_path.clone();
                                        let iroh_doc = iroh_doc.clone();
                                        pool.spawn(async move {
                                            if import_threads_task_tracker0.is_closed() {
                                                return
                                            }
                                            let join_handle = import_threads_task_tracker0.spawn(async move {
                                                let iroh_doc = iroh_doc.clone();
                                                let import_progress = match iroh_doc
                                                    .import_file(
                                                        table.author_id,
                                                        key,
                                                        &entry.path(),
                                                        true,
                                                    )
                                                    .await
                                                    .map_err(Error::doc) {
                                                    Ok(import_progress) => import_progress,
                                                    Err(error) => {
                                                        error!(error = ?error, path = ?entry.path(), key = ?entry.file_name(), "import_progress_error");
                                                        return;
                                                    }
                                                };
                                                if let Err(error) = import_progress.finish().await.map_err(Error::storage) {
                                                    error!(
                                                        error = ?error,
                                                        path = ?entry.path(),
                                                        key = ?entry.file_name(),
                                                        "import_progress_error"
                                                    );
                                                }
                                                info!(action = "imported", key = ?entry.file_name())
                                            }.instrument(info_span!(parent: None, "import_missing", shard = ?base_path)));
                                            if let Err(error) = join_handle.await {
                                                error!(
                                                    error = ?error,
                                                    "join_import_threads"
                                                );
                                            }
                                        })
                                        .await
                                        .map_err(Error::io_error)?;
                                    } else {
                                        return Ok::<(), Error>(())
                                    }
                                }
                            }
                            }
                        }.instrument(info_span!(parent: None, "import_missing", shard = ?shard_path)));
                    }
                    Ok::<(), Error>(())
                });
            }
        }
        Ok(table)
    }

    async fn process_remote_entry(&self, key: &str, entry: &Entry) -> Result<Option<PathBuf>> {
        let Some(storage) = &self.storage else {
            return Ok(None);
        };
        info!(action = "process_remote_entry", entry = ?entry);
        let shard_path = storage.get_path(key)?;
        if entry.content_len() > 0 {
            self.iroh_doc()
                .export_file(entry.clone(), shard_path.clone(), ExportMode::TryReference)
                .await
                .map_err(Error::storage)?
                .finish()
                .await
                .map_err(Error::storage)?;
            return Ok(Some(shard_path));
        }
        Ok(None)
    }

    async fn download_entry_from_peers(&self, entry: &Entry, peers: &[PeerIdBytes]) -> Result<()> {
        let key = std::str::from_utf8(entry.key()).map_err(Error::incorrect_key)?;
        for peer in peers {
            match PublicKey::from_bytes(peer) {
                Ok(public_key) => {
                    let progress = match self
                        .node
                        .blobs
                        .download(BlobDownloadRequest {
                            hash: entry.content_hash(),
                            format: BlobFormat::Raw,
                            peer: NodeAddr::new(public_key),
                            tag: SetTagOption::Auto,
                        })
                        .await
                        .map_err(Error::io_error)
                    {
                        Ok(progress) => progress,
                        Err(error) => {
                            warn!(error = ?error);
                            continue;
                        }
                    };
                    match progress.finish().await {
                        Ok(import_result) => {
                            info!(
                                "found local_size {}, downloaded_size {}, content_len {}",
                                import_result.local_size,
                                import_result.downloaded_size,
                                entry.content_len()
                            );
                            if import_result.local_size + import_result.downloaded_size
                                == entry.content_len()
                            {
                                self.process_remote_entry(key, entry).await?;
                                return Ok(());
                            }
                        }
                        Err(error) => {
                            warn!(error = ?error);
                            continue;
                        }
                    }
                }
                Err(_signing_error) => {
                    warn!("potential db corruption: peers per doc can't be decoded");
                    continue;
                }
            };
        }
        Err(Error::failed_download(key))
    }

    pub async fn download_missing(
        &self,
        download_policy: Option<DownloadPolicy>,
        threads: u32,
    ) -> Result<()> {
        let Ok(Some(peers)) = self.iroh_doc.get_sync_peers().await else {
            return Ok(());
        };
        let download_policy =
            download_policy.unwrap_or_else(|| self.table_config.download_policy.clone());
        let all_entries: Vec<_> = self
            .iroh_doc
            .get_many(Query::all())
            .await
            .map_err(Error::doc)?
            .map(|x| x.unwrap())
            .collect()
            .await;
        let pool = Arc::new(Pool::bounded(threads as usize));

        for entry in all_entries {
            if self.cancellation_token.is_cancelled() {
                info!("cancel");
                break;
            }
            if match &download_policy {
                DownloadPolicy::NothingExcept(patterns) => {
                    patterns.iter().any(|pattern| pattern.matches(entry.key()))
                }
                DownloadPolicy::EverythingExcept(patterns) => {
                    patterns.iter().all(|pattern| !pattern.matches(entry.key()))
                }
            } {
                let storage0 = self.clone();
                let mut peers0 = peers.clone();
                peers0.shuffle(&mut thread_rng());
                let join_handle = pool
                    .spawn(async move {
                        if let Err(error) =
                            storage0.download_entry_from_peers(&entry, &peers0).await
                        {
                            warn!(error = ?error);
                        }
                    })
                    .await
                    .map_err(Error::io_error)?;
                let _ = self.task_tracker.track_future(join_handle);
            }
        }
        Ok(())
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

    pub async fn insert<S: AsyncRead + Send + Unpin>(
        &self,
        key: &str,
        mut value: S,
    ) -> Result<Hash> {
        match &self.storage {
            Some(storage) => {
                let data_path = storage.insert(key, value).await?;
                let import_progress = self
                    .iroh_doc
                    .import_file(self.author_id, key_to_bytes(key), &data_path, true)
                    .await
                    .map_err(Error::doc)?;
                Ok(import_progress.finish().await.map_err(Error::storage)?.hash)
            }
            None => {
                let mut buffer = Vec::new();
                tokio::io::copy(&mut value, &mut buffer)
                    .await
                    .map_err(Error::io_error)?;
                Ok(self
                    .iroh_doc
                    .set_bytes(self.author_id, key_to_bytes(key), buffer)
                    .await
                    .map_err(Error::doc)?)
            }
        }
    }

    pub fn iroh_doc(&self) -> &IrohDoc {
        &self.iroh_doc
    }

    pub async fn get(&self, key: &str) -> Result<Option<(Box<dyn AsyncRead + Unpin + Send>, u64)>> {
        let entry = self
            .iroh_doc
            .get_one(
                Query::key_exact(key_to_bytes(key)).sort_by(SortBy::KeyAuthor, SortDirection::Asc),
            )
            .await
            .map_err(Error::entry)?;
        if let Some(entry) = entry {
            if let Ok(Some(db_entry)) = self.node.db().get(&entry.content_hash()).await {
                if !db_entry.is_complete() {
                    if let Ok(Some(mut peers)) = self.iroh_doc.get_sync_peers().await {
                        peers.shuffle(&mut thread_rng());
                        self.download_entry_from_peers(&entry, &peers).await?;
                    }
                }
                return Ok(Some((
                    Box::new(
                        entry
                            .content_reader(self.node.client())
                            .await
                            .map_err(Error::storage)?,
                    ),
                    entry.content_len(),
                )));
            }
        }
        Ok(None)
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
            .get_one(
                Query::key_exact(key_to_bytes(key)).sort_by(SortBy::KeyAuthor, SortDirection::Asc),
            )
            .await
            .map_err(Error::missing_key)?
            .map(|entry| (entry.content_hash(), entry.content_len())))
    }

    pub async fn insert_hash(&self, key: &str, hash: Hash, size: u64) -> Result<()> {
        self.iroh_doc
            .set_hash(self.author_id, key_to_bytes(key), hash, size)
            .await
            .map_err(Error::storage)
    }
}
