use crate::config::{StorageEngineConfig, TableConfig};
use crate::error::{Error, Result};
use crate::file_shard::FileShard;
use crate::hash_ring::HashRing;
use crate::sinks::Sink;
use crate::utils::key_to_bytes;
use crate::{IrohClient, IrohDoc};
use async_stream::stream;
use futures::{Stream, StreamExt, TryStreamExt};
use iroh::bytes::store::ExportMode;
use iroh::bytes::Hash;
use iroh::client::{Entry, LiveEvent};
use iroh::net::key::PublicKey;
use iroh::net::NodeAddr;
use iroh::rpc_protocol::{BlobDownloadRequest, DownloadLocation, SetTagOption, ShareMode};
use iroh::sync::store::{DownloadPolicy, Query, SortBy, SortDirection};
use iroh::sync::{AuthorId, ContentStatus, PeerIdBytes};
use iroh::ticket::DocTicket;
use iroh_base::hash::BlobFormat;
use lru::LruCache;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::os::unix::fs::MetadataExt;
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
    shards: HashMap<String, FileShard>,
    sinks: Vec<Arc<dyn Sink>>,
    table_config: TableConfig,
    cancellation_token: CancellationToken,
}

impl Storage {
    pub async fn new(
        table_name: &str,
        author_id: AuthorId,
        iroh_doc: IrohDoc,
        sync_client: IrohClient,
        storage_config: StorageEngineConfig,
        sinks: Vec<Arc<dyn Sink>>,
        table_config: TableConfig,
        cancellation_token: CancellationToken,
        task_tracker: TaskTracker,
    ) -> Result<Self> {
        let mut shards = HashMap::new();
        for shard in &storage_config.shards {
            shards.insert(
                shard.name.clone(),
                FileShard::new(&shard.path.join(table_name)).await?,
            );
        }
        let storage = Storage {
            author_id,
            iroh_doc: iroh_doc.clone(),
            sync_client,
            hash_ring: HashRing::with_hasher(storage_config.shards.iter()),
            shards,
            sinks,
            table_config,
            cancellation_token: cancellation_token.clone(),
        };
        let storage_clone = storage.clone();
        let mut stream = storage_clone
            .iroh_doc()
            .subscribe()
            .await
            .map_err(Error::storage)?;

        task_tracker.spawn({
            let cancellation_token = cancellation_token.clone();
            async move {
                let mut wait_list = LruCache::new(NonZeroUsize::new(4096).expect("not possible"));
                info!("started");
                loop {
                    tokio::select! {
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
                                                storage_clone.process_remote_entry(key, entry).await?;
                                                storage_clone.process_sinks(key).await?;
                                            }
                                            ContentStatus::Missing | ContentStatus::Incomplete => {
                                                wait_list.put(entry.content_hash(), entry.clone());
                                            }
                                        };
                                    }
                                    LiveEvent::InsertLocal { entry } => {
                                        let key = std::str::from_utf8(entry.key()).map_err(Error::incorrect_key)?;
                                        storage_clone.process_sinks(key).await?;
                                    }
                                    LiveEvent::ContentReady { hash } => {
                                        info!(event = ?event);
                                        let Some(entry) = &wait_list.pop(hash) else {
                                            warn!(action = "skipped_absent_hash", hash = ?hash);
                                            continue;
                                        };
                                        let key = std::str::from_utf8(entry.key()).map_err(Error::incorrect_key)?;
                                        storage_clone.process_remote_entry(key, entry).await?;
                                        storage_clone.process_sinks(key).await?;
                                        storage_clone.retain_blob_if_needed(key, entry.content_hash()).await?;
                                    }
                                    _ => {}
                                };
                        }
                    }
                }
            }
            .instrument(info_span!(parent: None, "fs_sync", table_id = iroh_doc.id().to_string()))
        });

        let storage_clone = storage.clone();

        if storage_config.import_threads > 0 {
            let task_tracker0 = task_tracker.clone();
            task_tracker.spawn(async move {
                let all_keys: Arc<HashSet<_>> = Arc::new(
                    storage_clone
                        .iroh_doc()
                        .get_many(Query::all())
                        .await
                        .map_err(Error::doc)?
                        .map(|x| bytes::Bytes::copy_from_slice(x.unwrap().key()))
                        .collect()
                        .await,
                );
                let pool = Arc::new(Pool::bounded(storage_config.import_threads as usize));

                for shard in storage_clone.shards.values() {
                    let storage_clone = storage_clone.clone();
                    let shard = shard.clone();
                    let all_keys = all_keys.clone();
                    let pool = pool.clone();
                    let cancellation_token = cancellation_token.clone();
                    task_tracker0.spawn(async move {
                        let base_path = shard.path().to_path_buf();
                        let mut read_dir_stream = tokio::fs::read_dir(&base_path)
                            .await
                            .map_err(Error::io_error)?;

                        loop {
                            tokio::select! {
                                _ = cancellation_token.cancelled() => {
                                    info!("cancel");
                                    return Ok::<(), Error>(())
                                },
                                entry = read_dir_stream.next_entry() => {
                                    let entry = entry.map_err(Error::io_error)?;
                                    if let Some(entry) = entry {
                                        let key = key_to_bytes(&entry.file_name().to_string_lossy());
                                        if all_keys.contains(&key) || key.starts_with(&[b'~']) {
                                            continue;
                                        }
                                        pool.spawn({
                                            let iroh_doc = storage_clone.iroh_doc().clone();
                                            async move {
                                                let import_progress = match iroh_doc
                                                    .import_file(
                                                        storage_clone.author_id,
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
                                                    error!(error = ?error, path = ?entry.path(), key = ?entry.file_name(), "import_progress_error");
                                                }
                                                info!(action = "imported", key = ?entry.file_name())
                                            }
                                                .instrument(info_span!(parent: None, "restore"))
                                        })
                                        .await
                                        .map_err(Error::io_error)?;
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

    pub async fn check_integrity(&self) {
        let mut stream = self.iroh_doc.get_many(Query::all()).await.unwrap();
        loop {
            tokio::select! {
                biased;
                _ = self.cancellation_token.cancelled() => {
                    info!("cancel");
                    break;
                },
                entry = stream.try_next() => {
                    match entry.unwrap() {
                        Some(entry) => {
                            let key = match std::str::from_utf8(entry.key()).map_err(Error::incorrect_key) {
                                Ok(key) => key,
                                Err(error) => {
                                    error!(error = ?error);
                                    break;
                                }
                            };
                            if self.get_path(key).metadata().map(|x| x.len()).unwrap_or(0) == 0 {
                                if let Err(error) = self.sync_client
                                    .blobs
                                    .delete_blob(entry.content_hash())
                                    .await
                                    .map_err(Error::entry) {
                                    error!(error = ?error);
                                }
                                if let Err(error) = self.delete_from_fs(key).await {
                                    error!(error = ?error);
                                }
                            }
                        }
                        None => break,
                    }

                }
            }
        }
    }

    async fn download_entry_from_peers(&self, entry: &Entry, peers: &[PeerIdBytes]) -> Result<()> {
        let key = std::str::from_utf8(entry.key()).map_err(Error::incorrect_key)?;
        for peer in peers {
            let node_addr = match PublicKey::from_bytes(&peer) {
                Ok(public_key) => NodeAddr::new(public_key),
                Err(_signing_error) => {
                    warn!("potential db corruption: peers per doc can't be decoded");
                    continue;
                }
            };
            let progress = self
                .sync_client
                .blobs
                .download(BlobDownloadRequest {
                    hash: entry.content_hash(),
                    format: BlobFormat::Raw,
                    peer: node_addr,
                    tag: SetTagOption::Auto,
                    out: DownloadLocation::External {
                        path: self.get_path(key),
                        in_place: true,
                    },
                })
                .await
                .map_err(Error::io_error)?;
            match progress.finish().await.map_err(Error::storage) {
                Ok(import_result) => if import_result.local_size + import_result.downloaded_size == entry.content_len() {
                    return Ok(())
                }
                Err(_) => continue,
            }
        }
        Err(Error::missing_key(key))
    }

    pub async fn download_missing(&self, download_policy: DownloadPolicy) -> Result<()> {
        let Ok(Some(peers)) = self.iroh_doc.get_sync_peers().await else {
            return Ok(());
        };
        let mut stream = self.iroh_doc.get_many(Query::all()).await.unwrap();
        loop {
            tokio::select! {
                biased;
                _ = self.cancellation_token.cancelled() => {
                    info!("cancel");
                    break;
                },
                entry = stream.try_next() => {
                    match entry.unwrap() {
                        Some(entry) => {
                            if match &download_policy {
                                DownloadPolicy::NothingExcept(patterns) => {
                                    patterns.iter().any(|pattern| pattern.matches(entry.key()))
                                }
                                DownloadPolicy::EverythingExcept(patterns) => {
                                    patterns.iter().all(|pattern| !pattern.matches(entry.key()))
                                }
                            } {
                                self.download_entry_from_peers(&entry, &peers).await?;
                            }
                        }
                        None => break,
                    }

                }
            }
        }
        Ok(())
    }

    fn get_path(&self, key: &str) -> PathBuf {
        if let Some(file_shard_config) = self.hash_ring.range(key, 1).into_iter().next() {
            let file_shard = &self.shards[&file_shard_config.name];
            return file_shard.get_path_for(key);
        }
        unreachable!()
    }

    async fn process_sinks(&self, key: &str) -> Result<()> {
        let shard_path = self.get_path(key);
        for sink in &self.sinks {
            if let Err(error) = sink.send(key, &shard_path).await {
                warn!(error = ?error);
                continue;
            }
            info!(action = "send", sink = sink.name(), key = ?key);
        }
        Ok(())
    }

    async fn process_remote_entry(&self, key: &str, entry: &Entry) -> Result<Option<PathBuf>> {
        info!(action = "process_remote_entry", key = ?key);
        if entry.content_len() == 0 {
            self.delete_from_fs(key).await?;
            Ok(None)
        } else {
            let shard_path = self.get_path(key);
            if !shard_path.exists() {
                info!(action = "export_file", key = ?key);
                self.iroh_doc()
                    .export_file(entry.clone(), shard_path.clone(), ExportMode::TryReference)
                    .await
                    .map_err(Error::storage)?
                    .finish()
                    .await
                    .map_err(Error::storage)?;
            }
            Ok(Some(shard_path))
        }
    }

    async fn retain_blob_if_needed(&self, key: &str, hash: Hash) -> Result<()> {
        if !self.table_config.keep_blob {
            if let Err(error) = self.sync_client.blobs.delete_blob(hash).await {
                warn!(error = ?error);
            }
            self.delete_from_fs(key).await?;
        }
        Ok(())
    }

    pub async fn delete_from_fs(&self, key: &str) -> Result<()> {
        info!("delete_from_fs {:?} {:?}", self.iroh_doc().id(), key);
        if let Some(file_shard_config) = self.hash_ring.range(key, 1).into_iter().next() {
            self.shards[&file_shard_config.name]
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
        self.delete_from_fs(key).await?;
        Ok(removed_items)
    }

    pub async fn insert<S: AsyncRead + Send + Unpin>(&self, key: &str, value: S) -> Result<Hash> {
        if let Some(file_shard_config) = self.hash_ring.range(key, 1).into_iter().next() {
            let file_shard = &self.shards[&file_shard_config.name];
            let data_path = file_shard
                .insert(key, value)
                .await
                .map_err(Error::io_error)?;
            let import_progress = self
                .iroh_doc
                .import_file(self.author_id, key_to_bytes(key), &data_path, true)
                .await
                .map_err(Error::doc)?;
            return Ok(import_progress.finish().await.map_err(Error::storage)?.hash);
        }
        Err(Error::FileShard {
            description: "missing file shards".to_string(),
        })
    }

    pub fn iroh_doc(&self) -> &IrohDoc {
        &self.iroh_doc
    }

    pub async fn get(&self, key: &str) -> Result<Option<(Box<dyn AsyncRead + Unpin + Send>, u64)>> {
        let first_shard_path = self
            .hash_ring
            .range(key, 1)
            .into_iter()
            .next()
            .ok_or_else(|| Error::storage("no shards"))?;
        let shard = &self.shards[&first_shard_path.name];
        match shard.open_store(key).await {
            Ok(Some(file)) => {
                let file_size = file.metadata().await.unwrap().size();
                return Ok(Some((Box::new(file), file_size)));
            }
            Err(e) => return Err(Error::io_error(e)),
            Ok(None) => {}
        }
        if !self.table_config.try_retrieve_from_iroh {
            return Ok(None);
        }
        let entry = self
            .iroh_doc
            .get_one(
                Query::key_exact(key_to_bytes(key)).sort_by(SortBy::KeyAuthor, SortDirection::Asc),
            )
            .await
            .map_err(Error::entry)?;
        if let Some(entry) = entry {
            if let Ok(Some(peers)) = self.iroh_doc.get_sync_peers().await {
                self.download_entry_from_peers(&entry, &peers).await?;
            }
            let file_size = entry.content_len();
            return Ok(Some((
                Box::new(
                    entry
                        .content_reader(&self.sync_client)
                        .await
                        .map_err(Error::storage)?,
                ),
                file_size,
            )));
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
            .get_one(Query::key_exact(key_to_bytes(key)))
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
