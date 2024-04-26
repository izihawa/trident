use crate::config::{StorageEngineConfig, TableConfig};
use crate::error::{Error, Result};
use crate::file_shard::FileShard;
use crate::hash_ring::HashRing;
use crate::ranges::{slice, to_byte_range, to_chunk_range};
use crate::utils::key_to_bytes;
use crate::IrohDoc;
use async_stream::stream;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use iroh::bytes::get::fsm::{BlobContentNext, ConnectedNext, DecodeError, EndBlobNext};
use iroh::bytes::protocol::RangeSpecSeq;
use iroh::bytes::store::bao_tree::io::BaoContentItem;
use iroh::bytes::store::fs::Store;
use iroh::bytes::store::{ExportMode, Map};
use iroh::bytes::Hash;
use iroh::client::{Entry, LiveEvent};
use iroh::net::key::PublicKey;
use iroh::net::NodeAddr;
use iroh::node::Node;
use iroh::rpc_protocol::{BlobDownloadRequest, DownloadMode, SetTagOption, ShareMode};
use iroh::sync::store::{DownloadPolicy, Query, SortBy, SortDirection};
use iroh::sync::{AuthorId, Capability, ContentStatus};
use iroh::ticket::DocTicket;
use iroh_base::hash::BlobFormat;
use lru::LruCache;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::result;
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

    async fn download_entry_from_peers(&self, entry: &Entry, nodes: Vec<NodeAddr>) -> Result<()> {
        let key = std::str::from_utf8(entry.key()).map_err(Error::incorrect_key)?;
        let progress = self
            .node
            .blobs
            .download(BlobDownloadRequest {
                hash: entry.content_hash(),
                format: BlobFormat::Raw,
                nodes: nodes.to_vec(),
                tag: SetTagOption::Auto,
                mode: DownloadMode::Queued,
            })
            .await
            .map_err(Error::io_error)?;
        progress.finish().await.map_err(Error::failed_download)?;
        self.process_remote_entry(key, entry).await?;
        Ok(())
    }

    pub async fn download_missing(
        &self,
        download_policy: Option<DownloadPolicy>,
        threads: u32,
    ) -> Result<()> {
        let Ok(Some(peers)) = self.iroh_doc.get_sync_peers().await else {
            return Ok(());
        };
        let nodes: Vec<_> = peers
            .iter()
            .filter_map(|peer| PublicKey::from_bytes(peer).map(NodeAddr::from).ok())
            .collect();
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
                let mut nodes0 = nodes.clone();
                nodes0.shuffle(&mut thread_rng());
                let join_handle = pool
                    .spawn(async move {
                        if let Err(error) = storage0.download_entry_from_peers(&entry, nodes0).await
                        {
                            warn!(error = ?error);
                        }
                    })
                    .await
                    .map_err(Error::io_error)?;
                tokio::spawn(self.task_tracker.track_future(join_handle));
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

    pub async fn get(
        &self,
        key: &str,
    ) -> Result<Option<(Box<dyn AsyncRead + Unpin + Send>, u64, Hash)>> {
        let entry = self
            .iroh_doc
            .get_one(
                Query::key_exact(key_to_bytes(key)).sort_by(SortBy::KeyAuthor, SortDirection::Asc),
            )
            .await
            .map_err(Error::entry)?;
        if let Some(entry) = entry {
            let db_entry = self
                .node
                .db()
                .get(&entry.content_hash())
                .await
                .unwrap_or_default();
            let is_complete = db_entry.map_or(false, |db_entry| db_entry.is_complete());
            if !is_complete {
                if let Ok(Some(mut peers)) = self.iroh_doc.get_sync_peers().await {
                    peers.shuffle(&mut thread_rng());
                    let nodes = peers
                        .iter()
                        .filter_map(|peer| PublicKey::from_bytes(peer).map(NodeAddr::from).ok())
                        .collect();
                    self.download_entry_from_peers(&entry, nodes).await?;
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
                entry.content_hash(),
            )));
        }
        Ok(None)
    }

    pub async fn get_partial(
        &self,
        key: &str,
        range: (Option<u64>, Option<u64>),
    ) -> Result<(
        flume::Receiver<result::Result<Bytes, DecodeError>>,
        u64,
        bool,
    )> {
        let (hash, _) = self.get_hash(key).await.unwrap().unwrap();

        let connection = self.get_default_connection().await.unwrap();

        let byte_ranges = to_byte_range(range.0, range.1);
        let is_all = byte_ranges.is_all();
        let chunk_ranges = to_chunk_range(range.0, range.1);
        let chunk_ranges = RangeSpecSeq::from_ranges(vec![chunk_ranges]);

        let request = iroh::bytes::protocol::GetRequest::new(hash, chunk_ranges.clone());
        let (send, recv) = flume::bounded::<result::Result<Bytes, DecodeError>>(2);

        let req = iroh::bytes::get::fsm::start(connection.clone(), request);
        let connected = req.next().await.unwrap();
        let ConnectedNext::StartRoot(x) = connected.next().await.unwrap() else {
            panic!("unexpected")
        };
        tracing::trace!("connected");
        let (mut current, size) = x.next().next().await.unwrap();
        tokio::spawn(async move {
            let end = loop {
                match current.next().await {
                    BlobContentNext::More((next, Ok(item))) => {
                        match item {
                            BaoContentItem::Leaf(leaf) => {
                                tracing::trace!("got leaf {} {}", leaf.offset, leaf.data.len());
                                for item in slice(leaf.offset, leaf.data, byte_ranges.clone()) {
                                    send.send_async(Ok(item)).await?;
                                }
                            }
                            BaoContentItem::Parent(parent) => {
                                tracing::trace!("got parent {:?}", parent);
                            }
                        }
                        current = next;
                    }
                    BlobContentNext::More((_, Err(err))) => {
                        send.send_async(Err(err)).await?;
                        anyhow::bail!("error");
                    }
                    BlobContentNext::Done(end) => break end,
                }
            };
            let EndBlobNext::Closing(at_closing) = end.next() else {
                anyhow::bail!("unexpected response");
            };
            let _stats = at_closing.next().await?;
            Ok(())
        });
        Ok((recv, size, is_all))
    }

    /// Get the mime type for a hash from the remote node.
    async fn get_default_connection(&self) -> anyhow::Result<quinn::Connection> {
        self.node
            .magic_endpoint()
            .connect(
                self.node.my_addr().await.unwrap(),
                iroh::bytes::protocol::ALPN,
            )
            .await
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
        let capability = match mode {
            ShareMode::Read => Capability::Read(self.iroh_doc.id()),
            ShareMode::Write => {
                let secret = self
                    .node
                    .sync_handle()
                    .export_secret_key(self.iroh_doc.id())
                    .await
                    .map_err(Error::missing_key)?;
                Capability::Write(secret)
            }
        };
        let me = self.node.my_addr().await.map_err(Error::io_error)?;
        Ok(DocTicket {
            capability,
            nodes: vec![NodeAddr::from(me.node_id)],
        })
    }

    pub async fn peers(&self) -> Result<Option<Vec<NodeAddr>>> {
        let peers = self
            .iroh_doc()
            .get_sync_peers()
            .await
            .map_err(Error::io_error)?;
        let Some(peers) = peers else {
            return Ok(None);
        };
        Ok(Some(
            peers
                .iter()
                .filter_map(|peer| PublicKey::from_bytes(peer).map(NodeAddr::from).ok())
                .collect(),
        ))
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
