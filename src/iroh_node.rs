use crate::config::{Config, SinkConfig, StorageEngineConfig, TableConfig};
use crate::error::{Error, Result};
use crate::sinks::{IpfsSink, S3Sink, Sink};
use crate::storage::Storage;
use crate::IrohClient;
use async_stream::stream;
use futures::StreamExt;
use iroh::bytes::store::file::Store;
use iroh::bytes::Hash;
use iroh::client::quic::RPC_ALPN;
use iroh::node::{GcPolicy, Node};
use iroh::rpc_protocol::{ProviderRequest, ProviderResponse, ShareMode};
use iroh::sync::store::DownloadPolicy;
use iroh::sync::{AuthorId, NamespaceId};
use iroh::ticket::DocTicket;
use quic_rpc::transport::quinn::QuinnServerEndpoint;
use quinn;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncRead;
use tokio::sync::RwLock;
use tokio_stream::Stream;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

pub struct IrohNode {
    sync_client: IrohClient,
    table_storages: HashMap<String, Storage>,
    author_id: AuthorId,
    config: Arc<RwLock<Config>>,
    fs_storage_configs: HashMap<String, StorageEngineConfig>,
    sinks: HashMap<String, Arc<dyn Sink>>,
    cancellation_token: CancellationToken,
    task_tracker: TaskTracker,
    node: Node<Store>,
}

impl Debug for IrohNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.sync_client.fmt(f)
    }
}

impl IrohNode {
    pub async fn new(
        config: Arc<RwLock<Config>>,
        cancellation_token: CancellationToken,
        task_tracker: TaskTracker,
    ) -> Result<Self> {
        let mut config_lock = config.write().await;
        tokio::fs::create_dir_all(&config_lock.iroh.path)
            .await
            .map_err(Error::node_create)?;

        let secret_key_path =
            iroh::util::path::IrohPaths::SecretKey.with_root(&config_lock.iroh.path);
        let secret_key = iroh::util::fs::load_secret_key(secret_key_path)
            .await
            .map_err(Error::node_create)?;

        let docs_path = iroh::util::path::IrohPaths::DocsDatabase.with_root(&config_lock.iroh.path);
        let docs = iroh::sync::store::fs::Store::new(&docs_path).map_err(Error::node_create)?;

        let blob_path = iroh::util::path::IrohPaths::BaoStoreDir.with_root(&config_lock.iroh.path);
        tokio::fs::create_dir_all(&blob_path)
            .await
            .map_err(Error::node_create)?;
        let db = Store::load(&blob_path).await.map_err(Error::node_create)?;

        let peer_data_path =
            iroh::util::path::IrohPaths::PeerData.with_root(&config_lock.iroh.path);

        let rpc_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, config_lock.iroh.rpc_port);
        let server_config = iroh::node::make_server_config(
            &secret_key,
            config_lock.iroh.max_rpc_streams,
            config_lock.iroh.max_rpc_connections,
            vec![RPC_ALPN.to_vec()],
        )
        .map_err(Error::node_create)?;

        let rpc_quinn_endpoint = quinn::Endpoint::server(server_config.clone(), rpc_addr.into())
            .map_err(Error::node_create)?;

        let rpc_endpoint =
            QuinnServerEndpoint::<ProviderRequest, ProviderResponse>::new(rpc_quinn_endpoint)
                .map_err(Error::node_create)?;

        let mut node_builder = Node::builder(db, docs)
            .secret_key(secret_key)
            .peers_data_path(peer_data_path)
            .bind_port(config_lock.iroh.bind_port)
            .rpc_endpoint(rpc_endpoint);

        if let Some(gc_interval_secs) = config_lock.iroh.gc_interval_secs {
            node_builder =
                node_builder.gc_policy(GcPolicy::Interval(Duration::from_secs(gc_interval_secs)))
        }
        let node = node_builder.spawn().await.map_err(Error::node_create)?;
        let sync_client = node.client();

        for doc in sync_client
            .docs
            .list()
            .await
            .unwrap()
            .map(|x| x.unwrap().0)
            .collect::<Vec<_>>()
            .await
        {
            info!(action = "all_docs", "{}", doc);
        }

        let author_id = match &config_lock.iroh.author {
            Some(author) => AuthorId::from_str(author).map_err(Error::author)?,
            None => {
                let author_id = sync_client.authors.create().await.map_err(Error::author)?;
                config_lock.iroh.author = Some(author_id.to_string());
                author_id
            }
        };

        let mut sinks = HashMap::new();
        for (name, sink_config) in config_lock.iroh.sinks.clone() {
            match sink_config {
                SinkConfig::S3(s3_config) => {
                    let sink = S3Sink::new(&name, &s3_config).await;
                    sinks.insert(name, Arc::new(sink) as Arc<dyn Sink>)
                }
                SinkConfig::Ipfs(ipfs_config) => {
                    let sink = IpfsSink::new(&name, &ipfs_config).await;
                    sinks.insert(name, Arc::new(sink) as Arc<dyn Sink>)
                }
            };
        }
        let mut table_storages = HashMap::new();
        let storage_configs = config_lock.iroh.storages.clone();
        for (table_name, table_config) in &mut config_lock.iroh.tables {
            let iroh_doc = sync_client
                .docs
                .open(NamespaceId::from_str(&table_config.id).map_err(Error::storage)?)
                .await
                .map_err(Error::table)?
                .ok_or_else(|| Error::table(format!("{} does not exist", table_config.id)))?;
            iroh_doc
                .set_download_policy(table_config.download_policy.clone())
                .await
                .map_err(Error::doc)?;

            iroh_doc.start_sync(vec![]).await.map_err(Error::doc)?;
            let materialised_sinks = table_config
                .sinks
                .iter()
                .map(|sink_name| sinks[sink_name].clone())
                .collect();
            let storage_engine = Storage::new(
                table_name,
                author_id,
                iroh_doc.clone(),
                sync_client.clone(),
                storage_configs[&table_config.storage_name].clone(),
                materialised_sinks,
                table_config.clone(),
                cancellation_token.clone(),
                task_tracker.clone(),
            )
            .await?;
            table_storages.insert(table_name.clone(), storage_engine);
        }

        let fs_storage_configs = config_lock.iroh.storages.clone();

        drop(config_lock);

        let iroh_node = IrohNode {
            node,
            sync_client,
            table_storages,
            author_id,
            config,
            fs_storage_configs,
            sinks,
            cancellation_token: cancellation_token.clone(),
            task_tracker: task_tracker.clone(),
        };

        Ok(iroh_node)
    }

    pub async fn sinks_ls(&self) -> HashMap<String, SinkConfig> {
        self.config.read().await.iroh.sinks.clone()
    }

    pub async fn sinks_create(&self, sink_name: &str, sink_config: SinkConfig) -> Result<()> {
        match self
            .config
            .write()
            .await
            .iroh
            .sinks
            .entry(sink_name.to_string())
        {
            Entry::Occupied(_) => return Err(Error::existing_sink(sink_name)),
            Entry::Vacant(entry) => entry.insert(sink_config),
        };
        Ok(())
    }

    pub async fn tables_ls(&self) -> HashMap<String, TableConfig> {
        self.config.read().await.iroh.tables.clone()
    }

    pub async fn tables_create(
        &mut self,
        table_name: &str,
        storage_name: &str,
        sinks: Vec<String>,
        keep_blob: bool,
        try_retrieve_from_iroh: bool,
    ) -> Result<NamespaceId> {
        match self.table_storages.entry(table_name.to_string()) {
            Entry::Occupied(_) => Err(Error::existing_table(table_name)),
            Entry::Vacant(entry) => {
                let iroh_doc = self.sync_client.docs.create().await.map_err(Error::table)?;
                let materialised_sinks = sinks
                    .iter()
                    .map(|sink_name| self.sinks[sink_name].clone())
                    .collect();
                let table_config = TableConfig {
                    id: iroh_doc.id().to_string(),
                    download_policy: DownloadPolicy::default(),
                    sinks,
                    storage_name: storage_name.to_string(),
                    keep_blob,
                    try_retrieve_from_iroh,
                };
                let storage_engine = Storage::new(
                    table_name,
                    self.author_id,
                    iroh_doc.clone(),
                    self.sync_client.clone(),
                    self.fs_storage_configs[storage_name].clone(),
                    materialised_sinks,
                    table_config.clone(),
                    self.cancellation_token.clone(),
                    self.task_tracker.clone(),
                )
                .await?;
                entry.insert(storage_engine);
                self.config
                    .write()
                    .await
                    .iroh
                    .tables
                    .insert(table_name.to_string(), table_config);

                Ok(iroh_doc.id())
            }
        }
    }

    pub async fn tables_exists(&self, table_name: &str) -> bool {
        return self.table_storages.get(table_name).is_some();
    }

    pub async fn tables_import(
        &mut self,
        table_name: &str,
        table_ticket: &str,
        storage_name: &str,
        download_policy: DownloadPolicy,
        sinks: Vec<String>,
        keep_blob: bool,
    ) -> Result<NamespaceId> {
        let ticket = DocTicket::from_str(table_ticket).map_err(Error::doc)?;
        let nodes = ticket.nodes.clone();
        match self.table_storages.entry(table_name.to_string()) {
            Entry::Occupied(entry) => {
                let iroh_doc = entry.get().iroh_doc();
                if iroh_doc.id() != ticket.capability.id() {
                    return Err(Error::existing_table(table_name));
                }
                iroh_doc.start_sync(nodes).await.map_err(Error::doc)?;
                Ok(entry.get().iroh_doc().id())
            }
            Entry::Vacant(entry) => {
                let iroh_doc = self
                    .sync_client
                    .docs
                    .import(ticket)
                    .await
                    .map_err(Error::table)?;
                iroh_doc
                    .set_download_policy(download_policy.clone())
                    .await
                    .map_err(Error::doc)?;
                iroh_doc.start_sync(nodes).await.map_err(Error::doc)?;
                let materialised_sinks = sinks
                    .iter()
                    .map(|sink_name| self.sinks[sink_name].clone())
                    .collect();
                let table_config = TableConfig {
                    id: iroh_doc.id().to_string(),
                    download_policy,
                    sinks,
                    storage_name: storage_name.to_string(),
                    keep_blob,
                    try_retrieve_from_iroh: true,
                };
                let storage_engine = Storage::new(
                    table_name,
                    self.author_id,
                    iroh_doc.clone(),
                    self.sync_client.clone(),
                    self.fs_storage_configs[storage_name].clone(),
                    materialised_sinks,
                    table_config.clone(),
                    self.cancellation_token.clone(),
                    self.task_tracker.clone(),
                )
                .await?;
                entry.insert(storage_engine);
                self.config
                    .write()
                    .await
                    .iroh
                    .tables
                    .insert(table_name.to_string(), table_config);
                Ok(iroh_doc.id())
            }
        }
    }

    pub async fn tables_integrity(&self, table_name: &str) -> Result<()> {
        let Some(storage) = self.table_storages.get(table_name) else {
            return Err(Error::missing_table(table_name));
        };
        let storage0 = storage.clone();
        self.task_tracker
            .spawn(async move { storage0.check_integrity().await });
        Ok(())
    }

    pub async fn tables_sync(
        &self,
        table_name: &str,
        download_policy: Option<DownloadPolicy>,
        threads: u32,
    ) -> Result<()> {
        let Some(storage) = self.table_storages.get(table_name) else {
            return Err(Error::missing_table(table_name));
        };
        let storage0 = storage.clone();
        self.task_tracker
            .spawn(async move { storage0.download_missing(download_policy, threads).await });
        Ok(())
    }

    pub async fn tables_drop(&mut self, table_name: &str) -> Result<()> {
        match self.table_storages.remove(table_name) {
            None => Err(Error::missing_table(table_name)),
            Some(table_storage) => {
                self.sync_client
                    .docs
                    .drop_doc(table_storage.iroh_doc().id())
                    .await
                    .map_err(Error::doc)?;
                Ok(self
                    .config
                    .write()
                    .await
                    .iroh
                    .tables
                    .remove(table_name)
                    .ok_or_else(|| Error::missing_table(table_name))?)
            }
        }?;
        Ok(())
    }

    pub async fn table_insert<S: AsyncRead + Send + Unpin>(
        &self,
        table_name: &str,
        key: &str,
        value: S,
    ) -> Result<Hash> {
        match self.table_storages.get(table_name) {
            Some(table_storage) => table_storage.insert(key, value).await,
            None => Err(Error::missing_table(table_name)),
        }
    }

    pub async fn tables_foreign_insert(
        &self,
        from_table_name: &str,
        from_key: &str,
        to_table_name: &str,
        to_key: &str,
    ) -> Result<Hash> {
        let Some(from_table) = self.table_storages.get(from_table_name) else {
            return Err(Error::missing_table(from_table_name));
        };
        let Some(to_table) = self.table_storages.get(to_table_name) else {
            return Err(Error::missing_table(to_table_name));
        };
        let Some((from_hash, from_size)) = from_table.get_hash(from_key).await? else {
            return Err(Error::missing_key(from_key));
        };
        to_table.insert_hash(to_key, from_hash, from_size).await?;
        Ok(from_hash)
    }

    pub async fn table_share(&self, table_name: &str) -> Result<DocTicket> {
        match self.table_storages.get(table_name) {
            Some(table_storage) => Ok(table_storage.share(ShareMode::Read).await?),
            None => Err(Error::missing_table(table_name)),
        }
    }

    pub async fn table_get(
        &self,
        table_name: &str,
        key: &str,
    ) -> Result<Option<(Box<dyn AsyncRead + Unpin + Send>, u64)>> {
        match self.table_storages.get(table_name) {
            Some(table_storage) => table_storage.get(key).await,
            None => Err(Error::missing_table(table_name)),
        }
    }

    pub async fn table_delete(&self, table_name: &str, key: &str) -> Result<usize> {
        match self.table_storages.get(table_name) {
            Some(table_holder) => table_holder.delete(key).await,
            None => Err(Error::missing_table(table_name)),
        }
    }

    pub fn table_keys(&self, table_name: &str) -> Option<impl Stream<Item = Result<String>>> {
        self.table_storages.get(table_name).cloned().map_or_else(
            || None,
            |table_storage| {
                Some(stream! {
                for await el in table_storage.get_all() {
                    yield Ok(format!("{}\n", std::str::from_utf8(el.unwrap().key()).unwrap()))
                    }
                })
            },
        )
    }

    pub async fn send_shutdown(&self) -> Result<()> {
        self.sync_client
            .node
            .shutdown(false)
            .await
            .map_err(Error::node_create)?;
        Ok(())
    }

    pub async fn shutdown(self) -> Result<()> {
        self.node.shutdown();
        Ok(())
    }
}
