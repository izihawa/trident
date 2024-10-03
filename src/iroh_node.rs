use crate::config::{Config, StorageEngineConfig, TableConfig};
use crate::error::{Error, Result};
use crate::table::Table;
use async_stream::stream;
use futures::StreamExt;
use iroh::blobs::store::fs::Store;
use iroh::client::docs::{Entry, ShareMode};
use iroh::docs::store::DownloadPolicy;
use iroh::docs::{AuthorId, DocTicket, NamespaceId};
use iroh::net::defaults::DEFAULT_STUN_PORT;
use iroh::net::relay::{RelayMap, RelayMode, RelayNode};
use iroh::node::{GcPolicy, Node};
use iroh_base::hash::Hash;
use iroh_base::node_addr::NodeAddr;
use serde_json::json;
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
use url::Url;

pub struct IrohNode {
    tables: HashMap<String, Table>,
    author_id: AuthorId,
    config: Arc<RwLock<Config>>,
    fs_storage_configs: HashMap<String, StorageEngineConfig>,
    cancellation_token: CancellationToken,
    task_tracker: TaskTracker,
    node: Node<Store>,
}

impl Debug for IrohNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.node.fmt(f)
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

        let relay_mode = match &config_lock.iroh.relays {
            None => RelayMode::Default,
            Some(relays) => RelayMode::Custom(
                RelayMap::from_nodes(relays.iter().map(|r| {
                    let url: Url = r.parse().expect("default url");
                    RelayNode {
                        url: url.into(),
                        stun_only: false,
                        stun_port: DEFAULT_STUN_PORT,
                    }
                }))
                .expect("relay config error"),
            ),
        };

        let mut node_builder = Node::persistent(&config_lock.iroh.path)
            .await
            .map_err(Error::node_create)?
            .relay_mode(relay_mode)
            .bind_addr_v4(SocketAddrV4::new(
                Ipv4Addr::from_str(&config_lock.iroh.bind_address).map_err(Error::node_create)?,
                config_lock.iroh.bind_port,
            ));

        if let Some(gc_interval_secs) = config_lock.iroh.gc_interval_secs {
            node_builder =
                node_builder.gc_policy(GcPolicy::Interval(Duration::from_secs(gc_interval_secs)))
        }
        let node = node_builder.spawn().await.map_err(Error::node_create)?;

        for doc in node
            .client()
            .docs()
            .list()
            .await
            .map_err(Error::io_error)?
            .map(|x| x.expect("Can't extract document").0)
            .collect::<Vec<_>>()
            .await
        {
            info!(action = "all_docs", "{}", doc);
        }

        let author_id = match &config_lock.iroh.author {
            Some(author) => AuthorId::from_str(author).map_err(Error::author)?,
            None => {
                let author_id = node
                    .client()
                    .authors()
                    .create()
                    .await
                    .map_err(Error::author)?;
                config_lock.iroh.author = Some(author_id.to_string());
                author_id
            }
        };

        let mut tables = HashMap::new();
        let storage_configs = config_lock.iroh.storages.clone();
        let mut init_futures = vec![];
        for (table_name, table_config) in &mut config_lock.iroh.tables {
            let table_name = table_name.clone();
            let table_config = table_config.clone();
            let storage_config = table_config
                .storage_name
                .as_ref()
                .map(|s| storage_configs[s].clone());
            let task_tracker = task_tracker.clone();
            let cancellation_token = cancellation_token.clone();
            let node = node.clone();
            let init_future = tokio::spawn(async move {
                let iroh_doc = node
                    .client()
                    .docs()
                    .open(NamespaceId::from_str(&table_config.id).map_err(Error::storage)?)
                    .await
                    .map_err(Error::table)?
                    .ok_or_else(|| Error::table(format!("{} does not exist", table_config.id)))?;
                iroh_doc
                    .set_download_policy(table_config.download_policy.clone())
                    .await
                    .map_err(Error::doc)?;
                iroh_doc.start_sync(vec![]).await.map_err(Error::doc)?;
                let table = Table::new(
                    &table_name,
                    author_id,
                    node.clone(),
                    iroh_doc.clone(),
                    storage_config,
                    table_config.clone(),
                    cancellation_token.clone(),
                    task_tracker.clone(),
                )
                .await?;
                Ok((table_name.clone(), table))
            });
            init_futures.push(init_future);
        }
        for init_future in init_futures {
            let (table_name, table) = init_future.await.map_err(Error::node_create)??;
            tables.insert(table_name, table);
        }

        let fs_storage_configs = config_lock.iroh.storages.clone();

        drop(config_lock);

        let iroh_node = IrohNode {
            node,
            tables,
            author_id,
            config,
            fs_storage_configs,
            cancellation_token,
            task_tracker,
        };

        Ok(iroh_node)
    }

    pub fn client(&self) -> &iroh::client::Iroh {
        self.node.client()
    }

    pub async fn tables_ls(&self) -> HashMap<String, TableConfig> {
        self.config.read().await.iroh.tables.clone()
    }

    pub async fn tables_create(
        &mut self,
        table_name: &str,
        storage_name: Option<String>,
    ) -> Result<NamespaceId> {
        match self.tables.entry(table_name.to_string()) {
            std::collections::hash_map::Entry::Occupied(_) => {
                Err(Error::existing_table(table_name))
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                let iroh_doc = self
                    .node
                    .client()
                    .docs()
                    .create()
                    .await
                    .map_err(Error::table)?;
                let table_config = TableConfig {
                    id: iroh_doc.id().to_string(),
                    download_policy: DownloadPolicy::default(),
                    storage_name: storage_name.clone(),
                    safe_mode: false,
                };
                let table = Table::new(
                    table_name,
                    self.author_id,
                    self.node.clone(),
                    iroh_doc.clone(),
                    storage_name.map(|s| self.fs_storage_configs[&s].clone()),
                    table_config.clone(),
                    self.cancellation_token.clone(),
                    self.task_tracker.clone(),
                )
                .await?;
                entry.insert(table);
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
        self.tables.contains_key(table_name)
    }

    pub async fn tables_peers(&self, table_name: &str) -> Result<Option<Vec<NodeAddr>>> {
        match self.tables.get(table_name) {
            None => Err(Error::missing_table(table_name)),
            Some(table) => table.peers().await,
        }
    }

    pub async fn tables_import(
        &mut self,
        table_name: &str,
        table_ticket: &str,
        storage_name: Option<String>,
        download_policy: DownloadPolicy,
    ) -> Result<NamespaceId> {
        let ticket = DocTicket::from_str(table_ticket).map_err(Error::doc)?;
        let nodes = ticket.nodes.clone();
        match self.tables.entry(table_name.to_string()) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                let iroh_doc = entry.get().iroh_doc();
                if ticket.capability.id() != iroh_doc.id() {
                    return Err(Error::existing_table("different document in table"));
                }
                self.node
                    .client()
                    .docs()
                    .import(ticket)
                    .await
                    .map_err(Error::table)?;
                iroh_doc.start_sync(nodes).await.map_err(Error::doc)?;
                Ok(entry.get().iroh_doc().id())
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                let iroh_doc = self
                    .node
                    .client()
                    .docs()
                    .import(ticket)
                    .await
                    .map_err(Error::table)?;
                iroh_doc
                    .set_download_policy(download_policy.clone())
                    .await
                    .map_err(Error::doc)?;
                iroh_doc.start_sync(nodes).await.map_err(Error::doc)?;
                let table_config = TableConfig {
                    id: iroh_doc.id().to_string(),
                    download_policy,
                    storage_name: storage_name.clone(),
                    safe_mode: false,
                };
                let storage_engine = Table::new(
                    table_name,
                    self.author_id,
                    self.node.clone(),
                    iroh_doc.clone(),
                    storage_name.map(|s| self.fs_storage_configs[&s].clone()),
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

    pub async fn tables_sync(
        &self,
        table_name: &str,
        download_policy: Option<DownloadPolicy>,
        threads: u32,
    ) -> Result<()> {
        let Some(table) = self.tables.get(table_name) else {
            return Err(Error::missing_table(table_name));
        };
        let table0 = table.clone();
        self.task_tracker
            .spawn(async move { table0.download_missing(download_policy, threads).await });
        Ok(())
    }

    pub async fn tables_drop(&mut self, table_name: &str) -> Result<()> {
        match self.tables.remove(table_name) {
            None => Err(Error::missing_table(table_name)),
            Some(table) => {
                self.node
                    .client()
                    .docs()
                    .drop_doc(table.iroh_doc().id())
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
        match self.tables.get(table_name) {
            Some(table) => table.insert(key, value).await,
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
        let Some(from_table) = self.tables.get(from_table_name) else {
            return Err(Error::missing_table(from_table_name));
        };
        let Some(to_table) = self.tables.get(to_table_name) else {
            return Err(Error::missing_table(to_table_name));
        };
        let Some((from_hash, from_size)) = from_table.get_hash(from_key).await? else {
            return Err(Error::missing_key(from_key));
        };
        to_table.insert_hash(to_key, from_hash, from_size).await?;
        Ok(from_hash)
    }

    pub async fn tables_hash_insert(
        &self,
        hash: &str,
        to_table_name: &str,
        to_key: &str,
    ) -> Result<Hash> {
        let Some(to_table) = self.tables.get(to_table_name) else {
            return Err(Error::missing_table(to_table_name));
        };
        let hash = Hash::from_str(hash).map_err(Error::blobs)?;
        match self.blobs_get(hash.clone()).await? {
            Some((_, size)) => {
                to_table.insert_hash(to_key, hash, size).await?;
                Ok(hash)
            }
            None => Err(Error::missing_key(hash)),
        }
    }

    pub async fn table_share(&self, table_name: &str, mode: ShareMode) -> Result<DocTicket> {
        match self.tables.get(table_name) {
            Some(table) => Ok(table.share(mode).await?),
            None => Err(Error::missing_table(table_name)),
        }
    }

    pub async fn table_get(&self, table_name: &str, key: &str) -> Result<Option<Entry>> {
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| Error::missing_table(table_name))?;
        table.get_with_import(key).await
    }

    pub async fn table_delete(&self, table_name: &str, key: &str) -> Result<usize> {
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| Error::missing_table(table_name))?;
        table.delete(key).await
    }

    pub fn table_keys(&self, table_name: &str) -> Option<impl Stream<Item = Result<String>>> {
        self.tables.get(table_name).cloned().map_or_else(
            || None,
            |table| {
                Some(stream! {
                    for await el in table.get_all() {
                        let entry = el.expect("Can't extract document");
                        let key = std::str::from_utf8(entry.key()).expect("Not utf8 symbol");
                        let hash_str = entry.content_hash().to_string();
                        let data = json!({
                            "key": key,
                            "hash": hash_str,
                            "size": entry.content_len(),
                        });
                        yield Ok(format!("{}\n", data.to_string()))
                    }
                })
            },
        )
    }

    pub async fn blobs_get(
        &self,
        hash: Hash,
    ) -> Result<Option<(Box<dyn AsyncRead + Unpin + Send>, u64)>> {
        match self.node.blobs().read(hash).await {
            Ok(blob_reader) => {
                if !blob_reader.is_complete() {
                    return Ok(None);
                }
                let file_size = blob_reader.size();
                Ok(Some((Box::new(blob_reader), file_size)))
            }
            Err(e) => {
                if e.to_string() == "Blob not found" {
                    return Ok(None);
                }
                Err(Error::blobs(e))
            }
        }
    }

    pub async fn send_shutdown(&self) -> Result<()> {
        self.node
            .client()
            .shutdown(false)
            .await
            .map_err(Error::node_create)?;
        Ok(())
    }

    pub async fn shutdown(self) -> Result<()> {
        self.node.shutdown().await.map_err(Error::node_create)?;
        Ok(())
    }
}
