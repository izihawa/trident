use crate::config::{
    Config, FSStorageEngineConfig, MirroringConfig, SinkConfig, StorageEngineConfig, TableConfig,
};
use crate::error::{Error, Result};
use crate::sinks::{S3Sink, Sink};
use crate::storages::fs_storage::FSStorageEngine;
use crate::storages::iroh_storage::IrohStorageEngine;
use crate::storages::mirroring::Mirroring;
use crate::storages::{Storage, StorageEngine};
use crate::utils::bytes_to_key;
use crate::IrohClient;
use async_stream::stream;
use iroh::bytes::Hash;
use iroh::client::quic::RPC_ALPN;
use iroh::net::derp::DerpMode;
use iroh::node::Node;
use iroh::rpc_protocol::{ProviderRequest, ProviderResponse, ShareMode};
use iroh::sync::{AuthorId, NamespaceId};
use iroh::ticket::DocTicket;
use quic_rpc::transport::quinn::QuinnServerEndpoint;
use quinn;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::str::FromStr;
use std::sync::Arc;
use tokio::io::AsyncRead;
use tokio::sync::RwLock;
use tokio_stream::Stream;

pub struct IrohNode {
    sync_client: IrohClient,
    table_storages: HashMap<String, Storage>,
    author_id: AuthorId,
    config: Arc<RwLock<Config>>,
    fs_storage_configs: HashMap<String, FSStorageEngineConfig>,
    sinks: HashMap<String, Arc<dyn Sink>>,
}

impl IrohNode {
    pub async fn new(config: Arc<RwLock<Config>>) -> Result<Self> {
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

        let blob_path =
            iroh::util::path::IrohPaths::BaoFlatStoreDir.with_root(&config_lock.iroh.path);
        tokio::fs::create_dir_all(&blob_path)
            .await
            .map_err(Error::node_create)?;
        let db = iroh::bytes::store::flat::Store::load(&blob_path)
            .await
            .map_err(Error::node_create)?;

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

        let node = Node::builder(db, docs)
            .secret_key(secret_key)
            .peers_data_path(peer_data_path)
            .derp_mode(DerpMode::Default)
            .bind_port(config_lock.iroh.bind_port)
            .rpc_endpoint(rpc_endpoint)
            .spawn()
            .await
            .map_err(Error::node_create)?;

        let sync_client = node.client();

        let author_id = match &config_lock.iroh.author {
            Some(author) => AuthorId::from_str(author).unwrap(),
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
            };
        }

        let mut table_storages = HashMap::new();
        let fs_storage_configs = config_lock.iroh.fs_storages.clone();
        for (table_name, table_config) in &mut config_lock.iroh.tables {
            let iroh_doc = sync_client
                .docs
                .open(NamespaceId::from_str(&table_config.id).unwrap())
                .await
                .map_err(Error::table)?
                .unwrap();
            iroh_doc.start_sync(vec![]).await.unwrap();
            let storage_engine = match &table_config.storage_engine {
                StorageEngineConfig::Iroh => {
                    StorageEngine::Iroh(IrohStorageEngine::new(author_id, iroh_doc.clone()))
                }
                StorageEngineConfig::FS(storage_name) => StorageEngine::FS(
                    FSStorageEngine::new(
                        author_id,
                        iroh_doc.clone(),
                        fs_storage_configs[storage_name].clone(),
                    )
                    .await?,
                ),
            };
            let mirroring = table_config.mirroring.clone().map(|mirroring_config| {
                Mirroring::new(
                    iroh_doc.clone(),
                    mirroring_config
                        .sinks
                        .clone()
                        .into_iter()
                        .map(|sink_name| sinks[&sink_name].clone())
                        .collect(),
                    sync_client.clone(),
                    mirroring_config.delete_after_mirroring,
                )
            });
            let storage = Storage::new(storage_engine, mirroring);
            table_storages.insert(table_name.clone(), storage);
        }

        let fs_storage_configs = config_lock.iroh.fs_storages.clone();

        drop(config_lock);

        let iroh_node = IrohNode {
            sync_client,
            table_storages,
            author_id,
            config,
            fs_storage_configs,
            sinks,
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
        storage_name: Option<&str>,
        mirroring_config: Option<MirroringConfig>,
    ) -> Result<NamespaceId> {
        match self.table_storages.entry(table_name.to_string()) {
            Entry::Occupied(_) => Err(Error::existing_table(table_name)),
            Entry::Vacant(entry) => {
                let iroh_doc = self.sync_client.docs.create().await.map_err(Error::table)?;

                let (storage_engine, storage_engine_config) = match storage_name {
                    None => (
                        StorageEngine::Iroh(IrohStorageEngine::new(
                            self.author_id,
                            iroh_doc.clone(),
                        )),
                        StorageEngineConfig::Iroh,
                    ),
                    Some(storage_name) => (
                        StorageEngine::FS(
                            FSStorageEngine::new(
                                self.author_id,
                                iroh_doc.clone(),
                                self.fs_storage_configs[storage_name].clone(),
                            )
                            .await?,
                        ),
                        StorageEngineConfig::FS(storage_name.to_string()),
                    ),
                };
                let mirroring = mirroring_config.clone().map(|mirroring_config| {
                    Mirroring::new(
                        iroh_doc.clone(),
                        mirroring_config
                            .sinks
                            .clone()
                            .into_iter()
                            .map(|sink_name| self.sinks[&sink_name].clone())
                            .collect(),
                        self.sync_client.clone(),
                        mirroring_config.delete_after_mirroring,
                    )
                });
                let storage = Storage::new(storage_engine, mirroring);
                entry.insert(storage);
                self.config.write().await.iroh.tables.insert(
                    table_name.to_string(),
                    TableConfig {
                        id: iroh_doc.id().to_string(),
                        mirroring: mirroring_config,
                        storage_engine: storage_engine_config,
                    },
                );

                Ok(iroh_doc.id())
            }
        }
    }

    pub async fn tables_import(
        &mut self,
        table_name: &str,
        table_ticket: &str,
        storage_name: Option<&str>,
        mirroring_config: Option<MirroringConfig>,
    ) -> Result<NamespaceId> {
        match self.table_storages.entry(table_name.to_string()) {
            Entry::Occupied(_) => Err(Error::existing_table(table_name)),
            Entry::Vacant(entry) => {
                let iroh_doc = self
                    .sync_client
                    .docs
                    .import(DocTicket::from_str(table_ticket).unwrap())
                    .await
                    .map_err(Error::table)?;
                let (storage_engine, storage_engine_config) = match storage_name {
                    None => (
                        StorageEngine::Iroh(IrohStorageEngine::new(
                            self.author_id,
                            iroh_doc.clone(),
                        )),
                        StorageEngineConfig::Iroh,
                    ),
                    Some(storage_name) => (
                        StorageEngine::FS(
                            FSStorageEngine::new(
                                self.author_id,
                                iroh_doc.clone(),
                                self.fs_storage_configs[storage_name].clone(),
                            )
                            .await?,
                        ),
                        StorageEngineConfig::FS(storage_name.to_string()),
                    ),
                };

                let mirroring = mirroring_config.clone().map(|mirroring_config| {
                    Mirroring::new(
                        iroh_doc.clone(),
                        mirroring_config
                            .sinks
                            .clone()
                            .into_iter()
                            .map(|sink_name| self.sinks[&sink_name].clone())
                            .collect(),
                        self.sync_client.clone(),
                        mirroring_config.delete_after_mirroring,
                    )
                });
                let storage = Storage::new(storage_engine, mirroring);

                entry.insert(storage);
                self.config.write().await.iroh.tables.insert(
                    table_name.to_string(),
                    TableConfig {
                        id: iroh_doc.id().to_string(),
                        mirroring: mirroring_config,
                        storage_engine: storage_engine_config,
                    },
                );

                Ok(iroh_doc.id())
            }
        }
    }

    pub async fn tables_drop(&mut self, table_name: &str) -> Result<()> {
        match self.table_storages.remove(table_name) {
            None => Err(Error::missing_table(table_name)),
            Some(_) => Ok(self
                .config
                .write()
                .await
                .iroh
                .tables
                .remove(table_name)
                .unwrap()),
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
        to_table
            .insert_hash(to_key, from_hash.clone(), from_size)
            .await?;
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
    ) -> Result<Box<dyn AsyncRead + Unpin + Send>> {
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

    pub async fn table_exists(&self, table_name: &str, key: &str) -> Result<bool> {
        match self.table_storages.get(table_name) {
            Some(table_storage) => Ok(table_storage.get_hash(key).await?.is_some()),
            None => Err(Error::missing_table(table_name)),
        }
    }

    pub fn table_keys(&self, table_name: &str) -> Option<impl Stream<Item = Result<String>>> {
        self.table_storages.get(table_name).cloned().map_or_else(|| None, |table_storage| Some(stream! {
        for await el in table_storage.get_all() {
            yield Ok(format!("{}\n", std::str::from_utf8(bytes_to_key(el.unwrap().key())).unwrap()))
            }
        }))
    }
}
