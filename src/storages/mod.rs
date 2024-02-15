use crate::error::{Error, Result};
use async_stream::stream;
use futures::Stream;
use iroh::bytes::Hash;
use iroh::client::Entry;
use iroh::rpc_protocol::ShareMode;
use iroh::sync::store::{Query, SortBy, SortDirection};
use iroh::ticket::DocTicket;
use tokio::io::AsyncRead;

pub mod fs_storage;
pub(crate) mod iroh_storage;
pub mod mirroring;

use crate::storages::iroh_storage::IrohStorageEngine;
use crate::storages::mirroring::Mirroring;
use crate::utils::key_to_bytes;
use crate::IrohDoc;
use fs_storage::FSStorageEngine;

#[derive(Clone)]
pub enum StorageEngine {
    FS(FSStorageEngine),
    Iroh(IrohStorageEngine),
}

#[derive(Clone)]
pub struct Storage {
    engine: StorageEngine,
    mirroring: Option<Mirroring>,
}

impl Storage {
    pub fn new(engine: StorageEngine, mirroring: Option<Mirroring>) -> Self {
        Storage { engine, mirroring }
    }

    pub async fn get(&self, key: &str) -> Result<Box<dyn AsyncRead + Unpin + Send>> {
        match &self.engine {
            StorageEngine::FS(storage) => storage.get(key).await,
            StorageEngine::Iroh(storage) => storage.get(key).await,
        }
    }

    pub async fn insert<S: AsyncRead + Send + Unpin>(&self, key: &str, value: S) -> Result<Hash> {
        match &self.engine {
            StorageEngine::FS(storage) => storage.insert(key, value).await,
            StorageEngine::Iroh(storage) => storage.insert(key, value).await,
        }
    }

    pub async fn delete(&self, key: &str) -> Result<usize> {
        match &self.engine {
            StorageEngine::FS(storage) => storage.delete(key).await,
            StorageEngine::Iroh(storage) => storage.delete(key).await,
        }
    }

    pub async fn insert_hash(&self, key: &str, hash: Hash, size: u64) -> Result<()> {
        match &self.engine {
            StorageEngine::FS(_) => Err(Error::storage("unsupported_operation")),
            StorageEngine::Iroh(storage) => storage.insert_hash(key, hash, size).await,
        }
    }

    pub async fn exists(&self, key: &str) -> Result<bool> {
        match &self.engine {
            StorageEngine::FS(storage) => Ok(storage.exists(key).await?.is_some()),
            StorageEngine::Iroh(storage) => storage.exists(key).await,
        }
    }

    pub fn iroh_doc(&self) -> &IrohDoc {
        match &self.engine {
            StorageEngine::FS(storage) => storage.iroh_doc(),
            StorageEngine::Iroh(storage) => storage.iroh_doc(),
        }
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

    pub async fn share(&self, mode: ShareMode) -> Result<DocTicket> {
        self.iroh_doc().share(mode).await.map_err(Error::storage)
    }

    pub fn get_all(&self) -> impl Stream<Item = Result<Entry>> {
        let iroh_doc = self.iroh_doc().clone();
        stream! {
            for await entry in iroh_doc.get_many(Query::all()).await.map_err(Error::table)? {
                yield entry.map_err(Error::entry)
            }
        }
    }
}
