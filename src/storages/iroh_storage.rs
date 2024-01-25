use crate::error::{Error, Result};
use crate::utils::key_to_bytes;
use crate::IrohDoc;
use iroh::bytes::Hash;
use iroh::sync::store::Query;
use iroh::sync::AuthorId;
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Clone)]
pub struct IrohStorageEngine {
    author_id: AuthorId,
    iroh_doc: IrohDoc,
}

impl IrohStorageEngine {
    pub fn new(author_id: AuthorId, iroh_doc: IrohDoc) -> Self {
        IrohStorageEngine {
            author_id,
            iroh_doc,
        }
    }

    pub async fn delete(&self, key: &str) -> Result<usize> {
        self.iroh_doc
            .del(self.author_id, key_to_bytes(key))
            .await
            .map_err(Error::missing_key)
    }

    pub async fn insert<S: AsyncRead + Unpin>(&self, key: &str, mut value: S) -> Result<Hash> {
        let mut buffer = vec![];
        value
            .read_to_end(&mut buffer)
            .await
            .map_err(Error::io_error)?;
        self.iroh_doc
            .set_bytes(self.author_id, key_to_bytes(key), buffer)
            .await
            .map_err(Error::hash)
    }

    pub async fn insert_hash(&self, key: &str, hash: Hash, size: u64) -> Result<()> {
        self.iroh_doc
            .set_hash(self.author_id, key_to_bytes(key), hash, size)
            .await
            .map_err(Error::hash)
    }

    pub async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self
            .iroh_doc
            .get_one(Query::key_exact(key_to_bytes(key)))
            .await
            .map_err(Error::doc)?
            .is_some())
    }

    pub const fn iroh_doc(&self) -> &IrohDoc {
        &self.iroh_doc
    }
}
