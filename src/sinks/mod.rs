use crate::error::Error;
use axum::async_trait;
use std::path::Path;

mod ipfs_sink;
mod s3_sink;

pub use ipfs_sink::IpfsSink;
pub use s3_sink::S3Sink;

#[async_trait]
pub trait Sink: Send + Sync {
    fn name(&self) -> &str;
    async fn send(&self, key: &[u8], path: &Path) -> Result<(), Error>;
}
