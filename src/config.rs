use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

fn return_false() -> bool {
    false
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum StorageEngineConfig {
    FS(String),
    Iroh,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FSStorageEngineConfig {
    pub replicas: u8,
    pub fs_shards: Vec<FSShardConfig>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FSShardConfig {
    pub name: String,
    pub path: PathBuf,
    pub weight: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct S3ConfigCredentials {
    pub aws_access_key_id: String,
    pub aws_secret_access_key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct S3Config {
    pub credentials: Option<S3ConfigCredentials>,
    pub bucket_name: String,
    pub prefix: String,
    pub region_name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SinkConfig {
    S3(S3Config),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MirroringConfig {
    #[serde(default = "Vec::new")]
    pub sinks: Vec<String>,
    #[serde(default = "return_false")]
    pub delete_after_mirroring: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableConfig {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mirroring: Option<MirroringConfig>,
    pub storage_engine: StorageEngineConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HttpConfig {
    pub endpoint: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IrohConfig {
    pub author: Option<String>,
    #[serde(default = "HashMap::new")]
    pub tables: HashMap<String, TableConfig>,
    pub path: PathBuf,
    pub bind_port: u16,
    pub rpc_port: u16,
    pub max_rpc_connections: u32,
    pub max_rpc_streams: u64,
    #[serde(default = "HashMap::new")]
    pub sinks: HashMap<String, SinkConfig>,
    #[serde(default = "HashMap::new")]
    pub fs_storages: HashMap<String, FSStorageEngineConfig>,
    pub gc_interval_secs: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub http: HttpConfig,
    pub iroh: IrohConfig,
}

pub async fn load_config(config_path: &str) -> Result<Config, Error> {
    let config_file_content = tokio::fs::read_to_string(config_path)
        .await
        .map_err(Error::io_error)?;
    serde_yaml::from_str(&config_file_content).map_err(Error::node_create)
}

pub async fn save_config(config_path: &str, config: &Config) -> Result<(), Error> {
    tokio::fs::write(
        config_path,
        serde_yaml::to_string(config).unwrap().as_bytes(),
    )
    .await
    .map_err(Error::io_error)
}
