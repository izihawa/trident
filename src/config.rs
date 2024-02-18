use crate::error::Error;
use iroh::sync::store::DownloadPolicy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

fn return_false() -> bool {
    false
}
fn return_true() -> bool {
    true
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageEngineConfig {
    pub shards: Vec<ShardConfig>,
    #[serde(default = "return_false")]
    pub is_import_missing_enabled: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ShardConfig {
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
pub struct IpfsConfig {
    pub api_base_url: String,
    pub in_place: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SinkConfig {
    S3(S3Config),
    Ipfs(IpfsConfig),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableConfig {
    pub id: String,
    #[serde(default = "DownloadPolicy::default")]
    pub download_policy: DownloadPolicy,
    #[serde(default = "Vec::new")]
    pub sinks: Vec<String>,
    pub storage_name: String,
    #[serde(default = "return_true")]
    pub keep_blob: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HttpConfig {
    pub endpoint: String,
}

impl Default for HttpConfig {
    fn default() -> Self {
        HttpConfig {
            endpoint: "127.0.0.1:80".to_string(),
        }
    }
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
    pub storages: HashMap<String, StorageEngineConfig>,
    pub gc_interval_secs: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub http: HttpConfig,
    pub iroh: IrohConfig,
}

impl Config {
    pub fn new(base_path: &Path, shards: u32) -> Self {
        Config {
            http: Default::default(),
            iroh: IrohConfig {
                author: None,
                tables: Default::default(),
                path: base_path.join("iroh").to_path_buf(),
                bind_port: 11204,
                rpc_port: 4919,
                max_rpc_connections: 32,
                max_rpc_streams: 1024,
                sinks: Default::default(),
                storages: HashMap::from_iter(
                    vec![(
                        "default".to_string(),
                        StorageEngineConfig {
                            shards: (1..=shards)
                                .map(|i| ShardConfig {
                                    name: format!("shard{i}"),
                                    path: base_path.join("data").join(format!("shard{i}")),
                                    weight: 1,
                                })
                                .collect(),
                            is_import_missing_enabled: false,
                        },
                    )]
                    .into_iter(),
                ),
                gc_interval_secs: None,
            },
        }
    }
    pub async fn load_config(config_path: &Path) -> Result<Config, Error> {
        let config_file_content = tokio::fs::read_to_string(config_path)
            .await
            .map_err(Error::io_error)?;
        serde_yaml::from_str(&config_file_content).map_err(Error::node_create)
    }

    pub async fn save_config(&self, config_path: &Path) -> Result<(), Error> {
        tokio::fs::write(
            config_path,
            serde_yaml::to_string(self).expect("unreachable").as_bytes(),
        )
        .await
        .map_err(Error::io_error)
    }
}
