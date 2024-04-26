use crate::error::Error;
use iroh::sync::store::DownloadPolicy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

fn return_0() -> u32 {
    0
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageEngineConfig {
    pub shards: Vec<ShardConfig>,
    #[serde(default = "return_0")]
    pub import_threads: u32,
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
pub struct TableConfig {
    pub id: String,
    #[serde(default = "DownloadPolicy::default")]
    pub download_policy: DownloadPolicy,
    pub storage_name: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HttpConfig {
    pub hostname: Option<String>,
    pub endpoint: String,
}

impl Default for HttpConfig {
    fn default() -> Self {
        HttpConfig {
            endpoint: "0.0.0.0:80".to_string(),
            hostname: None,
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
    #[serde(default = "HashMap::new")]
    pub storages: HashMap<String, StorageEngineConfig>,
    pub gc_interval_secs: Option<u64>,
    pub relays: Option<Vec<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub http: HttpConfig,
    pub iroh: IrohConfig,
}

impl Config {
    pub fn new(base_path: &Path, shards: Option<u32>) -> Self {
        Config {
            http: Default::default(),
            iroh: IrohConfig {
                author: None,
                tables: Default::default(),
                path: base_path.join("iroh").to_path_buf(),
                bind_port: 11204,
                storages: HashMap::from_iter(vec![(
                    "default".to_string(),
                    StorageEngineConfig {
                        shards: (0..=shards.unwrap_or(0))
                            .map(|i| ShardConfig {
                                name: format!("shard{i}"),
                                path: base_path.join("data").join(format!("shard{i}")),
                                weight: 1,
                            })
                            .collect(),
                        import_threads: 0,
                    },
                )]),
                gc_interval_secs: None,
                relays: None,
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
