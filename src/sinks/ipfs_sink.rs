use crate::config::IpfsConfig;
use crate::error::Error;
use crate::sinks::Sink;
use crate::utils::{bytes_to_key, FRAGMENT};
use axum::async_trait;
use percent_encoding::utf8_percent_encode;
use reqwest::header::HeaderMap;
use reqwest::Client;
use std::path::Path;

pub struct IpfsSink {
    name: String,
    config: IpfsConfig,
    client: Client,
}

impl IpfsSink {
    pub async fn new(name: &str, config: &IpfsConfig) -> Self {
        let mut config = config.clone();
        config.api_base_url = config.api_base_url.trim_end_matches('/').to_string();
        IpfsSink {
            name: name.to_string(),
            config,
            client: Client::new(),
        }
    }
}

#[async_trait]
impl Sink for IpfsSink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, key: &[u8], path: &Path) -> Result<(), Error> {
        // ToDo: Remove allocating and return stream
        // https://github.com/awslabs/aws-sdk-rust/discussions/361
        let encoded_key =
            utf8_percent_encode(std::str::from_utf8(bytes_to_key(key)).unwrap(), FRAGMENT)
                .collect::<String>()
                .to_lowercase();

        let mut headers = HeaderMap::new();

        headers.insert("Abspath", path.to_string_lossy().parse().unwrap());

        let file_part = reqwest::multipart::Part::bytes(tokio::fs::read(path).await.unwrap())
            .file_name(encoded_key)
            .headers(headers)
            .mime_str("application/octet-stream")
            .unwrap();
        let form = reqwest::multipart::Form::new().part("file", file_part);
        let res = self
            .client
            .post(format!(
                "{}/api/v0/add?hash=blake3&chunker=size-1048576&nocopy=true&pin=true",
                self.config.api_base_url,
            ))
            .multipart(form)
            .send()
            .await
            .map_err(Error::sink)?;
        if !res.status().is_success() {
            return Err(Error::sink(res.text().await.unwrap()));
        }
        Ok(())
    }
}
