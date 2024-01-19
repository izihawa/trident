use crate::config::S3Config;
use crate::error::Error;
use crate::utils::FRAGMENT;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::{BehaviorVersion, Region};
use aws_sdk_s3::primitives::ByteStream;
use axum::async_trait;
use percent_encoding::utf8_percent_encode;

#[async_trait]
pub trait Sink: Send + Sync {
    fn name(&self) -> &str;
    async fn send(&self, key: &[u8], value: Vec<u8>) -> Result<(), Error>;
}

pub struct S3Sink {
    name: String,
    client: aws_sdk_s3::Client,
    config: S3Config,
}

impl S3Sink {
    pub async fn new(name: &str, config: &S3Config) -> Self {
        let sdk_config = match &config.credentials {
            Some(credentials) => {
                let credentials = Credentials::from_keys(
                    &credentials.aws_access_key_id,
                    &credentials.aws_secret_access_key,
                    None,
                );
                aws_config::defaults(BehaviorVersion::latest())
                    .credentials_provider(credentials)
                    .region(Region::new(config.region_name.clone()))
                    .load()
                    .await
            }
            None => {
                aws_config::defaults(BehaviorVersion::latest())
                    .region(Region::new(config.region_name.clone()))
                    .load()
                    .await
            }
        };
        let client = aws_sdk_s3::Client::new(&sdk_config);
        S3Sink {
            name: name.to_string(),
            config: config.clone(),
            client,
        }
    }
}

#[async_trait]
impl Sink for S3Sink {
    fn name(&self) -> &str {
        &self.name
    }

    async fn send(&self, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
        // ToDo: Remove allocating and return stream
        // https://github.com/awslabs/aws-sdk-rust/discussions/361
        let encoded_key = utf8_percent_encode(std::str::from_utf8(key).unwrap(), FRAGMENT)
            .collect::<String>()
            .to_lowercase();

        self.client
            .put_object()
            .bucket(&self.config.bucket_name)
            .key([self.config.prefix.as_str(), encoded_key.as_str()].join("/"))
            .body(ByteStream::from(value))
            .send()
            .await
            .map_err(Error::sink)?;
        Ok(())
    }
}
