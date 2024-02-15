use crate::error::{Error, Result};
use crate::sinks::Sink;
use crate::{IrohClient, IrohDoc};
use futures::StreamExt;
use iroh::client::LiveEvent;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{info, info_span, warn, Instrument};

#[derive(Clone)]
pub struct Mirroring {
    thread: Arc<JoinHandle<Result<()>>>,
}

impl Mirroring {
    pub async fn new(
        iroh_doc: IrohDoc,
        sinks: Vec<Arc<dyn Sink>>,
        sync_client: IrohClient,
        delete_after_mirroring: bool,
    ) -> Result<Self> {
        let table_id = iroh_doc.id().to_string();
        let mut stream = iroh_doc.subscribe().await.map_err(Error::doc)?;
        let thread = tokio::spawn(
            async move {
                let mut wait_list = LruCache::new(NonZeroUsize::new(1024).expect("not_possible"));
                info!("started");
                while let Some(event) = stream.next().await {
                    let event = match event {
                        Ok(event) => event,
                        Err(error) => {
                            warn!(error = ?error);
                            continue
                        }
                    };
                    match &event {
                        LiveEvent::InsertLocal { entry } => {
                            info!(event = ?event);
                            match sync_client.blobs.read(entry.content_hash()).await {
                                Ok(mut reader) => {
                                    let bytes = match reader.read_to_bytes().await {
                                        Ok(bytes) => bytes,
                                        Err(error) => {
                                            warn!(error = ?error);
                                            continue;
                                        }
                                    };
                                    for sink in &sinks {
                                        if let Err(error) =
                                            sink.send(entry.key(), bytes.clone()).await
                                        {
                                            warn!(error = ?error);
                                            continue;
                                        }
                                        info!(action = "sent", sink = sink.name(), key = ?std::str::from_utf8(entry.key()));
                                    }
                                }
                                Err(error) => warn!(error = ?error),
                            }
                        }
                        LiveEvent::InsertRemote { entry, .. } => {
                            info!(event = ?event);
                            wait_list.put(entry.content_hash(), entry.key().to_vec());
                        }
                        LiveEvent::ContentReady { hash } => {
                            info!(event = ?event);
                            let Some(key) = wait_list.get(hash) else {
                                warn!(error = "missing_key_in_wait_list");
                                continue;
                            };
                            match sync_client.blobs.read(*hash).await {
                                Ok(mut reader) => {
                                    let bytes = match reader.read_to_bytes().await {
                                        Ok(bytes) => bytes,
                                        Err(error) => {
                                            warn!(error = ?error);
                                            continue;
                                        }
                                    };
                                    for sink in &sinks {
                                        if let Err(error) = sink.send(key, bytes.clone()).await {
                                            warn!(error = ?error);
                                            continue;
                                        }
                                        info!(action = "sent", sink = sink.name(), key = ?std::str::from_utf8(key));
                                    }
                                    if delete_after_mirroring {
                                        let sync_client = sync_client.clone();
                                        let hash = hash.clone();
                                        tokio::spawn(async move {
                                            tokio::time::sleep(Duration::from_secs(120)).await;
                                            if let Err(error) =
                                                sync_client.blobs.delete_blob(hash).await
                                            {
                                                warn!(error = ?error);
                                            }
                                        });
                                    }
                                }
                                Err(error) => warn!(error = ?error),
                            }
                        }
                        _ => {}
                    }
                }
                warn!("stopped_mirroring");
                Ok(())
            }
            .instrument(info_span!(parent: None, "mirroring", table_id = table_id)),
        );
        Ok(Self {
            thread: Arc::new(thread),
        })
    }
}
