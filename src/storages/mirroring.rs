use crate::error::Error;
use crate::sinks::Sink;
use crate::{IrohClient, IrohDoc};
use futures::StreamExt;
use iroh::client::LiveEvent;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::task::JoinHandle;
use tracing::{info, info_span, warn, Instrument};

#[derive(Clone)]
pub struct Mirroring {
    thread: Arc<JoinHandle<Result<(), Error>>>,
}

impl Mirroring {
    #[must_use]
    pub fn new(
        iroh_doc: IrohDoc,
        sinks: Vec<Arc<dyn Sink>>,
        sync_client: IrohClient,
        delete_after_mirroring: bool,
    ) -> Self {
        let table_id = iroh_doc.id().to_string();
        let thread = tokio::spawn(
            async move {
                let mut stream = iroh_doc.subscribe().await.unwrap();
                let mut wait_list = HashMap::new();
                info!("started");
                while let Some(event) = stream.next().await {
                    let event = event.unwrap();
                    match &event {
                        LiveEvent::InsertLocal { entry } => {
                            info!(event = ?event);
                            let mut reader = match entry.content_reader(&sync_client).await {
                                Ok(reader) => reader,
                                Err(error) => {
                                    warn!(error = ?error);
                                    continue;
                                }
                            };
                            let mut buffer = vec![];
                            if let Err(error) = reader.read_to_end(&mut buffer).await {
                                warn!(error = ?error);
                                continue;
                            }
                            for sink in &sinks {
                                if let Err(error) = sink.send(entry.key(), buffer.clone()).await {
                                    warn!(error = ?error);
                                    continue;
                                }
                            }
                        }
                        LiveEvent::InsertRemote { entry, .. } => {
                            info!(event = ?event);
                            wait_list.insert(entry.content_hash(), entry.key().to_vec());
                        }
                        LiveEvent::ContentReady { hash } => {
                            info!(event = ?event);
                            let Some(key) = wait_list.remove(hash) else {
                                warn!(error = "missing_key_in_wait_list");
                                continue;
                            };
                            match sync_client.blobs.read(*hash).await {
                                Ok(mut reader) => {
                                    let mut buffer = vec![];
                                    if let Err(error) = reader.read_to_end(&mut buffer).await {
                                        warn!(error = ?error);
                                        continue;
                                    }
                                    for sink in &sinks {
                                        info!(action = "send", sink = sink.name());
                                        if let Err(error) = sink.send(&key, buffer.clone()).await {
                                            warn!(error = ?error);
                                            continue;
                                        }
                                    }
                                    if delete_after_mirroring {
                                        if let Err(error) =
                                            sync_client.blobs.delete_blob(*hash).await
                                        {
                                            warn!(error = ?error);
                                            continue;
                                        }
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
            .instrument(info_span!("mirroring", table_id = table_id)),
        );
        Self {
            thread: Arc::new(thread),
        }
    }
}
