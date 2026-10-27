use crate::store::actor::StoreReply;
use display_error_chain::ErrorChainExt as _;
use flume::{Receiver, Sender};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::warn;

pub enum DemuxRequest {
    Register {
        corr: u64,
        reply: Sender<StoreReply>,
    },
}

pub async fn run(
    reply_rx: Receiver<StoreReply>,
    register_rx: Receiver<DemuxRequest>,
    cancel: CancellationToken,
) {
    let mut pending: HashMap<u64, Sender<StoreReply>> = HashMap::new();

    loop {
        select! {
            _ = cancel.cancelled() => break,
            request = register_rx.recv_async() => {
                let Ok(request) = request else { break };
                match request {
                    DemuxRequest::Register { corr, reply } => {
                        if pending.insert(corr, reply).is_some() {
                            warn!(corr, "duplicate corr registration");
                        }
                    }
                }
            }
            reply = reply_rx.recv_async() => {
                let Ok(reply) = reply else { break };
                let corr = reply.corr();
                match pending.remove(&corr) {
                    Some(tx) => {
                        if tx.send_async(reply).await.is_err() {
                            warn!(corr, "reply waiter gone");
                        }
                    }
                    None => {
                        warn!(corr, "no waiter for store reply");
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct DemuxClient {
    register_tx: Sender<DemuxRequest>,
    next_corr: Arc<AtomicU64>,
}

impl DemuxClient {
    pub fn new(register_tx: Sender<DemuxRequest>) -> Self {
        Self {
            register_tx,
            next_corr: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn next_corr(&self) -> u64 {
        self.next_corr.fetch_add(1, Relaxed)
    }

    /// Register waiter before sending the matching StoreCmd.
    pub async fn register(&self, corr: u64) -> Receiver<StoreReply> {
        let (tx, rx) = flume::bounded(0);
        self.register_tx
            .send_async(DemuxRequest::Register { corr, reply: tx })
            .await
            .unwrap_or_else(|error| {
                panic!("failed to register reply waiter: {}", error.chain());
            });
        rx
    }
}
