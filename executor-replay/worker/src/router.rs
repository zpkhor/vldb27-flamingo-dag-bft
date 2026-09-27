use crate::executor::WorkerToExecutorMessage;
use crate::worker::WorkerMessage;
use config::{Committee, ExecutorId};
use crypto::{Digest, PublicKey};
use log::{info, warn};
use network::SimpleSender;
use store::Store;
use tokio::sync::mpsc::Receiver;
use std::collections::HashMap;
use std::net::SocketAddr;

/// Command from primary to execute a committed batch.
pub struct ExecuteCommand {
    pub digest: Digest,
    pub sequence: u64,
}

pub struct Router;

impl Router {
    /// Spawn the Router that reads committed batches from store and broadcasts them
    /// to all executors of this validator.
    pub fn spawn(
        name: PublicKey,
        _worker_id: u32,
        committee: Committee,
        store: Store,
        rx_execute: Receiver<ExecuteCommand>,
    ) {
        // Collect executor addresses for this validator
        let executor_addresses: HashMap<ExecutorId, SocketAddr> = committee
            .authorities
            .get(&name)
            .map(|authority| {
                authority
                    .executors
                    .iter()
                    .map(|(eid, addr)| (*eid, addr.worker_to_executor))
                    .collect()
            })
            .unwrap_or_default();

        if executor_addresses.is_empty() {
            info!("Router: no executors configured, Execute messages will be dropped");
        } else {
            info!(
                "Router: broadcasting to {} executors: {:?}",
                executor_addresses.len(),
                executor_addresses
            );
        }

        tokio::spawn(async move {
            Self::run(store, rx_execute, executor_addresses).await;
        });
    }

    async fn run(
        mut store: Store,
        mut rx_execute: Receiver<ExecuteCommand>,
        executor_addresses: HashMap<ExecutorId, SocketAddr>,
    ) {
        let mut network = SimpleSender::new();

        while let Some(cmd) = rx_execute.recv().await {
            // Read batch from store
            let transactions = match store.read(cmd.digest.to_vec()).await {
                Ok(Some(data)) => {
                    match bincode::deserialize::<WorkerMessage>(&data) {
                        Ok(WorkerMessage::Batch(batch)) => batch,
                        Ok(_) => {
                            warn!("Router: unexpected message type for digest {:?}", cmd.digest);
                            continue;
                        }
                        Err(e) => {
                            warn!("Router: failed to deserialize batch {:?}: {}", cmd.digest, e);
                            continue;
                        }
                    }
                }
                Ok(None) => {
                    // Batch not yet available, wait for it
                    match store.notify_read(cmd.digest.to_vec()).await {
                        Ok(data) => {
                            match bincode::deserialize::<WorkerMessage>(&data) {
                                Ok(WorkerMessage::Batch(batch)) => batch,
                                Ok(_) => continue,
                                Err(e) => {
                                    warn!("Router: deserialize after wait failed: {}", e);
                                    continue;
                                }
                            }
                        }
                        Err(e) => {
                            warn!("Router: notify_read failed for {:?}: {}", cmd.digest, e);
                            continue;
                        }
                    }
                }
                Err(e) => {
                    warn!("Router: store read failed for {:?}: {}", cmd.digest, e);
                    continue;
                }
            };

            // Broadcast to all executors
            for (&_eid, &addr) in &executor_addresses {
                let msg = if transactions.is_empty() {
                    WorkerToExecutorMessage::SequenceSync(cmd.sequence)
                } else {
                    WorkerToExecutorMessage::Execute(
                        cmd.digest.clone(),
                        cmd.sequence,
                        transactions.clone(),
                    )
                };
                let serialized = bincode::serialize(&msg)
                    .expect("Failed to serialize WorkerToExecutorMessage");
                network.send(addr, bytes::Bytes::from(serialized)).await;
            }
        }
    }
}
