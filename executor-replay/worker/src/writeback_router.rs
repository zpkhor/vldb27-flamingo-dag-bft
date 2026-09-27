// Copyright(C) Facebook, Inc. and its affiliates.
// Adapted from origin/old-executor-patch:worker/src/router.rs
use crate::executor::WorkerToExecutorMessage;
use crate::router::ExecuteCommand;
use crate::transaction::Transaction;
use crate::worker::WorkerMessage;
use crate::workload::WorkloadType;
use bytes::Bytes;
use config::{Committee, ExecutorId, Partition, WorkerId};
use crypto::{Digest, PublicKey};
use log::{debug, info, warn};
use network::SimpleSender;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use store::Store;
use tokio::sync::mpsc::Receiver;

/// Helper function to send execution message to executor process via network.
async fn send_to_executor(
    network: &mut SimpleSender,
    addr: SocketAddr,
    digest: Digest,
    sequence: u64,
    txs: Vec<Vec<u8>>,
) {
    let msg = if txs.is_empty() {
        WorkerToExecutorMessage::SequenceSync(sequence)
    } else {
        WorkerToExecutorMessage::Execute(digest, sequence, txs)
    };

    let serialized = bincode::serialize(&msg)
        .expect("Failed to serialize WorkerToExecutorMessage");

    network.send(addr, Bytes::from(serialized)).await;
}

/// The `WritebackRouter` receives Execute commands from the Primary, partitions
/// transactions by account ownership, and routes them to the executor that owns the account.
///
/// Key difference from HEAD's Router: this router does per-transaction partitioning
/// (only sends each executor its relevant transactions), whereas Router
/// broadcasts the full batch to all executors.
///
/// For distributed SendPayment (src and dest on different executors), the transaction
/// is multicast to BOTH executors.
pub struct WritebackRouter {
    /// The id of this worker.
    worker_id: WorkerId,
    /// The persistent storage to retrieve batches.
    store: Store,
    /// Current partition for states for execution, static during runtime.
    states_partition_cache: Arc<RwLock<Option<Partition>>>,
    /// Receives execute commands from the primary.
    rx_execute: Receiver<ExecuteCommand>,
    /// Workload type configured at worker initialization (static per deployment).
    workload_type: WorkloadType,
    /// Network addresses of all executors for this authority (including local).
    executor_addresses: HashMap<ExecutorId, SocketAddr>,
    /// Network sender for executor communication.
    executor_network: SimpleSender,
}

impl WritebackRouter {
    pub fn spawn(
        name: PublicKey,
        worker_id: WorkerId,
        committee: Committee,
        store: Store,
        states_partition_cache: Arc<RwLock<Option<Partition>>>,
        rx_execute: Receiver<ExecuteCommand>,
        workload_type: WorkloadType,
    ) {
        // Get all executor addresses for this authority
        let executor_addresses: HashMap<ExecutorId, SocketAddr> = committee
            .authorities
            .get(&name)
            .map(|authority| {
                authority
                    .executors
                    .iter()
                    .map(|(wid, addr)| (*wid, addr.worker_to_executor))
                    .collect()
            })
            .unwrap_or_else(HashMap::new);

        tokio::spawn(async move {
            Self {
                worker_id,
                store,
                states_partition_cache,
                rx_execute,
                workload_type,
                executor_addresses,
                executor_network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some(cmd) = self.rx_execute.recv().await {
            if let Err(e) = self.route_execution(cmd).await {
                warn!("Failed to route execution: {}", e);
            }
        }
    }

    async fn route_execution(&mut self, cmd: ExecuteCommand) -> Result<(), Box<dyn std::error::Error>> {
        let ExecuteCommand { digest, sequence } = cmd;

        // Retrieve the batch from storage.
        let serialized = match self.store.read(digest.to_vec()).await? {
            Some(data) => data,
            None => {
                info!(
                    "Batch {:?} (seq={}) not yet in storage, waiting for sync to complete...",
                    digest, sequence
                );

                self.store.notify_read(digest.to_vec()).await?;

                info!(
                    "Batch {:?} (seq={}) sync completed, proceeding with execution",
                    digest, sequence
                );

                self.store
                    .read(digest.to_vec())
                    .await?
                    .ok_or_else(|| format!("Batch {:?} not found in storage after sync notification", digest))?
            }
        };

        // Deserialize the WorkerMessage and extract the batch.
        let message: WorkerMessage = bincode::deserialize(&serialized)?;
        let batch = match message {
            WorkerMessage::Batch(batch) => batch,
            _ => return Err(format!("Expected WorkerMessage::Batch, got other variant").into()),
        };

        let partition = self.states_partition_cache.read().unwrap().clone();

        // Pre-populate HashMap with all executor IDs so every executor gets a message
        // O(W²) behavior: ALL workers must receive messages (even empty) for synchronization.
        // This ensures strict sequence ordering across all workers within the validator.
        let mut txs_by_executors: HashMap<ExecutorId, Vec<Vec<u8>>> = HashMap::new();
        for executor_id in self.executor_addresses.keys() {
            txs_by_executors.insert(*executor_id, Vec::new());
        }

        for tx_bytes in batch.iter() {
            let transaction = Transaction::new(tx_bytes);

            match self.workload_type {
                WorkloadType::SmallBank => {
                    let (src_opt, dest_opt) = transaction.extract_account_ids();

                    match (src_opt, dest_opt) {
                        (Some(src), Some(dest)) => {
                            let partition_assignment = partition.as_ref()
                                .expect("Partition required for SmallBank workload");

                            let src_owner = partition_assignment.get(&src).copied()
                                .expect("Source account must have owner in partition");
                            let dest_owner = partition_assignment.get(&dest).copied()
                                .expect("Dest account must have owner in partition");

                            if src != dest && src_owner != dest_owner {
                                // Distributed SendPayment: multicast to BOTH executors
                                txs_by_executors.get_mut(&src_owner).unwrap().push(tx_bytes.clone());
                                txs_by_executors.get_mut(&dest_owner).unwrap().push(tx_bytes.clone());
                            } else {
                                txs_by_executors.get_mut(&src_owner).unwrap().push(tx_bytes.clone());
                            }
                        }
                        _ => {
                            warn!("Malformed SmallBank transaction, ignoring: {:?}", tx_bytes);
                        }
                    }
                }
            }
        }

        // Send to all executors via network
        for (target_executor_id, txs) in txs_by_executors.iter() {
            if let Some(&addr) = self.executor_addresses.get(target_executor_id) {
                send_to_executor(&mut self.executor_network, addr, digest.clone(), sequence, txs.clone()).await;

                if !txs.is_empty() {
                    debug!(
                        "Sent to executor {} at {} with {} txs for batch {:?} (seq={})",
                        target_executor_id,
                        addr,
                        txs.len(),
                        digest,
                        sequence
                    );
                }
            } else {
                warn!("Executor {} not found in cached addresses", target_executor_id);
            }
        }

        // Log routing summary.
        let local_txs_len = txs_by_executors.get(&self.worker_id).map(|v| v.len()).unwrap_or(0);
        let remote_count: usize = txs_by_executors
            .iter()
            .filter(|(wid, _)| **wid != self.worker_id)
            .map(|(_, v)| v.len())
            .sum();
        let remote_workers = txs_by_executors
            .keys()
            .filter(|wid| **wid != self.worker_id)
            .count();

        info!(
            "Routed batch {:?} (seq={}): {} local, {} remote txs to {} executors",
            digest,
            sequence,
            local_txs_len,
            remote_count,
            remote_workers
        );

        Ok(())
    }
}
