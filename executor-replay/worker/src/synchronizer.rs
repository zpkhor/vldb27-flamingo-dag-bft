// Copyright(C) Facebook, Inc. and its affiliates.
#[allow(unused_imports)]
use crate::worker::{CommitNotification, Round, WorkerMessage};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error, info};
use network::SimpleSender;
use primary::PrimaryWorkerMessage;
use std::collections::HashMap;
use std::convert::TryInto;
use std::time::{SystemTime, UNIX_EPOCH};
use store::{Store, StoreError};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/synchronizer_tests.rs"]
pub mod synchronizer_tests;

/// Resolution of the timer managing retrials of sync requests (in ms).
const TIMER_RESOLUTION: u64 = 1_000;

// The `Synchronizer` is responsible to keep the worker in sync with the others.
pub struct Synchronizer {
    /// The public key of this authority.
    name: PublicKey,
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    // The persistent storage.
    store: Store,
    /// The depth of the garbage collection.
    gc_depth: Round,
    /// The delay to wait before re-trying to send sync requests.
    sync_retry_delay: u64,
    /// Determine with how many nodes to sync when re-trying to send sync-requests. These nodes
    /// are picked at random from the committee.
    sync_retry_nodes: usize,
    /// Input channel to receive the commands from the primary.
    rx_message: Receiver<PrimaryWorkerMessage>,
    /// A network sender to send requests to the other workers.
    network: SimpleSender,
    /// Loosely keep track of the primary's round number (only used for cleanup).
    round: Round,
    /// Keeps the digests (of batches) that are waiting to be processed by the primary. Their
    /// processing will resume when we get the missing batches in the store or we no longer need them.
    /// It also keeps the round number and a timestamp (`u128`) of each request we sent.
    pending: HashMap<Digest, (Round, Sender<()>, u128)>,
    /// Tracks committed batch digests per consensus round for in-memory store GC.
    committed_batches: HashMap<Round, Vec<Digest>>,
}

impl Synchronizer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        id: WorkerId,
        committee: Committee,
        store: Store,
        gc_depth: Round,
        sync_retry_delay: u64,
        sync_retry_nodes: usize,
        rx_message: Receiver<PrimaryWorkerMessage>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                id,
                committee,
                store,
                gc_depth,
                sync_retry_delay,
                sync_retry_nodes,
                rx_message,
                network: SimpleSender::new(),
                round: Round::default(),
                pending: HashMap::new(),
                committed_batches: HashMap::new(),
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a batch to become available in the storage
    /// and then delivers its digest.
    async fn waiter(
        missing: Digest,
        mut store: Store,
        deliver: Digest,
        mut handler: Receiver<()>,
    ) -> Result<Option<Digest>, StoreError> {
        tokio::select! {
            result = store.notify_read(missing.to_vec()) => {
                result.map(|_| Some(deliver))
            }
            _ = handler.recv() => Ok(None),
        }
    }

    /// Handle committed batches: wait for each batch to be available and log sample txs.
    async fn handle_committed_batches(&mut self, round: Round, digests: Vec<Digest>) {
        self.committed_batches.entry(round).or_default().extend(digests.iter().cloned());
        for digest in digests {
            let batch_data = self.store
                .notify_read(digest.to_vec())
                .await
                .expect("Failed to read committed batch from store");

            let txs = match bincode::deserialize::<WorkerMessage>(&batch_data) {
                Ok(WorkerMessage::Batch(txs)) => txs,
                _ => {
                    panic!("Failed to read committed batch {}", digest);
                }
            };

            for tx in &txs {
                // SmallBank format: [tx_type:1][client_id:1][tx_counter:8][src_account:8]...
                if tx.len() >= 10 && tx[0] == 0u8 {
                    let counter = u64::from_be_bytes(tx[2..10].try_into().unwrap());
                    info!("Committed sample tx {} from batch {:?}", counter, digest);
                }
            }
        }
    }

    /// Forward migration notices to the affected clients via client_reply addresses.
    async fn handle_migration_notices(&mut self, notices: Vec<primary::MigrationNotice>) {
        // Group notices by home validator (= the client that needs to reroute)
        let mut per_client: HashMap<PublicKey, Vec<primary::MigrationNotice>> =
            HashMap::new();
        for notice in notices {
            if let Some(pk) = self.committee.home_validator(notice.account_id) {
                per_client.entry(pk).or_default().push(notice);
            }
        }

        for (client_validator, client_notices) in per_client {
            if let Ok(addr) = self.committee.client_reply(&client_validator) {
                info!(
                    "Sending {} migration notices to client of validator {} at {} ({} chunks)",
                    client_notices.len(), client_validator, addr,
                    (client_notices.len() + primary::MIGRATION_CHUNK_SIZE - 1) / primary::MIGRATION_CHUNK_SIZE
                );
                for chunk in client_notices.chunks(primary::MIGRATION_CHUNK_SIZE) {
                    let msg = primary::MigrationMessage {
                        sender: self.name,
                        notices: chunk.to_vec(),
                    };
                    let serialized = bincode::serialize(&msg)
                        .expect("Failed to serialize MigrationMessage");
                    self.network.send(addr, bytes::Bytes::from(serialized)).await;
                }
            }
        }
    }

    /// Main loop listening to the primary's messages.
    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();

        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Handle primary's messages.
                Some(message) = self.rx_message.recv() => match message {
                    PrimaryWorkerMessage::Synchronize(digests, target) => {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Failed to measure time")
                            .as_millis();

                        let mut missing = Vec::new();
                        for digest in digests {
                            // Ensure we do not send twice the same sync request.
                            if self.pending.contains_key(&digest) {
                                continue;
                            }

                            // Check if we received the batch in the meantime.
                            match self.store.read(digest.to_vec()).await {
                                Ok(None) => {
                                    missing.push(digest.clone());
                                    debug!("Requesting sync for batch {}", digest);
                                },
                                Ok(Some(_)) => {
                                    // The batch arrived in the meantime: no need to request it.
                                },
                                Err(e) => {
                                    error!("{}", e);
                                    continue;
                                }
                            }

                            // Add the digest to the waiter.
                            let deliver = digest.clone();
                            let (tx_cancel, rx_cancel) = channel(1);
                            let fut = Self::waiter(digest.clone(), self.store.clone(), deliver, rx_cancel);
                            waiting.push(fut);
                            self.pending.insert(digest, (self.round, tx_cancel, now));
                        }

                        // Send sync request to a single node. If this fails, we will send it
                        // to other nodes when a timer times out.
                        let address = match self.committee.worker(&target, &self.id) {
                            Ok(address) => address.worker_to_worker,
                            Err(e) => {
                                error!("The primary asked us to sync with an unknown node: {}", e);
                                continue;
                            }
                        };
                        let message = WorkerMessage::BatchRequest(missing, self.name);
                        let serialized = bincode::serialize(&message).expect("Failed to serialize our own message");
                        self.network.send(address, Bytes::from(serialized)).await;
                    },
                    PrimaryWorkerMessage::CommittedBatches(round, digests) => {
                        self.handle_committed_batches(round, digests).await;
                    },
                    PrimaryWorkerMessage::MigrationNotices(notices) => {
                        self.handle_migration_notices(notices).await;
                    },
                    PrimaryWorkerMessage::Execute(..) => {
                        // Handled by PrimaryReceiverHandler directly (routed to Router).
                    },
                    PrimaryWorkerMessage::Cleanup(round) => {
                        // Keep track of the primary's round number.
                        self.round = round;

                        // Cleanup internal state.
                        if self.round < self.gc_depth {
                            continue;
                        }

                        let mut gc_round = self.round - self.gc_depth;
                        for (r, handler, _) in self.pending.values() {
                            if r <= &gc_round {
                                let _ = handler.send(()).await;
                            }
                        }
                        self.pending.retain(|_, (r, _, _)| r > &mut gc_round);

                        // GC old batches from the in-memory store.
                        if self.round >= 2 * self.gc_depth {
                            let batch_gc_round = self.round - 2 * self.gc_depth;
                            let old_rounds: Vec<Round> = self.committed_batches
                                .keys()
                                .filter(|&&r| r <= batch_gc_round)
                                .copied()
                                .collect();
                            for r in old_rounds {
                                if let Some(digests) = self.committed_batches.remove(&r) {
                                    for digest in &digests {
                                        self.store.delete(digest.to_vec()).await;
                                    }
                                    debug!("GC'd {} batches from round {}", digests.len(), r);
                                }
                            }
                        }
                    }
                },

                // Stream out the futures of the `FuturesUnordered` that completed.
                Some(result) = waiting.next() => match result {
                    Ok(Some(digest)) => {
                        // We got the batch, remove it from the pending list.
                        self.pending.remove(&digest);
                    },
                    Ok(None) => {
                        // The sync request for this batch has been canceled.
                    },
                    Err(e) => error!("{}", e)
                },

                // Triggers on timer's expiration.
                () = &mut timer => {
                    // We optimistically sent sync requests to a single node. If this timer triggers,
                    // it means we were wrong to trust it. We are done waiting for a reply and we now
                    // broadcast the request to a bunch of other nodes (selected at random).
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Failed to measure time")
                        .as_millis();

                    let mut retry = Vec::new();
                    for (digest, (_, _, timestamp)) in &self.pending {
                        if timestamp + (self.sync_retry_delay as u128) < now {
                            debug!("Requesting sync for batch {} (retry)", digest);
                            retry.push(digest.clone());
                        }
                    }
                    if !retry.is_empty() {
                        let addresses = self.committee
                            .others_workers(&self.name, &self.id)
                            .iter().map(|(_, address)| address.worker_to_worker)
                            .collect();
                        let message = WorkerMessage::BatchRequest(retry, self.name);
                        let serialized = bincode::serialize(&message).expect("Failed to serialize our own message");
                        self.network
                            .lucky_broadcast(addresses, Bytes::from(serialized), self.sync_retry_nodes)
                            .await;
                    }

                    // Reschedule the timer.
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                },
            }
        }
    }
}
