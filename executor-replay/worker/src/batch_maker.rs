// Copyright(C) Facebook, Inc. and its affiliates.
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::WorkerMessage;
use bytes::Bytes;
use crypto::Digest;
use crypto::PublicKey;
use ed25519_dalek::{Digest as _, Sha512};
use log::info;
use network::ReliableSender;
use std::convert::TryInto as _;
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

pub type Transaction = Vec<u8>;
pub type Batch = Vec<Transaction>;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,
    /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_message: Sender<QuorumWaiterMessage>,
    /// The network addresses of the other workers that share our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Holds the current batch of transactions (each includes 8-byte account_id prefix).
    current_batch: Vec<Transaction>,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other workers.
    network: ReliableSender,
}

impl BatchMaker {
    pub fn spawn(
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        tx_message: Sender<QuorumWaiterMessage>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
    ) {
        tokio::spawn(async move {
            Self {
                batch_size,
                max_batch_delay,
                rx_transaction,
                tx_message,
                workers_addresses,
                current_batch: Vec::with_capacity(batch_size * 2),
                current_batch_size: 0,
                network: ReliableSender::new(),
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(tx) = self.rx_transaction.recv() => {
                    self.current_batch_size += tx.len();
                    self.current_batch.push(tx);
                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        let size = self.current_batch_size;

        // Drain accumulator.
        self.current_batch_size = 0;
        let txs: Vec<Transaction> = self.current_batch.drain(..).collect();

        #[cfg(feature = "benchmark")]
        // Look for sample txs (tx_type=0 at byte 0) and gather their counter and account_id.
        // SmallBank format: [tx_type:1][client_id:1][tx_counter:8][src_account:8]...
        let sample_ids: Vec<_> = txs
            .iter()
            .filter(|tx| tx.len() >= 18 && tx[0] == 0u8)
            .filter_map(|tx| {
                let counter: [u8; 8] = tx[2..10].try_into().ok()?;
                let account_id = u64::from_be_bytes(tx[10..18].try_into().ok()?);
                Some((counter, account_id))
            })
            .collect();

        let message = WorkerMessage::Batch(txs);
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        // NOTE: This is one extra hash that is only needed to print the following log entries.
        let digest = Digest(
            Sha512::digest(&serialized).as_ref()[..32]
                .try_into()
                .unwrap(),
        );

        #[cfg(feature = "benchmark")]
        {

            for (id, account_id) in sample_ids {
                // NOTE: This log entry is used to compute performance.
                info!(
                    "Batch {:?} contains sample tx {} account {}",
                    digest,
                    u64::from_be_bytes(id),
                    account_id,
                );
            }
        }

        // NOTE: This log entry is used to compute performance.
        info!("Batch {:?} contains {} B", digest, size);

        // Broadcast the batch through the network.
        let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        let batch_sending_enqueue_at = std::time::Instant::now();
        let handlers = self.network.broadcast(addresses, bytes).await;

        // Send the batch through the deliver channel for further processing.
        self.tx_message
            .send(QuorumWaiterMessage {
                batch: serialized,
                handlers: names.into_iter().zip(handlers.into_iter()).collect(),
                batch_sending_enqueue_at,
            })
            .await
            .expect("Failed to deliver batch");
    }
}
