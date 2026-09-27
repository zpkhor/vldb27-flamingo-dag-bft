// Copyright(C) Facebook, Inc. and its affiliates.
use crate::processor::SerializedBatchMessage;
use config::{Committee, Stake};
#[cfg(feature = "benchmark")]
use crypto::Digest;
use crypto::PublicKey;
#[cfg(feature = "benchmark")]
use ed25519_dalek::{Digest as _, Sha512};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
#[cfg(feature = "benchmark")]
use log::info;
use network::CancelHandler;
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use std::time::Instant;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/quorum_waiter_tests.rs"]
pub mod quorum_waiter_tests;

#[derive(Debug)]
pub struct QuorumWaiterMessage {
    /// A serialized `WorkerMessage::Batch` message.
    pub batch: SerializedBatchMessage,
    /// The cancel handlers to receive the acknowledgements of our broadcast.
    pub handlers: Vec<(PublicKey, CancelHandler)>,
    pub batch_sending_enqueue_at: Instant,
}

/// The QuorumWaiter waits for 2f authorities to acknowledge reception of a batch.
pub struct QuorumWaiter {
    /// The committee information.
    committee: Committee,
    /// The stake of this authority.
    stake: Stake,
    /// Input Channel to receive commands.
    rx_message: Receiver<QuorumWaiterMessage>,
    /// Channel to deliver batches for which we have enough acknowledgements.
    tx_batch: Sender<(SerializedBatchMessage, u64)>,
}

impl QuorumWaiter {
    /// Spawn a new QuorumWaiter.
    pub fn spawn(
        committee: Committee,
        stake: Stake,
        rx_message: Receiver<QuorumWaiterMessage>,
        tx_batch: Sender<(Vec<u8>, u64)>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                stake,
                rx_message,
                tx_batch,
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a future to complete and then delivers a value.
    async fn waiter(wait_for: CancelHandler, deliver: Stake) -> Stake {
        let _ = wait_for.await;
        deliver
    }

    /// Main loop.
    async fn run(&mut self) {
        while let Some(QuorumWaiterMessage { batch, handlers, batch_sending_enqueue_at }) = self.rx_message.recv().await {
            let quorum_start_at = Instant::now();
            let queue_delay_ms = quorum_start_at.duration_since(batch_sending_enqueue_at).as_millis() as u64;

            let mut wait_for_quorum: FuturesUnordered<_> = handlers
                .into_iter()
                .map(|(name, handler)| {
                    let stake = self.committee.stake(&name);
                    Self::waiter(handler, stake)
                })
                .collect();

            // Wait for the first 2f nodes to send back an Ack. Then we consider the batch
            // delivered and we send its digest to the primary (that will include it into
            // the dag). This should reduce the amount of synching.
            let mut total_stake = self.stake;
            while let Some(stake) = wait_for_quorum.next().await {
                total_stake += stake;
                if total_stake >= self.committee.quorum_threshold() {
                    #[cfg(feature = "benchmark")]
                    {
                        let quorum_latency_ms = quorum_start_at.elapsed().as_millis() as u64;
                        let digest = Digest(
                            Sha512::digest(&batch).as_ref()[..32].try_into().unwrap(),
                        );
                        info!("Quorum for batch {:?} queue_delay {}ms quorum_latency {}ms", digest, queue_delay_ms, quorum_latency_ms);
                    }
                    self.tx_batch
                        .send((batch, queue_delay_ms))
                        .await
                        .expect("Failed to deliver batch");
                    break;
                }
            }
        }
    }
}
