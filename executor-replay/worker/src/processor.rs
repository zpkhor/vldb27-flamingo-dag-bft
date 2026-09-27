// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::SerializedBatchDigestMessage;
use config::WorkerId;
use crypto::Digest;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
#[cfg(feature = "benchmark")]
use log::info;
use primary::{QuorumMetrics, WorkerPrimaryMessage};
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

/// Indicates a serialized `WorkerMessage::Batch` message.
pub type SerializedBatchMessage = Vec<u8>;

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct Processor;

impl Processor {
    pub fn spawn(
        // Our worker's id.
        id: WorkerId,
        // The persistent storage.
        mut store: Store,
        // Input channel to receive batches.
        mut rx_batch: Receiver<(SerializedBatchMessage, QuorumMetrics)>,
        // Output channel to send out batches' digests.
        tx_digest: Sender<SerializedBatchDigestMessage>,
        // Whether we are processing our own batches or the batches of other nodes.
        own_digest: bool,
        // When true, account_counts are not computed (baseline mode).
        baseline_mode: bool,
    ) {
        tokio::spawn(async move {
            while let Some((batch, quorum_metrics)) = rx_batch.recv().await {
                // Hash the batch.
                let digest = Digest(Sha512::digest(&batch).as_ref()[..32].try_into().unwrap());

                // Compute account_counts from transactions.
                let account_counts = if baseline_mode {
                    std::collections::BTreeMap::new()
                } else {
                    match bincode::deserialize::<crate::worker::WorkerMessage>(&batch) {
                        Ok(crate::worker::WorkerMessage::Batch(txs)) => {
                            let mut counts: std::collections::BTreeMap<u32, u16> = std::collections::BTreeMap::new();
                            for tx in &txs {
                                if tx.len() >= 18 {
                                    let account_id = u64::from_be_bytes(tx[10..18].try_into().unwrap()) as u32;
                                    *counts.entry(account_id).or_insert(0) += 1;
                                }
                            }
                            counts
                        }
                        _ => std::collections::BTreeMap::new(),
                    }
                };

                // Store the batch.
                store.write(digest.to_vec(), batch).await;

                #[cfg(feature = "benchmark")]
                if own_digest {
                    info!("Processed batch {:?}", digest);
                }

                // Deliver the batch's digest.
                let message = match own_digest {
                    true => WorkerPrimaryMessage::OurBatch(digest, id, account_counts, quorum_metrics),
                    false => WorkerPrimaryMessage::OthersBatch(digest, id, account_counts),
                };
                let message = bincode::serialize(&message)
                    .expect("Failed to serialize our own worker-primary message");
                tx_digest
                    .send(message)
                    .await
                    .expect("Failed to send digest");
            }
        });
    }
}
