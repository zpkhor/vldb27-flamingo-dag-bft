// Copyright(C) Facebook, Inc. and its affiliates.
use config::WorkerId;
use crypto::Digest;
use std::collections::HashMap;
use store::Store;
use tokio::sync::mpsc::Receiver;

/// Receives batches' digests of other authorities. These are only needed to verify incoming
/// headers (ie. make sure we have their payload).
pub struct PayloadReceiver {
    /// The persistent storage.
    store: Store,
    /// Receives batches' digests from the network.
    rx_workers: Receiver<(Digest, WorkerId, HashMap<u32, u16>)>,
}

impl PayloadReceiver {
    pub fn spawn(store: Store, rx_workers: Receiver<(Digest, WorkerId, HashMap<u32, u16>)>) {
        tokio::spawn(async move {
            Self { store, rx_workers }.run().await;
        });
    }

    async fn run(&mut self) {
        while let Some((digest, worker_id, account_counts)) = self.rx_workers.recv().await {
            let key = [digest.as_ref(), &worker_id.to_le_bytes()].concat();
            let value = bincode::serialize(&account_counts).expect("Failed to serialize account_counts");
            self.store.write(key.to_vec(), value).await;
        }
    }
}
