use crate::primary::PrimaryWorkerMessage;
use bytes::Bytes;
use config::{Committee, Parameters, WorkerId};
use config::replay::{self, ReplayAssignment};
use crypto::{Digest, PublicKey};
use log::info;
use network::SimpleSender;
use std::collections::HashMap;
use tokio::sync::mpsc::Receiver;
use tokio::time::{Duration, Instant};

/// Replays batches from a CSV recording by pacing Execute messages to workers
/// based on recorded timestamps. Replaces Consensus + BatchDispatcher in replay mode.
pub struct ReplaySequencer {
    name: PublicKey,
    committee: Committee,
    /// All batch assignments (batch_index -> worker_id, num_tx, timestamp)
    assignments: Vec<ReplayAssignment>,
    /// Receives (batch_index, digest, worker_id) from workers
    rx_replay_ready: Receiver<(u64, Digest, WorkerId)>,
    network: SimpleSender,
}

impl ReplaySequencer {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        rx_replay_ready: Receiver<(u64, Digest, WorkerId)>,
    ) {
        tokio::spawn(async move {
            let entries = replay::parse_replay_csv(&parameters.replay_csv)
                .expect("Failed to parse replay CSV");
            let assignments = replay::assign_batches_round_robin(
                &entries,
                parameters.num_workers,
                parameters.replay_tx_size,
            );

            info!(
                "ReplaySequencer: loaded {} batch entries from CSV",
                assignments.len()
            );

            Self {
                name,
                committee,
                assignments,
                rx_replay_ready,
                network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let total_batches = self.assignments.len();
        if total_batches == 0 {
            info!("ReplaySequencer: no batches to replay");
            return;
        }

        // Phase 1: Collect all batch digests from workers
        info!(
            "ReplaySequencer: waiting for {} batch digests from workers...",
            total_batches
        );
        let mut batch_digests: HashMap<u64, (WorkerId, Digest)> = HashMap::new();
        while batch_digests.len() < total_batches {
            match self.rx_replay_ready.recv().await {
                Some((index, digest, worker_id)) => {
                    batch_digests.insert(index, (worker_id, digest));
                    if batch_digests.len() % 500 == 0 {
                        info!(
                            "ReplaySequencer: received {}/{} digests",
                            batch_digests.len(),
                            total_batches
                        );
                    }
                }
                None => {
                    info!("ReplaySequencer: channel closed, received {}/{} digests",
                        batch_digests.len(), total_batches);
                    return;
                }
            }
        }
        info!(
            "ReplaySequencer: all {} digests received, starting replay",
            total_batches
        );

        // Phase 2: Replay with timing
        let base_timestamp = self.assignments[0].timestamp_ms;
        let start_time = Instant::now();
        let mut sequence: u64 = 0;

        for assignment in &self.assignments {
            // Pace based on recorded timestamps
            let target_elapsed =
                Duration::from_millis(assignment.timestamp_ms - base_timestamp);
            let actual_elapsed = start_time.elapsed();
            if target_elapsed > actual_elapsed {
                tokio::time::sleep(target_elapsed - actual_elapsed).await;
            }

            let (worker_id, digest) = batch_digests
                .get(&assignment.batch_index)
                .expect("Missing digest for batch index");

            let message =
                PrimaryWorkerMessage::Execute(digest.clone(), *worker_id, sequence);
            let serialized =
                bincode::serialize(&message).expect("Failed to serialize Execute");

            let worker_address = self
                .committee
                .worker(&self.name, worker_id)
                .expect("Worker not in committee")
                .primary_to_worker;

            self.network
                .send(worker_address, Bytes::from(serialized))
                .await;

            info!(
                "Replay Execute seq={} worker={} num_tx={}",
                sequence, worker_id, assignment.num_tx
            );

            sequence += 1;
        }

        info!(
            "ReplaySequencer: completed replay of {} batches (sequences 0..{})",
            total_batches,
            sequence - 1
        );
    }
}
