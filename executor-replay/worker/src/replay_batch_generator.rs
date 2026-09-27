use crate::worker::{SerializedBatchDigestMessage, WorkerMessage};
use config::replay;
use config::{Parameters, WorkerId};
use crypto::Digest;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use log::info;
use primary::WorkerPrimaryMessage;
use rand::Rng;
use rand::SeedableRng;
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::Sender;

pub struct ReplayBatchGenerator;

impl ReplayBatchGenerator {
    pub fn spawn(
        id: WorkerId,
        mut store: Store,
        parameters: Parameters,
        tx_primary: Sender<SerializedBatchDigestMessage>,
    ) {
        tokio::spawn(async move {
            let entries = replay::parse_replay_csv(&parameters.replay_csv)
                .expect("Failed to parse replay CSV");
            let assignments = replay::assign_batches_round_robin(
                &entries,
                parameters.num_workers,
                parameters.replay_tx_size,
            );

            // Filter to batches assigned to this worker.
            let my_assignments: Vec<_> = assignments
                .iter()
                .filter(|a| a.worker_id == id)
                .collect();

            info!(
                "ReplayBatchGenerator worker {}: generating {} batches (of {} total)",
                id,
                my_assignments.len(),
                assignments.len(),
            );

            let tx_size = parameters.replay_tx_size;
            let num_accounts = parameters.num_accounts;
            assert!(num_accounts > 0, "num_accounts must be > 0 for replay mode");

            let distributed_tx_rate = parameters.distributed_tx_rate;

            // Precompute executor sub-range shard boundaries and cumulative weights.
            // Shard i covers accounts [i*shard_size, (i+1)*shard_size), last shard extends to num_accounts.
            let skew_weights = &parameters.executor_skew_weights;
            let num_shards = skew_weights.len();
            let (shard_size, cumulative_weights): (u64, Vec<f64>) = if num_shards > 0 {
                assert!(
                    skew_weights.iter().all(|&w| w > 0.0),
                    "executor_skew_weights must all be positive"
                );
                let total: f64 = skew_weights.iter().sum();
                let mut cum = 0.0f64;
                let cw = skew_weights.iter().map(|w| { cum += w / total; cum }).collect();
                (num_accounts / num_shards as u64, cw)
            } else {
                (0, vec![])
            };

            for (count, assignment) in my_assignments.iter().enumerate() {
                let mut rng = rand::rngs::StdRng::seed_from_u64(assignment.batch_index);
                let mut txs: Vec<Vec<u8>> = Vec::with_capacity(assignment.num_tx);
                // Counter for cycling through non-SendPayment tx types: Balance(0),
                // DepositChecking(1), TransactSavings(2), WriteCheck(3).
                let mut non_dist_idx: u8 = 0;

                for tx_index in 0..assignment.num_tx as u64 {
                    let mut tx = vec![0u8; tx_size];

                    // Standard header: [tx_type:1][client_id:1][tx_counter:8]
                    tx[0] = 1; // tx_type = 1 (regular, non-sample)
                    tx[1] = 0; // client_id = 0
                    // Unique counter per tx to avoid TxID collisions in state transfer maps.
                    let tx_counter = assignment.batch_index * assignment.num_tx as u64 + tx_index;
                    tx[2..10].copy_from_slice(&tx_counter.to_be_bytes());

                    // Select source account: skewed sub-range or uniform.
                    let src_account: u64 = if num_shards > 0 {
                        let p: f64 = rng.gen();
                        let shard_idx = cumulative_weights.iter().position(|&c| p < c)
                            .unwrap_or(num_shards - 1);
                        let shard_start = shard_idx as u64 * shard_size;
                        let shard_count = if shard_idx == num_shards - 1 {
                            num_accounts - shard_idx as u64 * shard_size
                        } else {
                            shard_size
                        };
                        shard_start + rng.gen_range(0, shard_count)
                    } else {
                        rng.gen_range(0, num_accounts)
                    };

                    // Decide cross-executor (SendPayment) vs single-account tx.
                    let is_distributed: bool = rng.gen::<f64>() < distributed_tx_rate;
                    let (sb_tx_type, dest_account) = if is_distributed {
                        // SendPayment: dest uniform from all accounts, different from src.
                        let dest = loop {
                            let d = rng.gen_range(0, num_accounts);
                            if d != src_account || num_accounts <= 1 {
                                break d;
                            }
                        };
                        (4u8, dest)
                    } else {
                        let t = non_dist_idx % 4;
                        non_dist_idx = non_dist_idx.wrapping_add(1);
                        (t, src_account)
                    };

                    let amount: f64 = match sb_tx_type {
                        1 => 1.3,   // DepositChecking
                        2 => 20.20, // TransactSavings
                        3 => 5.0,   // WriteCheck
                        4 => 5.0,   // SendPayment
                        _ => 0.0,   // Balance
                    };

                    tx[10..18].copy_from_slice(&src_account.to_be_bytes());
                    tx[18..26].copy_from_slice(&dest_account.to_be_bytes());
                    tx[26] = sb_tx_type;
                    tx[27..35].copy_from_slice(&amount.to_be_bytes());

                    txs.push(tx);
                }

                // Serialize as WorkerMessage::Batch (same format as BatchMaker/Processor)
                let serialized = bincode::serialize(&WorkerMessage::Batch(txs))
                    .expect("Failed to serialize batch");

                // Compute digest identically to Processor
                let digest = Digest(
                    Sha512::digest(&serialized).as_ref()[..32]
                        .try_into()
                        .unwrap(),
                );

                // Store the batch
                store.write(digest.to_vec(), serialized).await;

                // Notify primary
                let message = WorkerPrimaryMessage::ReplayBatchReady(
                    assignment.batch_index,
                    digest,
                    id,
                );
                let serialized_msg = bincode::serialize(&message)
                    .expect("Failed to serialize ReplayBatchReady");
                tx_primary
                    .send(serialized_msg)
                    .await
                    .expect("Failed to send ReplayBatchReady to primary");

                if (count + 1) % 500 == 0 {
                    info!(
                        "ReplayBatchGenerator worker {}: generated {}/{} batches",
                        id,
                        count + 1,
                        my_assignments.len(),
                    );
                }
            }

            info!(
                "ReplayBatchGenerator worker {}: finished generating all {} batches",
                id,
                my_assignments.len(),
            );
        });
    }
}
