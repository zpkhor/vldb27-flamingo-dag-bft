// Copyright(C) Facebook, Inc. and its affiliates.
use crypto::{generate_production_keypair, PublicKey, SecretKey};
use log::info;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::BufWriter;
use std::io::Write as _;
use std::net::SocketAddr;
pub mod replay;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Node {0} is not in the committee")]
    NotInCommittee(PublicKey),

    #[error("Unknown worker id {0}")]
    UnknownWorker(WorkerId),

    #[error("Unknown executor id {0}")]
    UnknownExecutor(ExecutorId),

    #[error("Failed to read config file '{file}': {message}")]
    ImportError { file: String, message: String },

    #[error("Failed to write config file '{file}': {message}")]
    ExportError { file: String, message: String },
}

pub trait Import: DeserializeOwned {
    fn import(path: &str) -> Result<Self, ConfigError> {
        let reader = || -> Result<Self, std::io::Error> {
            let data = fs::read(path)?;
            Ok(serde_json::from_slice(data.as_slice())?)
        };
        reader().map_err(|e| ConfigError::ImportError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }
}

pub trait Export: Serialize {
    fn export(&self, path: &str) -> Result<(), ConfigError> {
        let writer = || -> Result<(), std::io::Error> {
            let file = OpenOptions::new().create(true).write(true).open(path)?;
            let mut writer = BufWriter::new(file);
            let data = serde_json::to_string_pretty(self).unwrap();
            writer.write_all(data.as_ref())?;
            writer.write_all(b"\n")?;
            Ok(())
        };
        writer().map_err(|e| ConfigError::ExportError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }
}

/// Sharding strategy for account states distribution among batch executors
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Copy)]
pub enum ShardingStrategy {
    Range,
    Hash,
}

impl ShardingStrategy {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "hash" => ShardingStrategy::Hash,
            _ => ShardingStrategy::Range,
        }
    }
}

pub type Stake = u32;
pub type WorkerId = u32;
pub type ExecutorId = u32;

#[derive(Deserialize, Clone)]
pub struct Parameters {
    /// The preferred header size. The primary creates a new header when it has enough parents and
    /// enough batches' digests to reach `header_size`. Denominated in bytes.
    pub header_size: usize,
    /// The maximum delay that the primary waits between generating two headers, even if the header
    /// did not reach `max_header_size`. Denominated in ms.
    pub max_header_delay: u64,
    /// The depth of the garbage collection (Denominated in number of rounds).
    pub gc_depth: u64,
    /// The delay after which the synchronizer retries to send sync requests. Denominated in ms.
    pub sync_retry_delay: u64,
    /// Determine with how many nodes to sync when re-trying to send sync-request. These nodes
    /// are picked at random from the committee.
    pub sync_retry_nodes: usize,
    /// The preferred batch size. The workers seal a batch of transactions when it reaches this size.
    /// Denominated in bytes.
    pub batch_size: usize,
    /// The delay after which the workers seal a batch of transactions, even if `max_batch_size`
    /// is not reached. Denominated in ms.
    pub max_batch_delay: u64,
    /// When true, disables account_counts tracking and rerouting computation.
    #[serde(default)]
    pub baseline_mode: bool,
    /// Total number of SmallBank accounts.
    #[serde(default)]
    pub num_accounts: u64,
    /// Number of executors per validator.
    #[serde(default)]
    pub num_executors: u32,
    /// Initial account balance range (min).
    #[serde(default)]
    pub min_balance: i64,
    /// Initial account balance range (max).
    #[serde(default)]
    pub max_balance: i64,
    /// Sharding strategy: "range" or "hash".
    #[serde(default = "default_sharding_strategy")]
    pub sharding_strategy: String,
    /// Use new scheduler (dynamic load-aware scheduling).
    #[serde(default)]
    pub use_new_scheduler: bool,
    /// Exclude SendPayment transactions (single-account txs only).
    #[serde(default)]
    pub no_send_payment_tx: bool,
    /// Use writeback executor (old distributed tx executor) instead of data fusion executor
    #[serde(default)]
    pub use_writeback_executor: bool,
    /// WAN bandwidth limit in bytes/sec per connection (0 = unlimited).
    #[serde(default = "default_wan_bandwidth")]
    pub wan_bandwidth: u64,
    /// Path to replay CSV file. Non-empty enables replay mode.
    #[serde(default)]
    pub replay_csv: String,
    /// Transaction size for replay batch generation.
    #[serde(default = "default_replay_tx_size")]
    pub replay_tx_size: usize,
    /// Number of workers per validator (for replay batch assignment).
    #[serde(default = "default_num_workers")]
    pub num_workers: u32,
    /// Executor account skew weights for replay batch generation (length == num_executors).
    /// Each weight controls the probability of selecting accounts from that executor's shard.
    /// Empty = uniform sampling across all accounts.
    #[serde(default)]
    pub executor_skew_weights: Vec<f64>,
    /// Fraction of replay transactions that are cross-executor (SendPayment). 0.0..=1.0.
    #[serde(default = "default_distributed_tx_rate")]
    pub distributed_tx_rate: f64,
}

fn default_sharding_strategy() -> String {
    "range".to_string()
}

fn default_wan_bandwidth() -> u64 {
    300_000_000 // 300 MB/s default WAN bandwidth limit
}

fn default_replay_tx_size() -> usize {
    512
}

fn default_num_workers() -> u32 {
    1
}

fn default_distributed_tx_rate() -> f64 {
    0.0
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            header_size: 1_000,
            max_header_delay: 100,
            gc_depth: 50,
            sync_retry_delay: 5_000,
            sync_retry_nodes: 3,
            batch_size: 500_000,
            max_batch_delay: 100,
            baseline_mode: false,
            num_accounts: 0,
            num_executors: 0,
            min_balance: 0,
            max_balance: 0,
            sharding_strategy: default_sharding_strategy(),
            use_new_scheduler: false,
            no_send_payment_tx: false,
            use_writeback_executor: false,
            wan_bandwidth: default_wan_bandwidth(),
            replay_csv: String::new(),
            replay_tx_size: default_replay_tx_size(),
            executor_skew_weights: Vec::new(),
            distributed_tx_rate: default_distributed_tx_rate(),
            num_workers: default_num_workers(),
        }
    }
}

impl Import for Parameters {}

impl Parameters {
    pub fn log(&self) {
        info!("Header size set to {} B", self.header_size);
        info!("Max header delay set to {} ms", self.max_header_delay);
        info!("Garbage collection depth set to {} rounds", self.gc_depth);
        info!("Sync retry delay set to {} ms", self.sync_retry_delay);
        info!("Sync retry nodes set to {} nodes", self.sync_retry_nodes);
        info!("Batch size set to {} B", self.batch_size);
        info!("Max batch delay set to {} ms", self.max_batch_delay);
        if self.num_accounts > 0 {
            info!("SmallBank: {} accounts across {} executors", self.num_accounts, self.num_executors);
            info!("SmallBank: Balance range [{}, {}]", self.min_balance, self.max_balance);
            info!("SmallBank: Sharding strategy: {}", self.sharding_strategy);
            info!("SmallBank: no_send_payment_tx={}", self.no_send_payment_tx);
        }
        if !self.replay_csv.is_empty() {
            info!("Replay mode: CSV={}, tx_size={}, num_workers={}", self.replay_csv, self.replay_tx_size, self.num_workers);
            info!("Replay: distributed_tx_rate={:.1}%", self.distributed_tx_rate * 100.0);
            if !self.executor_skew_weights.is_empty() {
                info!("Replay: executor_skew_weights={:?}", self.executor_skew_weights);
            }
        }
    }
}

#[derive(Clone, Deserialize)]
pub struct PrimaryAddresses {
    /// Address to receive messages from other primaries (WAN).
    pub primary_to_primary: SocketAddr,
    /// Address to receive messages from our workers (LAN).
    pub worker_to_primary: SocketAddr,
    /// Address to receive feedback from our executors (LAN).
    pub executor_to_primary: SocketAddr,
}

#[derive(Clone, Deserialize, Eq, Hash, PartialEq)]
pub struct ExecutorAddresses {
    /// Address to receive execution requests from workers.
    pub worker_to_executor: SocketAddr,
    /// Address to receive state transfers from other executors.
    pub executor_to_executor: SocketAddr,
}

#[derive(Clone, Deserialize, Eq, Hash, PartialEq)]
pub struct WorkerAddresses {
    /// Address to receive client transactions (WAN).
    pub transactions: SocketAddr,
    /// Address to receive messages from other workers (WAN).
    pub worker_to_worker: SocketAddr,
    /// Address to receive messages from our primary (LAN).
    pub primary_to_worker: SocketAddr,
}

#[derive(Clone, Deserialize)]
pub struct Authority {
    /// The voting power of this authority.
    pub stake: Stake,
    /// The network addresses of the primary.
    pub primary: PrimaryAddresses,
    /// Map of workers' id and their network addresses.
    pub workers: HashMap<WorkerId, WorkerAddresses>,
    /// Map of executors' id and their network addresses.
    #[serde(default)]
    pub executors: HashMap<ExecutorId, ExecutorAddresses>,
    /// Address to send commit replies to the client.
    pub client_reply: SocketAddr,
    /// Estimated max request throughput (requests/sec)
    /// Used as initial capacity; may be updated via control plane consensus.
    #[serde(default)]
    pub capacity_by_bw: u64,
}

#[derive(Clone, Deserialize)]
pub struct Committee {
    pub authorities: BTreeMap<PublicKey, Authority>,
    #[serde(default)]
    pub latency_matrix: BTreeMap<PublicKey, BTreeMap<PublicKey, u64>>,
    /// Per-validator (account_start, account_count), keyed by validator public key.
    #[serde(default)]
    pub account_ranges: BTreeMap<PublicKey, (u64, u64)>,
    /// Client reply addresses keyed by client_id (u8).
    #[serde(default)]
    pub client_reply_addresses: HashMap<u8, SocketAddr>,
}

impl Import for Committee {}

impl Committee {
    /// Returns the public key of the validator whose account_range contains `account_id`, or None.
    pub fn home_validator(&self, account_id: u64) -> Option<PublicKey> {
        self.account_ranges.iter().find_map(|(pk, &(start, count))| {
            if account_id >= start && account_id < start + count { Some(*pk) } else { None }
        })
    }

    /// Returns the number of authorities.
    pub fn size(&self) -> usize {
        self.authorities.len()
    }

    /// Return the stake of a specific authority.
    pub fn stake(&self, name: &PublicKey) -> Stake {
        self.authorities.get(&name).map_or_else(|| 0, |x| x.stake)
    }

    /// Returns the stake of all authorities except `myself`.
    pub fn others_stake(&self, myself: &PublicKey) -> Vec<(PublicKey, Stake)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, authority)| (*name, authority.stake))
            .collect()
    }

    /// Returns the stake required to reach a quorum (2f+1).
    pub fn quorum_threshold(&self) -> Stake {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (2 N + 3) / 3 = 2f + 1 + (2k + 2)/3 = 2f + 1 + k = N - f
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        2 * total_votes / 3 + 1
    }

    /// Returns the stake required to reach availability (f+1).
    pub fn validity_threshold(&self) -> Stake {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (N + 2) / 3 = f + 1 + k/3 = f + 1
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        (total_votes + 2) / 3
    }

    /// Returns the client reply address of the target authority.
    pub fn client_reply(&self, to: &PublicKey) -> Result<SocketAddr, ConfigError> {
        self.authorities
            .get(to)
            .map(|x| x.client_reply)
            .ok_or_else(|| ConfigError::NotInCommittee(*to))
    }

    /// Returns the primary addresses of the target primary.
    pub fn primary(&self, to: &PublicKey) -> Result<PrimaryAddresses, ConfigError> {
        self.authorities
            .get(to)
            .map(|x| x.primary.clone())
            .ok_or_else(|| ConfigError::NotInCommittee(*to))
    }

    /// Returns the addresses of all primaries except `myself`.
    pub fn others_primaries(&self, myself: &PublicKey) -> Vec<(PublicKey, PrimaryAddresses)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, authority)| (*name, authority.primary.clone()))
            .collect()
    }

    /// Returns the addresses of a specific worker (`id`) of a specific authority (`to`).
    pub fn worker(&self, to: &PublicKey, id: &WorkerId) -> Result<WorkerAddresses, ConfigError> {
        self.authorities
            .iter()
            .find(|(name, _)| name == &to)
            .map(|(_, authority)| authority)
            .ok_or_else(|| ConfigError::NotInCommittee(*to))?
            .workers
            .iter()
            .find(|(worker_id, _)| worker_id == &id)
            .map(|(_, worker)| worker.clone())
            .ok_or_else(|| ConfigError::NotInCommittee(*to))
    }

    /// Returns the addresses of all our workers.
    pub fn our_workers(&self, myself: &PublicKey) -> Result<Vec<WorkerAddresses>, ConfigError> {
        self.authorities
            .iter()
            .find(|(name, _)| name == &myself)
            .map(|(_, authority)| authority)
            .ok_or_else(|| ConfigError::NotInCommittee(*myself))?
            .workers
            .values()
            .cloned()
            .map(Ok)
            .collect()
    }

    /// Returns the addresses of a specific executor (`id`) of a specific authority (`to`).
    pub fn executor(&self, to: &PublicKey, id: &ExecutorId) -> Result<ExecutorAddresses, ConfigError> {
        self.authorities
            .iter()
            .find(|(name, _)| name == &to)
            .map(|(_, authority)| authority)
            .ok_or_else(|| ConfigError::NotInCommittee(*to))?
            .executors
            .iter()
            .find(|(executor_id, _)| executor_id == &id)
            .map(|(_, executor)| executor.clone())
            .ok_or_else(|| ConfigError::UnknownExecutor(*id))
    }

    /// Returns the reply address for a specific client_id.
    pub fn client_reply_address(&self, client_id: u8) -> Option<&SocketAddr> {
        self.client_reply_addresses.get(&client_id)
    }

    /// Returns the addresses of all workers with a specific id except the ones of the authority
    /// specified by `myself`.
    pub fn others_workers(
        &self,
        myself: &PublicKey,
        id: &WorkerId,
    ) -> Vec<(PublicKey, WorkerAddresses)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .filter_map(|(name, authority)| {
                authority
                    .workers
                    .iter()
                    .find(|(worker_id, _)| worker_id == &id)
                    .map(|(_, addresses)| (*name, addresses.clone()))
            })
            .collect()
    }
}

#[derive(Serialize, Deserialize)]
pub struct KeyPair {
    /// The node's public key (and identifier).
    pub name: PublicKey,
    /// The node's secret key.
    pub secret: SecretKey,
}

impl Import for KeyPair {}
impl Export for KeyPair {}

impl KeyPair {
    pub fn new() -> Self {
        let (name, secret) = generate_production_keypair();
        Self { name, secret }
    }
}

impl Default for KeyPair {
    fn default() -> Self {
        Self::new()
    }
}

/// Account→ExecutorId mapping. Updated deterministically by the scheduler.
pub type Partition = HashMap<u64, ExecutorId>;

/// Creates an initial Partition based on sharding strategy.
/// validator_ranges is (account_start, account_count) for every validator.
/// For each validator's range, executor i owns the i-th equal slice of that range.
/// The partition covers all accounts globally so executors can handle batches from any validator.
pub fn create_initial_partition(
    num_executors: u32,
    validator_ranges: &[(u64, u64)],
    strategy: ShardingStrategy,
) -> Partition {
    let mut partition: Partition = HashMap::new();
    for &(account_start, account_count) in validator_ranges {
        for account_id in account_start..account_start + account_count {
            let executor_id = compute_worker_for_account(
                account_id - account_start,
                num_executors,
                account_count,
                strategy,
            );
            partition.insert(account_id, executor_id);
        }
    }
    partition
}

/// Compute the initial shard range [start, end) for an executor at startup.
pub fn compute_initial_shard_range(executor_id: u32, num_executors: u32, num_accounts: u64) -> (u64, u64) {
    if num_executors == 0 {
        return (0, 0);
    }
    let accounts_per_shard = (num_accounts + num_executors as u64 - 1) / num_executors as u64;
    let start = executor_id as u64 * accounts_per_shard;
    let end = ((executor_id + 1) as u64 * accounts_per_shard).min(num_accounts);
    (start, end)
}

fn hash_account_id(account_id: u64) -> u64 {
    let mut hasher = DefaultHasher::new();
    account_id.hash(&mut hasher);
    hasher.finish()
}

pub fn compute_worker_for_account(
    account_id: u64,
    num_workers: u32,
    num_accounts: u64,
    strategy: ShardingStrategy,
) -> u32 {
    match strategy {
        ShardingStrategy::Range => {
            if num_workers == 0 {
                return 0;
            }
            let accounts_per_shard = (num_accounts + num_workers as u64 - 1) / num_workers as u64;
            (account_id / accounts_per_shard).min(num_workers as u64 - 1) as u32
        }
        ShardingStrategy::Hash => {
            if num_workers == 0 {
                return 0;
            }
            (hash_account_id(account_id) % num_workers as u64) as u32
        }
    }
}
