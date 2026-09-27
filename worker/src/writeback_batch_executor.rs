// Copyright(C) Facebook, Inc. and its affiliates.
// Adapted from origin/old-executor-patch:worker/src/batch_executor.rs
use crate::batch_executor::ClientReplyRequest;
use crate::state_helper::{StateTransfer, StateTransferRequest};
use crate::writeback_state_helper::{OutgoingStateInfo, StateWritebackArrival, StateWritebackRequest};
use crate::transaction::{Transaction, SAMPLE_TX_TYPE, TxID};
use crate::workload::{AccountState, AccountStore, WorkloadType};
use config::{Committee, Partition};
use crypto::{Digest, PublicKey};
use log::{debug, info, warn};
use core::panic;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{Receiver, Sender};

/// Flow control feedback interval
const FEEDBACK_INTERVAL: u64 = 100;

/// Buffered batch waiting for its sequence number to be executed.
struct BufferedBatch {
    digest: Digest,
    transactions: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, Copy)]
enum TransactionLocality {
    Local,
    DistributedSource { src_account: u64, dest_executor_id: u32 },
    DistributedDest { dest_account: u64, src_executor_id: u32 },
}

/// Transaction waiting for locked accounts to become available.
#[derive(Clone)]
struct PendingTransaction {
    /// Raw transaction bytes for re-execution
    tx_bytes: Vec<u8>,
    /// Parsed transaction header (avoid re-parsing)
    tx_header: crate::transaction::TransactionHeader,
    /// Parsed SmallBank payload
    sb_tx: crate::workload::SmallBankTransaction,
    /// Batch digest this transaction belongs to
    batch_digest: Digest,
    /// Required accounts (1 or 2 depending on transaction type)
    required_accounts: Vec<u64>,
    locality: TransactionLocality,
}

/// Metrics for lock contention analysis.
#[derive(Debug)]
struct LockStatistics {
    /// Total transactions attempted (regardless whether using locks)
    total_transactions: u64,
    /// Transactions that hit locks and were queued
    lock_hits: u64,
    /// Transactions that executed immediately
    eager_executions: u64,
    /// Current total queue depth across all accounts
    current_queue_depth: usize,
    /// Maximum queue depth observed for any single account
    max_single_queue_depth: usize,
    /// Maximum total queue depth observed
    max_total_queue_depth: usize,
}

impl LockStatistics {
    fn new() -> Self {
        Self {
            total_transactions: 0,
            lock_hits: 0,
            eager_executions: 0,
            current_queue_depth: 0,
            max_single_queue_depth: 0,
            max_total_queue_depth: 0,
        }
    }
}

/// Handles the execution of committed batches with strict sequence ordering.
pub struct DistributedTxExecutor {
    /// Receives execute requests from both local Router and remote workers (merged channel).
    rx_batch_executor: Receiver<(Digest, u64, Vec<Vec<u8>>)>,
    /// Sends client reply requests to ClientReplier.
    tx_client_reply: Sender<ClientReplyRequest>,
    /// Committee configuration for client reply address lookup.
    committee: Committee,
    /// Workload type configured at worker initialization (static per deployment).
    workload_type: WorkloadType,
    /// In-memory account store for SmallBank workload (None for Default workload).
    account_store: Option<AccountStore>,
    /// Next expected sequence number.
    next_sequence: u64,
    /// Buffer for out-of-order batches.
    buffer: BTreeMap<u64, BufferedBatch>,
    /// Executor ID for logging and tracking purposes.
    executor_id: u32,
    /// Node ID extracted from key file.
    _node_id: usize,
    states_partition: Option<Partition>,
    /// Tracks which transaction currently holds each account lock.
    account_locks: HashMap<u64, TxID>,
    /// Per-account pending queues for transactions waiting on locked accounts.
    pending_queues: HashMap<u64, VecDeque<PendingTransaction>>,
    /// Locally drained incoming transfers (from shared buffer), keyed by TxID.
    local_transfers: HashMap<TxID, StateTransfer>,
    /// Locally drained incoming writebacks (from shared buffer), keyed by TxID.
    local_writebacks: HashMap<TxID, StateWritebackArrival>,
    /// Reverse lookup: tx_id → dest_account_id for DistributedDest txs (O(1) lookup in process_incoming_transfers).
    distributed_dest_accounts: HashMap<TxID, u64>,
    /// Lock contention metrics for overhead analysis.
    lock_stats: LockStatistics,
    /// Authority's public key (for StateHelper address lookup).
    name: PublicKey,
    /// Channel to send StateTransfer requests to StateHelper.
    tx_send_state: Sender<StateTransferRequest>,
    /// Channel to send StateWriteback requests to StateHelper.
    tx_state_writeback: Sender<StateWritebackRequest>,
    /// Shared buffer for outgoing states awaiting writeback (Source side).
    outgoing_states_buffer: Arc<Mutex<HashMap<TxID, OutgoingStateInfo>>>,
    /// Shared buffer for incoming StateTransfers (Dest side).
    incoming_transfers: Arc<Mutex<HashMap<TxID, StateTransfer>>>,
    /// Shared buffer for incoming StateWritebacks (Source side).
    incoming_writebacks: Arc<Mutex<HashMap<TxID, StateWritebackArrival>>>,
    /// Channel to send execution feedback to StateHelper (for forwarding to Primary).
    tx_feedback: Sender<(u32, u64)>,
    /// Last feedback sequence sent (to avoid redundant sends).
    last_feedback_sent: u64,
    /// Cumulative count of actually executed transactions.
    executed_tx_count: u64,
}

impl DistributedTxExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        rx_batch_executor: Receiver<(Digest, u64, Vec<Vec<u8>>)>,
        tx_client_reply: Sender<ClientReplyRequest>,
        committee: Committee,
        workload_type: WorkloadType,
        num_accounts: u64,
        num_workers: u32,
        min_balance: i64,
        max_balance: i64,
        executor_id: u32,
        node_id: usize,
        initial_partition: Option<Partition>,
        name: PublicKey,
        tx_send_state: Sender<StateTransferRequest>,
        tx_state_writeback: Sender<StateWritebackRequest>,
        outgoing_states_buffer: Arc<Mutex<HashMap<TxID, OutgoingStateInfo>>>,
        incoming_transfers: Arc<Mutex<HashMap<TxID, StateTransfer>>>,
        incoming_writebacks: Arc<Mutex<HashMap<TxID, StateWritebackArrival>>>,
        tx_feedback: Sender<(u32, u64)>,
    ) {
        // Initialize account_store based on workload type
        let account_store = if workload_type == WorkloadType::SmallBank {
            let mut store = AccountStore::new();

            if let Some(ref partition) = initial_partition {
                // Use the provided partition to determine which accounts belong to this worker
                for (&account_id, &owner_id) in partition.iter() {
                    if owner_id == executor_id {
                        store.insert(
                            account_id,
                            crate::workload::create_account_with_seed(
                                account_id,
                                min_balance,
                                max_balance,
                            ),
                        );
                    }
                }
            }

            info!(
                "Executor executor {}: loaded {} accounts",
                executor_id,
                store.len(),
            );

            if let Some(ref partition) = initial_partition {
                Self::check_partition_consistency(executor_id, partition, &store);
            }

            Some(store)
        } else {
            None
        };

        info!(
            "DistributedTxExecutor {} initialized: workload={:?}, accounts={}, workers={}",
            executor_id, workload_type, num_accounts, num_workers
        );

        tokio::spawn(async move {
            Self {
                rx_batch_executor,
                tx_client_reply,
                committee,
                workload_type,
                account_store,
                next_sequence: 0,
                buffer: BTreeMap::new(),
                executor_id,
                _node_id: node_id,
                states_partition: initial_partition,
                executed_tx_count: 0,
                account_locks: HashMap::new(),
                pending_queues: HashMap::new(),
                local_transfers: HashMap::new(),
                local_writebacks: HashMap::new(),
                distributed_dest_accounts: HashMap::new(),
                lock_stats: LockStatistics::new(),
                name,
                tx_send_state,
                tx_state_writeback,
                outgoing_states_buffer,
                incoming_transfers,
                incoming_writebacks,
                tx_feedback,
                last_feedback_sent: 0,
            }
            .run()
            .await;
        });
    }

    /// Verifies that the accounts in the store matches the accounts assigned to this executor in the partition.
    fn check_partition_consistency(
        executor_id: u32,
        partition: &Partition,
        store: &AccountStore,
    ) {
        for account in store.keys() { // TODO
            let owner = partition.get(account).expect("Account missing in partition");
            if *owner != executor_id {
                panic!(
                    "Partition inconsistency: account {} owned by {} but found in executor {}'s store",
                    account, owner, executor_id,
                );
            }
        }
    }

    async fn run(&mut self) {
        while let Some((digest, sequence, transactions)) = self.rx_batch_executor.recv().await {
            self.process_execute(digest, sequence, transactions).await;
        }
    }

    async fn process_execute(&mut self, digest: Digest, sequence: u64, transactions: Vec<Vec<u8>>) {
        if sequence == self.next_sequence {
            self.execute_batch(&digest, &transactions).await;
            self.next_sequence += 1;
            self.send_feedback_if_needed().await;

            while let Some(buffered) = self.buffer.remove(&self.next_sequence) {
                self.execute_batch(&buffered.digest, &buffered.transactions).await;
                self.next_sequence += 1;
                self.send_feedback_if_needed().await;
            }
        } else if sequence > self.next_sequence {
            if transactions.is_empty() && digest == Digest::default() {
                warn!(
                    "Buffering sequence sync marker (seq={}), expecting seq={}, buffer_size={}",
                    sequence,
                    self.next_sequence,
                    self.buffer.len() + 1
                );
            } else {
                warn!(
                    "Buffering batch {:?} (seq={}), expecting seq={}, buffer_size={}",
                    digest,
                    sequence,
                    self.next_sequence,
                    self.buffer.len() + 1
                );
            }

            if self.buffer.len() >= 5000 {
                warn!("Panicking");
                panic!(
                    "DistributedTxExecutor buffer exceeded limit of 5000 batches! \
                     Executor is stuck waiting for sequence {} but received up to seq={}. \
                     Buffer contents: {:?}",
                    self.next_sequence,
                    sequence,
                    self.buffer.keys().collect::<Vec<_>>()
                );
            }

            self.buffer.insert(
                sequence,
                BufferedBatch {
                    digest,
                    transactions,
                },
            );

            if self.buffer.len() % 100 == 0 {
                warn!(
                    "missing seq {}",
                    self.next_sequence
                );
            }
        } else {
            if transactions.is_empty() && digest == Digest::default() {
                warn!(
                    "Received old sequence {} (sync marker), expecting {} (discarding)",
                    sequence, self.next_sequence
                );
            } else {
                warn!(
                    "Received old sequence {} for batch {:?}, expecting {} (discarding)",
                    sequence, digest, self.next_sequence
                );
            }
        }
    }

    async fn send_feedback_if_needed(&mut self) {
        let should_send = (self.next_sequence - 1) >= self.last_feedback_sent + FEEDBACK_INTERVAL;

        if should_send {
            let last_executed = self.next_sequence - 1;
            if let Err(e) = self.tx_feedback.send((self.executor_id, last_executed)).await {
                warn!(
                    "Failed to send execution feedback for executor {}: {}",
                    self.executor_id, e
                );
            } else {
                debug!(
                    "Sent feedback: executor={}, last_executed={}",
                    self.executor_id, last_executed
                );
                self.last_feedback_sent = last_executed;
            }
        }
    }

    /// Extracts the required accounts for a transaction.
    fn extract_required_accounts(
        &self,
        transaction: &Transaction,
        _header: &crate::transaction::TransactionHeader,
    ) -> Vec<u64> {
        match self.workload_type {
            WorkloadType::SmallBank => {
                if let Some(sb_tx) = transaction.parse_smallbank_payload() {
                    match sb_tx.tx_type {
                        crate::workload::SmallBankTxType::SendPayment => {
                            // Check if dest account is local
                            vec![sb_tx.account_id, sb_tx.dest_account_id]
                        }
                        _ => vec![sb_tx.account_id], // Single-account transactions
                    }
                } else {
                    panic!("Failed to parse SmallBank payload for transaction");
                }
            }
        }
    }

    /// Checks if a transaction is ready to execute.
    /// Returns true if all required accounts are:
    /// 1. Not currently locked by another executing transaction.
    /// 2. Not blocked by another pending transaction in the queue (FIFO enforcement).
    fn is_ready_to_execute(&self, tx_id: TxID, accounts: &[u64]) -> bool {
        for account_id in accounts {
            // Check if account is currently locked by an executing transaction
            if let Some(owner) = self.account_locks.get(account_id) {
                if *owner != tx_id {
                    return false;
                }
            }

            // Check if account has a pending queue and we are not at the head
            if let Some(queue) = self.pending_queues.get(account_id) {
                if let Some(head) = queue.front() {
                    if head.tx_header.id != tx_id {
                        // Another transaction is ahead of us in this account's queue
                        return false;
                    }
                }
            }
        }
        true
    }

    /// Updates queue depth metrics.
    fn update_queue_metrics(&mut self) {
        // Calculate total queue depth
        let total_depth: usize = self.pending_queues.values().map(|q| q.len()).sum();
        self.lock_stats.current_queue_depth = total_depth;

        // Update max total depth
        if total_depth > self.lock_stats.max_total_queue_depth {
            self.lock_stats.max_total_queue_depth = total_depth;
        }

        // Update max single queue depth
        if let Some(max_single) = self.pending_queues.values().map(|q| q.len()).max() {
            if max_single > self.lock_stats.max_single_queue_depth {
                self.lock_stats.max_single_queue_depth = max_single;
            }
        }
    }

    /// Removes a transaction from all queues it's in (handles SendPayment in multiple queues).
    fn remove_from_all_queues(&mut self, accounts: &[u64], tx_id: TxID) -> PendingTransaction {
        let mut pending_tx = None;

        if accounts.is_empty() {
             panic!("remove_from_all_queues called with empty accounts for tx {:?}", tx_id);
        }

        for (idx, account_id) in accounts.iter().enumerate() {
            let queue = self.pending_queues.get_mut(account_id).unwrap_or_else(|| {
                panic!("Pending queue for account {} not found!", account_id);
            });

            // We expect the transaction to be at the head of the queue
            if let Some(tx) = queue.pop_front() {
                if tx.tx_header.id != tx_id {
                    panic!(
                        "Transaction mismatch in pending queue for account {}: expected {:?}, found {:?} (head)",
                        account_id, tx_id, tx.tx_header.id
                    );
                }

                if idx == 0 {
                    pending_tx = Some(tx);
                } else {
                    assert_eq!(
                        pending_tx.as_ref().unwrap().tx_header.id,
                        tx.tx_header.id,
                        "Inconsistent transaction IDs across queues for tx {:?}",
                        tx_id
                    );
                }
            } else {
                panic!(
                    "Pending queue for account {} is empty, expected tx {:?}",
                    account_id, tx_id
                );
            }
        }

        for account_id in accounts {
            if self.pending_queues.get(account_id).map_or(false, |q| q.is_empty()) {
                self.pending_queues.remove(account_id);
            }
        }

        pending_tx.expect("Failed to retrieve pending transaction")
    }

    /// Executes a single transaction (extracted from execute_batch to avoid duplication).
    async fn execute_single_local_transaction( 
        &mut self,
        digest: &Digest,
        tx_bytes: &[u8],
        header: &crate::transaction::TransactionHeader,
    ) -> bool {
        self.executed_tx_count += 1;
        let tx_type = header.tx_type;
        let tx_id = header.id;

        // Dimension 1: Count for performance measurement (independent of workload type)
        if tx_type == SAMPLE_TX_TYPE {
            info!(
                "Executing sample tx counter {} from client {} in batch {:?}",
                tx_id.tx_counter, tx_id.region_id, digest
            );
        }

        // Dimension 2: Parse workload-specific data based on configured workload type
        let mut success = true;
        let mut account_id = 0u64;
        let mut account_state = None;

        match self.workload_type {
            WorkloadType::SmallBank => {
                let transaction = Transaction::new(tx_bytes);
                if let Some(sb_tx) = transaction.parse_smallbank_payload() {
                    account_id = sb_tx.account_id;

                    // Execute transaction and capture state
                    match sb_tx.tx_type {
                        crate::workload::SmallBankTxType::Balance => {
                            let account = self.account_store.as_mut().unwrap().get_mut(&sb_tx.account_id).expect("Account not found");
                            account.execute_balance();
                            success = true;
                            account_state = Some(account.clone());
                        }
                        crate::workload::SmallBankTxType::DepositChecking => {
                            let account = self.account_store.as_mut().unwrap().get_mut(&sb_tx.account_id).expect("Account not found");
                            success = account.execute_deposit_checking(sb_tx.amount);
                            account_state = Some(account.clone());
                        }
                        crate::workload::SmallBankTxType::TransactSavings => {
                            let account = self.account_store.as_mut().unwrap().get_mut(&sb_tx.account_id).expect("Account not found");
                            success = account.execute_transact_savings(sb_tx.amount);
                            account_state = Some(account.clone());
                        }
                        crate::workload::SmallBankTxType::WriteCheck => {
                            let account = self.account_store.as_mut().unwrap().get_mut(&sb_tx.account_id).expect("Account not found");
                            success = account.execute_write_check(sb_tx.amount);
                            account_state = Some(account.clone());
                        }
                        crate::workload::SmallBankTxType::SendPayment => {
                            let store = self.account_store.as_mut().unwrap();
                            let src_account = store.get_mut(&sb_tx.account_id).expect("Source account not found");
                            success = src_account.execute_send_payment_debit(sb_tx.amount);
                            account_state = Some(src_account.clone());
                            
                            let dest_account_id = sb_tx.dest_account_id;
                            if success {
                                let dest_account = store.get_mut(&dest_account_id).expect("Dest account not found");
                                dest_account.execute_send_payment_credit(sb_tx.amount);
                            }

                            if tx_type == SAMPLE_TX_TYPE {
                                debug!(
                                    "\t\t SendPayment: src={}, dest={}, amount={:.2}, success={}",
                                    account_id, dest_account_id, sb_tx.amount, success
                                );
                            }
                        }
                    }

                    if tx_type == SAMPLE_TX_TYPE {
                        debug!(
                            "\t\t smallbank: type={:?}, account={}, amount={:.2}, success={}",
                            sb_tx.tx_type, sb_tx.account_id, sb_tx.amount, success
                        );
                    }
                }
            }
        }

        // Send client reply
        let client_addr = match self.committee.client_reply_address(tx_id.region_id) {
            Some(addr) => *addr,
            None => {
                warn!("Client ID {} not found in committee, skipping reply", tx_id.region_id);
                return false;
            }
        };

        let reply_request = ClientReplyRequest {
            batch_digest: digest.clone(),
            client_addr,
            tx_type,
            tx_id,
            success,
            account_id,
            account_state,
        };

        if let Err(e) = self.tx_client_reply.send(reply_request).await {
            warn!("Failed to send client reply request: {}", e);
        }

        // Log completion for sample transactions for performance measurement
        if tx_type == SAMPLE_TX_TYPE {
            info!(
                "Completed executing sample tx from client {} counter {}",
                tx_id.region_id, tx_id.tx_counter
            );
        }

        true
    }

    fn lock_and_enqueue_transaction(
        &mut self,
        digest: &Digest,
        tx_bytes: &[u8],
        header: crate::transaction::TransactionHeader,
        required_accounts: Vec<u64>,
        locality: TransactionLocality,
    ) {
        const MAX_PENDING_QUEUE_SIZE: usize = 10000;

        // Update metrics
        self.lock_stats.lock_hits += 1;

        // // Check queue size limit
        // if self.lock_stats.current_queue_depth >= MAX_PENDING_QUEUE_SIZE {
        //     panic!(
        //         "Pending queue exceeded limit of {}! Severe lock contention. Stats: {:?}",
        //         MAX_PENDING_QUEUE_SIZE, self.lock_stats
        //     );
        // }

        // Parse SmallBank payload once
        let transaction = Transaction::new(tx_bytes);
        let sb_tx = transaction.parse_smallbank_payload().expect("Failed to parse SmallBank payload");

        // Compute local accounts based on locality:
        // - For Local: all required_accounts are local
        // - For DistributedSource: only src_account is local
        // - For DistributedDest: only dest_account is local
        let local_accounts: Vec<u64> = match locality {
            TransactionLocality::Local => required_accounts.clone(),
            TransactionLocality::DistributedSource { src_account, .. } => vec![src_account],
            TransactionLocality::DistributedDest { dest_account, .. } => vec![dest_account],
        };

        // Populate reverse lookup for DistributedDest transactions
        if let TransactionLocality::DistributedDest { dest_account, .. } = locality {
            self.distributed_dest_accounts.insert(header.id, dest_account);
        }

        // Create pending transaction
        let pending_tx = PendingTransaction {
            tx_bytes: tx_bytes.to_vec(),
            tx_header: header,
            sb_tx,
            batch_digest: digest.clone(),
            required_accounts: required_accounts.clone(),
            locality,
        };

        // Only enqueue to LOCAL accounts' queues
        for account_id in &local_accounts {
            self.pending_queues
                .entry(*account_id)
                .or_insert_with(VecDeque::new)
                .push_back(pending_tx.clone());
            self.account_locks.insert(*account_id, header.id);
        }
    }

    /// Iteratively drains pending queues for unlocked accounts.
    async fn drain_pending_queues(&mut self, mut work_set: HashSet<u64>) {
        while let Some(&account_id) = work_set.iter().next() {
            work_set.remove(&account_id);

            loop {
                // Peek at the head transaction
                let (tx_id, required_accounts) = {
                    let queue: &VecDeque<PendingTransaction> = match self.pending_queues.get(&account_id) {
                        Some(q) if !q.is_empty() => q,
                        _ => break, // Queue empty or doesn't exist
                    };

                    let pending_tx = queue.front().unwrap();

                    match pending_tx.locality {
                        TransactionLocality::DistributedSource { src_account, dest_executor_id } => {
                            // Check if we're at queue head and haven't sent StateTransfer yet
                            let should_send = queue
                                .front()
                                .filter(|head| head.tx_header.id == pending_tx.tx_header.id)
                                .map(|_| {
                                    !self
                                        .outgoing_states_buffer
                                        .lock()
                                        .unwrap()
                                        .contains_key(&pending_tx.tx_header.id)
                                })
                                .unwrap_or(false);

                            if should_send {
                                // Clone to avoid borrow issues
                                let tx_to_send = pending_tx.clone();
                                self.send_state_transfer_for_tx(&tx_to_send, src_account, dest_executor_id).await;
                            }
                            // Always break - wait for writeback
                            break;
                        },
                        TransactionLocality::DistributedDest { .. } => {
                            // Dest waits for StateTransfer - processed at end of execute_batch
                            break;
                        },
                        TransactionLocality::Local => { /* proceed */ },
                    }
                    // Check if ALL required accounts for this transaction are now free and it is at head
                    if !self.is_ready_to_execute(pending_tx.tx_header.id, &pending_tx.required_accounts) {
                        // Still locked or blocked by other queues, cannot execute yet
                        break;
                    }

                    (pending_tx.tx_header.id, pending_tx.required_accounts.clone())
                };

                // All accounts are free! Pop from ALL queues this transaction is in.
                // We checked it's at the head of one queue, and lock ordering implies it should be at head of others.
                let pending_tx = self.remove_from_all_queues(&required_accounts, tx_id);

                // Execute the transaction
                self.execute_single_local_transaction(
                    &pending_tx.batch_digest,
                    &pending_tx.tx_bytes,
                    &pending_tx.tx_header,
                ).await;

                // Unlock accounts for the completed local transaction
                for account_id in &pending_tx.required_accounts {
                    self.account_locks.remove(account_id);
                }

                // Add newly unlocked accounts to the work set
                work_set.extend(pending_tx.required_accounts.iter());
            }

        }
    }

    async fn execute_batch(&mut self, digest: &Digest, transactions: &[Vec<u8>]) {
        // Single-pass execution: count transactions, parse workload data, and send client replies
        // Transaction format: [tx_type:1][region_id:1][tx_counter:8][workload_payload:variable][padding:size in client]
        //
        // A transaction can be BOTH sample AND SmallBank (e.g., sample=true, workload=SmallBank)
        let mut sample_count = 0;

        for tx_bytes in transactions.iter() {
            // Parse transaction header
            let transaction = Transaction::new(tx_bytes);
            let header = match transaction.parse_header() {
                Some(h) => h,
                None => {
                    warn!(
                        "Transaction too short (< 10 bytes) in batch {:?}, skipping",
                        digest
                    );
                    continue;
                }
            };

            // Count sample transactions
            if header.tx_type == SAMPLE_TX_TYPE {
                sample_count += 1;
            }

            self.lock_stats.total_transactions += 1;

            let required_accounts = self.extract_required_accounts(&transaction, &header);
            let locality = self.determine_locality(&required_accounts);
            let is_local_tx = matches!(locality, TransactionLocality::Local);

            if self.workload_type == WorkloadType::SmallBank {
                if !is_local_tx {
                    // Always enqueue distributed transactions first
                    self.lock_and_enqueue_transaction(digest, tx_bytes, header, required_accounts.clone(), locality);

                    // Send StateTransfer IMMEDIATELY if this tx is at queue head (Source side)
                    match locality {
                        TransactionLocality::DistributedSource { src_account, dest_executor_id } => {
                            // Clone the head tx to avoid borrow checker issues
                            let head_tx_clone = self.pending_queues.get(&src_account)
                                .and_then(|q| q.front())
                                .filter(|head| head.tx_header.id == header.id)
                                .cloned();

                            if let Some(ref head_tx) = head_tx_clone {
                                // At head - send StateTransfer immediately
                                self.send_state_transfer_for_tx(head_tx, src_account, dest_executor_id).await;
                            }
                        },
                        TransactionLocality::DistributedDest { .. } => {
                            // Dest side waits passively for StateTransfer arrival
                        },
                        TransactionLocality::Local => unreachable!("Already checked !is_local_tx"),
                    }
                } else if !self.is_ready_to_execute(header.id, &required_accounts) {
                    // Local tx but locked - enqueue
                    self.lock_and_enqueue_transaction(digest, tx_bytes, header, required_accounts, locality);
                } else {
                    // Local tx ready to execute immediately
                    self.lock_stats.eager_executions += 1;
                    self.execute_single_local_transaction(digest, tx_bytes, &header).await;
                }
            } else {
                // For Default workload: empty required_accounts is expected (no accounts), execute without locking
                self.lock_stats.eager_executions += 1;
                self.execute_single_local_transaction(digest, tx_bytes, &header).await;
            }

        }

        // Log batch execution with transaction type breakdown.
        if transactions.is_empty() && *digest == Digest::default() {
            // Empty batch with dummy digest (sequence sync marker)
            debug!("Processed sequence sync marker (empty batch)");
        } else {
            // For performance measurement, log sample transaction count
            info!(
                "Executed batch {:?}, cumulative total_executed {} ({} sample)",
                digest, self.executed_tx_count, sample_count,
            );
        }

        // Process incoming state transfers (Dest side) and writebacks (Source side)
        self.process_incoming_writebacks().await;
        self.process_incoming_transfers().await;

        self.update_queue_metrics();
        if self.next_sequence % 200 == 0 && self.lock_stats.total_transactions > 0 {
            self.log_account_store_size();
            let lock_hit_rate = (self.lock_stats.lock_hits as f64) / (self.lock_stats.total_transactions as f64) * 100.0;
            info!(
                "Lock Stats - Total: {}, Eager: {}, Queued: {}, Hit Rate: {:.2}%, Queue Depth: {}/{}, Max Single: {}",
                self.lock_stats.total_transactions,
                self.lock_stats.eager_executions,
                self.lock_stats.lock_hits,
                lock_hit_rate,
                self.lock_stats.current_queue_depth,
                self.lock_stats.max_total_queue_depth,
                self.lock_stats.max_single_queue_depth,
            );            
            if self.lock_stats.current_queue_depth > 0 {
                info!(
                    "Lock contention: {} transactions queued across {} accounts at seq={}",
                    self.lock_stats.current_queue_depth,
                    self.pending_queues.len(),
                    self.next_sequence
                );
            }
        }
    }

    /// Sends StateTransfer request for a distributed source transaction.
    async fn send_state_transfer_for_tx(
        &mut self,
        pending_tx: &PendingTransaction,
        src_account: u64,
        dest_executor_id: u32,
    ) {
        // performance measurement, please don't remove
        if pending_tx.tx_header.tx_type == SAMPLE_TX_TYPE {
            info!(
                "SendPayment Executing sample tx counter {} from client {} in batch {:?}",
                pending_tx.tx_header.id.tx_counter, pending_tx.tx_header.id.region_id, pending_tx.batch_digest
            );
        }
        // Get current src account state (clone for transfer)
        let src_account_state = self.account_store
            .as_ref()
            .unwrap()
            .get(&src_account)
            .expect("Source account not found in account_store")
            .clone();

        let state_transfer = StateTransfer {
            tx_id: pending_tx.tx_header.id,
            src_account_id: src_account,
            src_account_state,
        };

        let request = StateTransferRequest {
            state_transfer: state_transfer.clone(),
            dest_executor_id,
        };

        // Track in outgoing buffer
        self.outgoing_states_buffer.lock().unwrap().insert(
            pending_tx.tx_header.id,
            OutgoingStateInfo {
                state_transfer_request: request.clone(),
                timestamp: std::time::Instant::now(),
            },
        );

        // Send to StateHelper
        if let Err(e) = self.tx_send_state.send(request).await {
            warn!("Failed to send StateTransfer request: {}", e);
        }

        // debug!(
        //     "Sent StateTransfer for tx {:?}: src_account {} → dest_executor {}",
        //     pending_tx.tx_header.id, src_account, dest_executor_id
        // );
    }

    /// Process incoming StateTransfers (Dest executor side).
    /// Drains the shared buffer once, then matches against queue heads via reverse lookup.
    async fn process_incoming_transfers(&mut self) {
        // Drain shared buffer once into local storage
        let drained = std::mem::take(&mut *self.incoming_transfers.lock().unwrap());
        self.local_transfers.extend(drained);

        if self.local_transfers.is_empty() {
            return;
        }

        let mut unlocked_accounts = HashSet::new();

        let tx_ids: Vec<TxID> = self.local_transfers.keys().cloned().collect();

        for tx_id in tx_ids {
            // Use reverse lookup to find the dest account for this tx_id
            let dest_account = match self.distributed_dest_accounts.get(&tx_id) {
                Some(&acct) => acct,
                None => continue, // Not yet enqueued
            };

            // Check if this tx is at the head of its queue
            let is_at_head = self.pending_queues.get(&dest_account)
                .and_then(|q| q.front())
                .map_or(false, |head| head.tx_header.id == tx_id);

            if !is_at_head {
                continue; // Not at head yet, leave in local_transfers
            }

            let state_transfer = self.local_transfers.remove(&tx_id).unwrap();

            // Look up src_executor_id from the queue head
            let src_executor_id = match self.pending_queues.get(&dest_account).unwrap().front().unwrap().locality {
                TransactionLocality::DistributedDest { src_executor_id, .. } => src_executor_id,
                _ => unreachable!("Expected DistributedDest at queue head"),
            };

            // Pop the transaction from queue (we confirmed it's at head)
            let pending_tx = self
                .pending_queues
                .get_mut(&dest_account)
                .and_then(|q| q.pop_front())
                .expect("Queue should have tx at head");

            assert_eq!(
                pending_tx.tx_header.id, tx_id,
                "Queue head mismatch after state transfer check"
            );

            let src_account_id = state_transfer.src_account_id;
            let amount = pending_tx.sb_tx.amount;

            // Check src balance using transferred state (read-only)
            let src_balance = state_transfer.src_account_state.checking_balance;
            let success = src_balance >= amount as i64;

            // Prepare updated src state (with debit applied if successful)
            let mut updated_src_state = state_transfer.src_account_state.clone();
            if success {
                updated_src_state.checking_balance -= amount as i64;

                // Credit dest account (local write)
                let dest_account_state = self.account_store
                    .as_mut()
                    .unwrap()
                    .get_mut(&dest_account)
                    .expect("Dest account not found");
                dest_account_state.execute_send_payment_credit(amount);
            }

            // Log for SAMPLE_TX_TYPE
            if pending_tx.tx_header.tx_type == SAMPLE_TX_TYPE {
                debug!(
                    "\t\t DistributedDest SendPayment: src={} (remote), dest={} (local), amount={:.2}, success={}",
                    src_account_id, dest_account, amount, success
                );
            }

            // Send StateWriteback to source executor
            let writeback_request = StateWritebackRequest {
                state_writeback: StateWritebackArrival {
                    tx_id,
                    src_account_id,
                    account_state: updated_src_state.clone(),
                    success,
                },
                src_executor_id,
            };

            if let Err(e) = self.tx_state_writeback.send(writeback_request).await {
                warn!("Failed to send StateWriteback: {}", e);
            }

            // Send client reply (Dest executor sends optimistic reply)
            self.send_client_reply_for_distributed_tx(
                &pending_tx,
                success,
                src_account_id,
                Some(updated_src_state),
            ).await;

            // Clean up reverse lookup
            self.distributed_dest_accounts.remove(&tx_id);

            // Unlock dest account (already popped from queue above)
            self.account_locks.remove(&dest_account);
            if self.pending_queues.get(&dest_account).map_or(false, |q| q.is_empty()) {
                self.pending_queues.remove(&dest_account);
            }
            unlocked_accounts.insert(dest_account);
        }

        // Drain pending queues for unlocked accounts
        if !unlocked_accounts.is_empty() {
            self.drain_pending_queues(unlocked_accounts).await;
        }
    }

    /// Process incoming StateWritebacks (Source executor side).
    /// Drains the shared buffer once, then matches against queue heads using src_account_id from writeback.
    async fn process_incoming_writebacks(&mut self) {
        // Drain shared buffer once into local storage
        let drained = std::mem::take(&mut *self.incoming_writebacks.lock().unwrap());
        self.local_writebacks.extend(drained);

        if self.local_writebacks.is_empty() {
            return;
        }

        let mut unlocked_accounts = HashSet::new();

        let tx_ids: Vec<TxID> = self.local_writebacks.keys().cloned().collect();

        for tx_id in tx_ids {
            let src_account_id = self.local_writebacks.get(&tx_id).unwrap().src_account_id;

            // Check if this tx is at the head of the src_account queue
            let is_at_head = self.pending_queues.get(&src_account_id)
                .and_then(|q| q.front())
                .map_or(false, |head| head.tx_header.id == tx_id);

            if !is_at_head {
                continue; // Not at head yet, leave in local_writebacks
            }

            let writeback = self.local_writebacks.remove(&tx_id).unwrap();

            // Pop the transaction from queue
            let pending_tx = self
                .pending_queues
                .get_mut(&src_account_id)
                .and_then(|q| q.pop_front())
                .expect("Queue should have tx at head");

            assert_eq!(
                pending_tx.tx_header.id, tx_id,
                "Queue head mismatch after writeback check"
            );

            // Update account_store with writeback state
            if let Some(account) = self.account_store.as_mut().unwrap().get_mut(&src_account_id) {
                *account = writeback.account_state.clone();
            }

            // Log for SAMPLE_TX_TYPE
            if pending_tx.tx_header.tx_type == SAMPLE_TX_TYPE {
                debug!(
                    "\t\t DistributedSource SendPayment: src={} (local), dest={} (remote), amount={:.2}, success={}",
                    src_account_id, pending_tx.sb_tx.dest_account_id, pending_tx.sb_tx.amount, writeback.success
                );
            }

            // Remove from outgoing_states_buffer
            self.outgoing_states_buffer.lock().unwrap().remove(&tx_id);

            // Unlock src account (already popped from queue above)
            self.account_locks.remove(&src_account_id);
            if self.pending_queues.get(&src_account_id).map_or(false, |q| q.is_empty()) {
                self.pending_queues.remove(&src_account_id);
            }
            unlocked_accounts.insert(src_account_id);
        }

        // Drain pending queues for unlocked accounts
        if !unlocked_accounts.is_empty() {
            self.drain_pending_queues(unlocked_accounts).await;
        }
    }

    /// Send client reply for distributed transaction.
    async fn send_client_reply_for_distributed_tx(
        &mut self,
        pending_tx: &PendingTransaction,
        success: bool,
        account_id: u64,
        account_state: Option<AccountState>,
    ) {
        let tx_type = pending_tx.tx_header.tx_type;
        let tx_id = pending_tx.tx_header.id;

        // Log completion for SAMPLE_TX_TYPE, performance measurement
        if tx_type == SAMPLE_TX_TYPE {
            info!(
                "Completed executing sample tx from client {} counter {} (distributed)",
                tx_id.region_id, tx_id.tx_counter
            );
        }

        let client_addr = match self.committee.client_reply_address(tx_id.region_id) {
            Some(addr) => *addr,
            None => {
                warn!("Client ID {} not found in committee, skipping reply", tx_id.region_id);
                return;
            }
        };

        let reply_request = ClientReplyRequest {
            batch_digest: pending_tx.batch_digest.clone(),
            client_addr,
            tx_type,
            tx_id,
            success,
            account_id,
            account_state,
        };

        if let Err(e) = self.tx_client_reply.send(reply_request).await {
            warn!("Failed to send client reply request: {}", e);
        }
    }

    fn executor_has_account(&self, account_id: u64) -> bool {
        self.get_account_executor_id(account_id) == self.executor_id
    }

    fn determine_locality(&self, required_accounts: &[u64]) -> TransactionLocality {
        if required_accounts.len() == 1 {
            // Single-account transaction (Balance, DepositChecking, etc.)
            return TransactionLocality::Local;
        }

        if required_accounts.len() == 2 {
            let src_account = required_accounts[0];
            let dest_account = required_accounts[1];

            let owns_src = self.executor_has_account(src_account);
            let owns_dest = self.executor_has_account(dest_account);

            match (owns_src, owns_dest) {
                (true, true) => TransactionLocality::Local,  // Both local
                (true, false) => {
                    // Source role: need to find dest_executor_id
                    let dest_executor_id = self.get_account_executor_id(dest_account);
                    TransactionLocality::DistributedSource { src_account, dest_executor_id }
                }
                (false, true) => {
                    // Dest role: need to find src_executor_id
                    let src_executor_id = self.get_account_executor_id(src_account);
                    TransactionLocality::DistributedDest { dest_account, src_executor_id }
                }
                (false, false) => {
                    // Neither account is local - should never happen
                    panic!(
                        "Transaction routed to executor {} but neither account is local: {:?}",
                        self.executor_id, required_accounts
                    );
                }
            }
        } else {
            // Should never happen (only 1 or 2 accounts in SmallBank)
            TransactionLocality::Local
        }
    }

    /// Gets the executor ID that owns the specified account.
    /// Panics if account not found in partition (should never happen for valid routing).
    fn get_account_executor_id(&self, account_id: u64) -> u32 {
        self.states_partition
            .as_ref()
            .and_then(|partition| partition.get(&account_id))
            .copied()
            .unwrap_or_else(|| {
                panic!("Account {} not found in partition", account_id)
            })
    }

    fn log_account_store_size(&self) {
        let size = self.account_store.as_ref().map(|s| s.len()).unwrap_or(0);
        info!(
            "E{} AccountStoreSize seq={}: size={}",
            self.executor_id,
            self.next_sequence,
            size,
        );
    }
}
