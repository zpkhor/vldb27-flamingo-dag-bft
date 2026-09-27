use crate::state_helper::{StateTransfer, StateTransferRequest};
use crate::transaction::{Transaction, TransactionHeader, SAMPLE_TX_TYPE, TxID};
use crate::workload::{AccountStore, SmallBankTransaction};
use config::{Committee, Partition};
use crypto::{Digest, PublicKey};
use log::{debug, info, warn};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::hint::black_box;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{Receiver, Sender};

/// Flow control feedback interval
const FEEDBACK_INTERVAL: u64 = 100;

/// To cap executor throughput to worker throughput
const BUSY_SPINS: u64 = 135_156;

/// Represents a client reply request with named fields.
pub struct ClientReplyRequest {
    pub batch_digest: Digest,
    pub client_addr: SocketAddr,
    pub tx_type: u8,
    pub tx_id: TxID,
    pub success: bool,
    pub account_id: u64,
    pub account_state: Option<crate::workload::AccountState>,
}

/// Buffered batch waiting for its sequence number to be executed.
struct BufferedBatch {
    digest: Digest,
    transactions: Vec<Vec<u8>>,
}

#[derive(Debug, Clone)]
enum TransactionLocality {
    /// All required accounts are physically present in local store.
    Local,
    /// Account(s) migrate out: this executor sends state and removes the accounts.
    MigrateOut {
        owned_accounts_to_migrate: Vec<u64>,
        others_accounts_to_update_partition: Vec<u64>,
        dest_executor_id: u32,
    },
    /// Account(s) migrate in: this executor receives state and gains ownership.
    MigrateIn {
        accounts: Vec<(u64, u32)>, // (account_id, src_executor_id)
    },
    Others {
        executor_id: u32,
    },
}

/// Transaction waiting for locked accounts to become available.
#[derive(Clone)]
struct PendingTransaction {
    tx_bytes: Vec<u8>,
    tx_header: TransactionHeader,
    sb_tx: SmallBankTransaction,
    batch_digest: Digest,
    required_accounts: Vec<u64>,
    locality: TransactionLocality,
}

/// Information about a queue head transaction that's ready to execute.
struct ExecutableQueueHead {
    tx_id: TxID,
    required_accounts: Vec<u64>,
    locality: TransactionLocality,
}

struct LockStatistics {
    total_transactions: u64,
    lock_hits: u64,
    eager_executions: u64,
    current_queue_depth: usize,
    max_single_queue_depth: (usize, u64),
    max_total_queue_depth: usize,
}

impl LockStatistics {
    fn new() -> Self {
        Self {
            total_transactions: 0,
            lock_hits: 0,
            eager_executions: 0,
            current_queue_depth: 0,
            max_single_queue_depth: (0, 0),
            max_total_queue_depth: 0,
        }
    }

    fn update_queue_depths(&mut self, pending_queues: &HashMap<u64, VecDeque<PendingTransaction>>) {
        let mut total_depth = 0;
        for (&acc_id, queue) in pending_queues.iter() {
            let depth = queue.len();
            total_depth += depth;
            if depth > self.max_single_queue_depth.0 {
                self.max_single_queue_depth = (depth, acc_id);
            }
        }
        self.current_queue_depth = total_depth;
        if total_depth > self.max_total_queue_depth {
            self.max_total_queue_depth = total_depth;
        }
    }
}

/// Handles the execution of committed batches with strict sequence ordering.
pub struct BatchExecutor {
    rx_batch_executor: Receiver<(Digest, u64, Vec<Vec<u8>>)>,
    tx_client_reply: Sender<ClientReplyRequest>,
    committee: Committee,
    account_store: AccountStore,
    next_sequence: u64,
    buffer: BTreeMap<u64, BufferedBatch>,
    executor_id: u32,
    /// Dynamic partition: account_id -> executor_id. Updated deterministically as each transaction is scheduled.
    states_schedule_partition: Partition,
    pending_queues: HashMap<u64, VecDeque<PendingTransaction>>,
    name: PublicKey,
    tx_send_state: Sender<StateTransferRequest>,
    incoming_transfers: Arc<Mutex<HashMap<TxID, Vec<StateTransfer>>>>,
    tx_feedback: Sender<(u32, u64)>,
    last_feedback_sent: u64,
    /// Global counter of parsed transactions, used by the scheduler.
    scheduled_tx_count: u64,
    executed_tx_count: u64,
    num_executors: u32,
    lock_stats: LockStatistics,
    use_new_scheduler: bool,
    /// Local buffer for drained incoming transfers (avoids repeated mutex locks).
    drained_incoming_transfers: HashMap<TxID, Vec<StateTransfer>>,
}

impl BatchExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        rx_batch_executor: Receiver<(Digest, u64, Vec<Vec<u8>>)>,
        tx_client_reply: Sender<ClientReplyRequest>,
        committee: Committee,
        num_accounts: u64,
        num_executors: u32,
        min_balance: i64,
        max_balance: i64,
        executor_id: u32,
        initial_partition: Partition,
        name: PublicKey,
        tx_send_state: Sender<StateTransferRequest>,
        incoming_transfers: Arc<Mutex<HashMap<TxID, Vec<StateTransfer>>>>,
        tx_feedback: Sender<(u32, u64)>,
        use_new_scheduler: bool,
    ) {
        // Initialize account store from partition
        let mut account_store = AccountStore::new();
        for (&account_id, &owner_id) in initial_partition.iter() {
            if owner_id == executor_id {
                account_store.insert(
                    account_id,
                    crate::workload::create_account_with_seed(account_id, min_balance, max_balance),
                );
            }
        }

        // Verify partition consistency
        for account in account_store.keys() {
            let owner = initial_partition.get(account).expect("Account missing in partition");
            assert_eq!(
                *owner, executor_id,
                "Partition inconsistency: account {} owned by {} but found in executor {}'s store",
                account, owner, executor_id,
            );
        }

        info!(
            "BatchExecutor {}: loaded {} accounts (num_accounts={}, num_executors={}, new_scheduler={})",
            executor_id, account_store.len(), num_accounts, num_executors, use_new_scheduler
        );

        tokio::spawn(async move {
            Self {
                rx_batch_executor,
                tx_client_reply,
                committee,
                account_store,
                next_sequence: 0,
                buffer: BTreeMap::new(),
                executor_id,
                states_schedule_partition: initial_partition,
                pending_queues: HashMap::new(),
                name,
                tx_send_state,
                incoming_transfers,
                tx_feedback,
                last_feedback_sent: 0,
                scheduled_tx_count: 0,
                executed_tx_count: 0,
                num_executors,
                lock_stats: LockStatistics::new(),
                use_new_scheduler,
                drained_incoming_transfers: HashMap::new(),
            }
            .run()
            .await;
        });
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

            // Drain consecutive buffered batches
            while let Some(buffered) = self.buffer.remove(&self.next_sequence) {
                self.execute_batch(&buffered.digest, &buffered.transactions).await;
                self.next_sequence += 1;
                self.send_feedback_if_needed().await;
            }
        } else if sequence > self.next_sequence {
            if transactions.is_empty() && digest == Digest::default() {
                panic!("BatchExecutor should not receive sync marker, router always broadcast full batch");
            } else {
                warn!(
                    "Buffering batch {:?} (seq={}), expecting seq={}, buffer_size={}",
                    digest, sequence, self.next_sequence, self.buffer.len() + 1
                );
            }

            assert!(
                self.buffer.len() < 5000,
                "BatchExecutor buffer exceeded limit of 5000 batches! \
                 Executor is stuck waiting for sequence {} but received up to seq={}. \
                 Buffer contents: {:?}",
                self.next_sequence, sequence, self.buffer.keys().collect::<Vec<_>>()
            );

            self.buffer.insert(
                sequence,
                BufferedBatch {
                    digest,
                    transactions,
                },
            );

            if self.buffer.len() % 100 == 0 {
                warn!("missing seq {}", self.next_sequence);
            }
        } else {
            unreachable!(
                "Received batch with past sequence number: seq={} < next_sequence={}",
                sequence, self.next_sequence
            );
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
    fn extract_required_accounts(&self, transaction: &Transaction) -> Vec<u64> {
        if let Some(sb_tx) = transaction.parse_smallbank_payload() {
            match sb_tx.tx_type {
                crate::workload::SmallBankTxType::SendPayment => {
                    vec![sb_tx.account_id, sb_tx.dest_account_id]
                }
                _ => vec![sb_tx.account_id],
            }
        } else {
            panic!("Failed to parse SmallBank payload for transaction");
        }
    }

    /// Parses all transactions in a batch, returning each with its header and required accounts.
    fn parse_and_schedule_batch_transactions<'a>(
        &mut self,
        transactions: &'a [Vec<u8>],
    ) -> Vec<(&'a [u8], TransactionHeader, Vec<u64>, u32)> {
        // this has to be same across all executors, otherwise the behavior is undefined
        if self.use_new_scheduler {
            // New scheduler: dynamic load-aware scheduling with epsilon-greedy load balancing
            let num_executors = self.num_executors as usize;
            let epsilon: f64 = 0.5;
            let max_allowed = ((transactions.len() as f64 / num_executors as f64) * (1.0 + epsilon)).ceil() as usize;

            let mut result = Vec::with_capacity(transactions.len());
            let mut local_updates: HashMap<u64, u32> = HashMap::new();
            let mut current_loads = vec![0usize; num_executors];

            for tx_bytes in transactions {
                let transaction = Transaction::new(tx_bytes);
                let header = match transaction.parse_header() {
                    Some(h) => h,
                    None => continue,
                };
                let required_accounts = self.extract_required_accounts(&transaction);

                let mut votes: HashMap<u32, usize> = HashMap::new();
                for &account in &required_accounts {
                    let owner = local_updates.get(&account).copied()
                        .unwrap_or_else(|| *self.states_schedule_partition.get(&account).expect("account not in partition"));
                    *votes.entry(owner).or_insert(0) += 1;
                }

                let mut best_executor: i32 = -1;

                if !votes.is_empty() {
                    if votes.len() == 1 {
                        let (&eid, _) = votes.iter().next().unwrap();
                        if current_loads[eid as usize] < max_allowed {
                            best_executor = eid as i32;
                        }
                    }
                    if best_executor == -1 {
                        let mut best_score: i32 = -1;
                        for (&eid, &score) in &votes {
                            if current_loads[eid as usize] < max_allowed {
                                let score_i = score as i32;
                                if score_i > best_score
                                    || (score_i == best_score && current_loads[eid as usize] < current_loads[best_executor as usize])
                                    || (score_i == best_score && current_loads[eid as usize] == current_loads[best_executor as usize] && (eid as i32) < best_executor)
                                {
                                    best_score = score_i;
                                    best_executor = eid as i32;
                                }
                            }
                        }
                    }
                }

                if best_executor == -1 {
                    best_executor = current_loads.iter().enumerate()
                        .min_by_key(|&(_, &load)| load).unwrap().0 as i32;
                }

                let target = best_executor as u32;
                current_loads[target as usize] += 1;

                for &account in &required_accounts {
                    local_updates.insert(account, target);
                }

                self.scheduled_tx_count += 1;
                result.push((tx_bytes.as_slice(), header, required_accounts, target));
            }
            result
        } else {
            // Old scheduler: static account-based routing
            transactions
                .iter()
                .filter_map(|tx_bytes| {
                    let transaction = Transaction::new(tx_bytes);
                    let header = transaction.parse_header()?;
                    let required_accounts = self.extract_required_accounts(&transaction);
                    let target_executor_id = self.get_executor_id_by_partition(required_accounts[0]);
                    self.scheduled_tx_count += 1;
                    Some((tx_bytes.as_slice(), header, required_accounts, target_executor_id))
                })
                .collect()
        }
    }

    /// Checks if a transaction is ready to execute:
    /// 1. All required accounts are in local store
    /// 2. This transaction is at the head of all its account queues (or no queue exists)
    fn is_ready_to_execute(&self, tx_id: TxID, accounts: &[u64]) -> bool {
        accounts.iter().all(|acc| {
            self.account_store.contains_key(acc) && self.is_account_unlocked_or_head(*acc, tx_id)
        })
    }

    fn is_account_unlocked_or_head(&self, account: u64, tx_id: TxID) -> bool {
        self.pending_queues
            .get(&account)
            .map_or(true, |q| q.front().map_or(false, |h| h.tx_header.id == tx_id))
    }

    fn is_enqueued_at_head(&self, account: u64, tx_id: TxID) -> bool {
        let queue = self
            .pending_queues
            .get(&account)
            .unwrap_or_else(|| {
                panic!(
                    "is_enqueued_at_head: no pending queue for account {} (tx {:?})",
                    account, tx_id
                )
            });
        queue
            .front()
            .map(|head| head.tx_header.id == tx_id)
            .unwrap_or_else(|| {
                panic!(
                    "is_enqueued_at_head: empty queue for account {} (tx {:?})",
                    account, tx_id
                )
            })
    }

    /// Pops a transaction from the head of all specified account queues.
    fn pop_transaction_from_accounts(&mut self, accounts: &[u64], tx_id: TxID) -> PendingTransaction {
        let mut pending_tx = None;
        let mut empty_queues = HashSet::new();

        assert!(
            !accounts.is_empty(),
            "pop_transaction_from_accounts called with empty accounts for tx {:?}",
            tx_id
        );

        for account_id in accounts.iter() {
            let queue = self
                .pending_queues
                .get_mut(account_id)
                .unwrap_or_else(|| panic!("Pending queue for account {} not found!", account_id));

            if let Some(tx) = queue.pop_front() {
                assert_eq!(
                    tx.tx_header.id, tx_id,
                    "Transaction mismatch in pending queue for account {}: expected {:?}, found {:?}",
                    account_id, tx_id, tx.tx_header.id
                );
                pending_tx = Some(tx);
            } else {
                panic!(
                    "Pending queue for account {} is empty, expected tx {:?}",
                    account_id, tx_id
                );
            }

            if queue.is_empty() {
                empty_queues.insert(*account_id);
            }
        }

        for account_id in empty_queues {
            self.pending_queues.remove(&account_id);
        }

        pending_tx.unwrap()
    }

    /// Executes a single transaction locally (all required accounts must be in account_store).
    /// Only sends client reply for sample transactions.
    async fn execute_single_local_transaction(
        &mut self,
        digest: &Digest,
        tx_bytes: &[u8],
        header: &TransactionHeader,
        _caller: &str,
    ) -> bool {
        self.executed_tx_count += 1;
        let tx_id = header.id;

        if header.is_sample() {
            info!(
                "Executing sample tx counter {} from client {} in batch {:?}",
                tx_id.tx_counter, tx_id.client_id, digest
            );
        }

        for i in 0..BUSY_SPINS {
            black_box(i);
        }

        let transaction = Transaction::new(tx_bytes);
        let sb_tx = match transaction.parse_smallbank_payload() {
            Some(sb) => sb,
            None => {
                warn!("Executor {}: failed to parse SmallBank payload, skipping", self.executor_id);
                return false;
            }
        };

        let account_id = sb_tx.account_id;

        // Execute transaction
        let success = match sb_tx.tx_type {
            crate::workload::SmallBankTxType::Balance => {
                let account = self.account_store.get_mut(&sb_tx.account_id).expect("Account not found");
                account.execute_balance();
                true
            }
            crate::workload::SmallBankTxType::DepositChecking => {
                let account = self.account_store.get_mut(&sb_tx.account_id).expect("Account not found");
                account.execute_deposit_checking(sb_tx.amount)
            }
            crate::workload::SmallBankTxType::TransactSavings => {
                let account = self.account_store.get_mut(&sb_tx.account_id).expect("Account not found");
                account.execute_transact_savings(sb_tx.amount)
            }
            crate::workload::SmallBankTxType::WriteCheck => {
                let account = self.account_store.get_mut(&sb_tx.account_id).expect("Account not found");
                account.execute_write_check(sb_tx.amount)
            }
            crate::workload::SmallBankTxType::SendPayment => {
                assert!(
                    self.account_store.contains_key(&sb_tx.account_id)
                        && self.account_store.contains_key(&sb_tx.dest_account_id),
                    "E{}: SendPayment MISSING ACCOUNT! src={} (in_store={}), dest={} (in_store={}), \
                     tx_id={:?}, batch={:?}",
                    self.executor_id,
                    sb_tx.account_id,
                    self.account_store.contains_key(&sb_tx.account_id),
                    sb_tx.dest_account_id,
                    self.account_store.contains_key(&sb_tx.dest_account_id),
                    tx_id,
                    digest,
                );
                let src_account = self.account_store.get_mut(&sb_tx.account_id).expect("Source account not found");
                let debit_success = src_account.execute_send_payment_debit(sb_tx.amount);

                if debit_success {
                    let dest_account = self
                        .account_store
                        .get_mut(&sb_tx.dest_account_id)
                        .expect("Dest account not found");
                    dest_account.execute_send_payment_credit(sb_tx.amount);
                }

                if header.is_sample() {
                    debug!(
                        "\t\t SendPayment: src={}, dest={}, amount={:.2}, success={}",
                        account_id, sb_tx.dest_account_id, sb_tx.amount, debit_success
                    );
                }
                debit_success
            }
        };

        // Send client reply for sample transactions only
        if header.is_sample() {
            let client_addr = match self.committee.client_reply_address(tx_id.client_id) {
                Some(addr) => *addr,
                None => {
                    warn!(
                        "Client ID {} not found in committee, skipping reply",
                        tx_id.client_id
                    );
                    return false;
                }
            };

            let reply_request = ClientReplyRequest {
                batch_digest: digest.clone(),
                client_addr,
                tx_type: header.tx_type,
                tx_id,
                success,
                account_id,
                account_state: self.account_store.get(&account_id).cloned(),
            };

            if let Err(e) = self.tx_client_reply.send(reply_request).await {
                warn!("Failed to send client reply request: {}", e);
            }

            // NOTE: This log entry is used to compute performance.
            info!(
                "Completed executing sample tx from client {} counter {}",
                tx_id.client_id, tx_id.tx_counter
            );
        }

        success
    }

    fn lock_and_enqueue_transaction(
        &mut self,
        digest: &Digest,
        tx_bytes: &[u8],
        header: TransactionHeader,
        required_accounts: Vec<u64>,
        locality: TransactionLocality,
    ) {
        self.lock_stats.lock_hits += 1;
        let transaction = Transaction::new(tx_bytes);
        let sb_tx = transaction
            .parse_smallbank_payload()
            .expect("Failed to parse SmallBank payload");

        // Determine which accounts to enqueue on
        let local_accounts: Vec<u64> = match &locality {
            TransactionLocality::Local => required_accounts.clone(),
            TransactionLocality::MigrateOut {
                owned_accounts_to_migrate: accounts,
                ..
            } => accounts.clone(),
            TransactionLocality::MigrateIn { .. } => required_accounts.clone(),
            TransactionLocality::Others { .. } => {
                unreachable!("Others transactions should skip execution and never be enqueued")
            }
        };

        let pending_tx = PendingTransaction {
            tx_bytes: tx_bytes.to_vec(),
            tx_header: header,
            sb_tx,
            batch_digest: digest.clone(),
            required_accounts,
            locality,
        };

        for account_id in &local_accounts {
            self.pending_queues
                .entry(*account_id)
                .or_insert_with(VecDeque::new)
                .push_back(pending_tx.clone());
        }
    }

    /// Iteratively drains pending queues for unlocked accounts.
    async fn drain_pending_queues(&mut self, initial_unlocked: &[u64]) {
        let mut worklist: HashSet<u64> = initial_unlocked.iter().copied().collect();

        while let Some(&account_id) = worklist.iter().next() {
            worklist.remove(&account_id);

            loop {
                let head = match self.get_executable_queue_head(account_id) {
                    Some(h) => h,
                    None => break,
                };

                let tx_id = head.tx_id;
                let required_accounts = head.required_accounts;
                let locality = head.locality;

                match locality {
                    TransactionLocality::MigrateOut {
                        owned_accounts_to_migrate,
                        dest_executor_id,
                        ..
                    } => {
                        let pending_tx =
                            self.pop_transaction_from_accounts(&owned_accounts_to_migrate, tx_id);
                        self.execute_migrate_out(
                            &pending_tx.tx_header,
                            &pending_tx.sb_tx,
                            &pending_tx.batch_digest,
                            &owned_accounts_to_migrate,
                            dest_executor_id,
                        )
                        .await;
                    }
                    TransactionLocality::Local => {
                        let pending_tx =
                            self.pop_transaction_from_accounts(&required_accounts, tx_id);
                        self.execute_single_local_transaction(
                            &pending_tx.batch_digest,
                            &pending_tx.tx_bytes,
                            &pending_tx.tx_header,
                            "drain_local",
                        )
                        .await;
                        worklist.extend(pending_tx.required_accounts.iter().copied());
                    }
                    TransactionLocality::MigrateIn { .. } => {
                        unreachable!("get_executable_queue_head never returns MigrateIn locality")
                    }
                    TransactionLocality::Others { .. } => {
                        unreachable!("Others transactions should never be enqueued")
                    }
                }
            }

            // Clean up empty queues
            if let Some(queue) = self.pending_queues.get(&account_id) {
                if queue.is_empty() {
                    self.pending_queues.remove(&account_id);
                }
            }
        }
    }

    /// Handles MigrateOut: sends state transfer for each account and removes them from local store.
    async fn execute_migrate_out(
        &mut self,
        tx_header: &TransactionHeader,
        _sb_tx: &SmallBankTransaction,
        batch_digest: &Digest,
        accounts: &[u64],
        dest_executor_id: u32,
    ) {
        if tx_header.tx_type == SAMPLE_TX_TYPE {
            info!(
                "MigrateOut: sample tx counter {} from client {} in batch {:?}",
                tx_header.id.tx_counter, tx_header.id.client_id, batch_digest
            );
        }

        // TODO: can we merge into one transfer with multiple accounts? Or send eagerly for any account that's ready?
        for account in accounts {
            let account_state = self
                .account_store
                .remove(account)
                .expect("MigrateOut: account not found in account_store");

            let state_transfer = StateTransfer {
                tx_id: tx_header.id,
                src_account_id: *account,
                src_account_state: account_state,
            };

            let request = StateTransferRequest {
                state_transfer,
                dest_executor_id,
            };

            if let Err(e) = self.tx_send_state.send(request).await {
                warn!("Failed to send StateTransfer request: {}", e);
            }
        }
    }

    async fn execute_batch(&mut self, digest: &Digest, transactions: &[Vec<u8>]) {
        // Loop 1 — Schedule: build locality decisions
        let parsed_txs = self.parse_and_schedule_batch_transactions(transactions);
        let mut localities: Vec<TransactionLocality> = Vec::with_capacity(parsed_txs.len());

        for (_tx_bytes, _header, required_accounts, target_executor_id) in parsed_txs.iter() {
            let locality = self.determine_locality(required_accounts, *target_executor_id);
            self.update_partition_for_locality(&locality, required_accounts);
            localities.push(locality);
        }

        // Loop 2 — Execute: use pre-computed schedule
        // TODO: can we merge with Loop 1? does await prevent that?
        for ((tx_bytes, header, required_accounts, _target_executor_id), locality) in
            parsed_txs.iter().zip(localities.iter())
        {
            let header = *header;
            self.lock_stats.total_transactions += 1;
            self.handle_transaction_by_locality(
                digest,
                tx_bytes,
                header,
                required_accounts.clone(),
                locality.clone(),
            )
            .await;
        }

        // Log batch execution
        info!(
            "Executed batch seq={} txs={} total_executed={}",
            self.next_sequence, transactions.len(), self.executed_tx_count,
        );

        // Process incoming state transfers (migration arrivals)
        self.process_incoming_transfers().await;

        self.lock_stats.update_queue_depths(&self.pending_queues);
        if self.next_sequence % 5000 == 0 && self.lock_stats.total_transactions > 0 {
            self.log_account_store_size();
            let lock_hit_rate =
                (self.lock_stats.lock_hits as f64) / (self.lock_stats.total_transactions as f64) * 100.0;
            info!(
                "E{} LockStats seq={}: total={}, hits={} ({:.2}%), eager={}, queue_depth={}, max_single={}(acc={}), max_total={}",
                self.executor_id,
                self.next_sequence,
                self.lock_stats.total_transactions,
                self.lock_stats.lock_hits,
                lock_hit_rate,
                self.lock_stats.eager_executions,
                self.lock_stats.current_queue_depth,
                self.lock_stats.max_single_queue_depth.0,
                self.lock_stats.max_single_queue_depth.1,
                self.lock_stats.max_total_queue_depth,
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

    /// Returns true if partition says this executor owns the account.
    fn partition_says_mine(&self, account_id: u64) -> bool {
        self.get_executor_id_by_partition(account_id) == self.executor_id
    }

    /// Sole handler for incoming StateTransfers (MigrateIn).
    async fn process_incoming_transfers(&mut self) {
        // Drain incoming_transfers once to minimize lock contention with StateHelper
        let drained = std::mem::take(&mut *self.incoming_transfers.lock().unwrap());
        for (tx_id, transfers) in drained {
            self.drained_incoming_transfers
                .entry(tx_id)
                .or_default()
                .extend(transfers);
        }

        let mut unlocked_accounts = Vec::new();

        let tx_ids: Vec<TxID> = self.drained_incoming_transfers.keys().cloned().collect();

        for tx_id in tx_ids {
            let transfers = self.drained_incoming_transfers.get(&tx_id).unwrap();

            let first_account_id = transfers[0].src_account_id;

            let head_info = {
                let queue = match self.pending_queues.get(&first_account_id) {
                    Some(q) if !q.is_empty() => q,
                    _ => continue, // the tx might not be enqueued on this account yet
                };
                let head = queue.front().unwrap();
                if head.tx_header.id != tx_id {
                    continue;
                }
                match &head.locality {
                    TransactionLocality::MigrateIn { accounts } => {
                        Some((accounts.clone(), head.required_accounts.clone())) // TODO: instead of reconstructing, get the existing one from pending_queues or assert_eq?
                    }
                    _ => unreachable!("deadlock indicator"),
                }
            };

            let (migrating_accounts, required_accounts) = head_info.unwrap();

            // Check if ALL expected transfers have arrived
            let has_all = transfers.len() == migrating_accounts.len();
            if !has_all {
                continue;
            }

            // Must be at head of ALL required account queues
            let at_all_heads = required_accounts
                .iter()
                .all(|acc| self.is_enqueued_at_head(*acc, tx_id));
            if !at_all_heads {
                continue;
            }

            // Check that ALL required accounts are either in migrating_accounts or already in local store
            let migrating_ids: Vec<u64> = migrating_accounts.iter().map(|(id, _)| *id).collect();
            let all_accounts_in_store = required_accounts.iter().all(|acc| {
                migrating_ids.contains(acc) || self.account_store.contains_key(acc)
            });
            if !all_accounts_in_store {
                continue;
            }

            // Pop transfers from local buffer and execute
            let state_transfers = self.drained_incoming_transfers.remove(&tx_id).unwrap();
            let pending_tx = self.pop_transaction_from_accounts(&required_accounts, tx_id);

            for st in state_transfers {
                self.account_store
                    .insert(st.src_account_id, st.src_account_state);
            }

            self.execute_single_local_transaction(
                &pending_tx.batch_digest,
                &pending_tx.tx_bytes,
                &pending_tx.tx_header,
                "process_incoming_migrate_in",
            )
            .await;

            for acc in &pending_tx.required_accounts {
                unlocked_accounts.push(*acc);
            }
        }

        if !unlocked_accounts.is_empty() {
            self.drain_pending_queues(&unlocked_accounts).await;
        }
    }

    /// Determines locality using prescient routing: target_executor_id decides who executes.
    fn determine_locality(
        &self,
        required_accounts: &[u64],
        target_executor_id: u32,
    ) -> TransactionLocality {
        if target_executor_id == self.executor_id {
            // I'm the target — check which accounts I'm missing
            let missing: Vec<(u64, u32)> = required_accounts
                .iter()
                .filter(|acc| !self.partition_says_mine(**acc))
                .map(|acc| (*acc, self.get_executor_id_by_partition(*acc)))
                .collect();
            if missing.is_empty() {
                TransactionLocality::Local
            } else {
                TransactionLocality::MigrateIn { accounts: missing }
            }
        } else {
            // I'm NOT the target — check which accounts I own
            let owned: Vec<u64> = required_accounts
                .iter()
                .filter(|acc| self.partition_says_mine(**acc))
                .cloned()
                .collect();
            if owned.is_empty() {
                TransactionLocality::Others {
                    executor_id: target_executor_id,
                }
            } else {
                let others: Vec<u64> = required_accounts
                    .iter()
                    .filter(|acc| !self.partition_says_mine(**acc))
                    .cloned()
                    .collect();
                TransactionLocality::MigrateOut {
                    owned_accounts_to_migrate: owned,
                    others_accounts_to_update_partition: others,
                    dest_executor_id: target_executor_id,
                }
            }
        }
    }

    /// Gets the executor ID that owns the specified account per the partition.
    fn get_executor_id_by_partition(&self, account_id: u64) -> u32 {
        *self
            .states_schedule_partition
            .get(&account_id)
            .unwrap_or_else(|| panic!("Account {} not found in partition", account_id))
    }

    /// Updates partition based on transaction locality for prescient routing.
    fn update_partition_for_locality(
        &mut self,
        locality: &TransactionLocality,
        required_accounts: &[u64],
    ) {
        match locality {
            TransactionLocality::MigrateIn { accounts } => {
                for (acc, _src) in accounts {
                    self.states_schedule_partition
                        .insert(*acc, self.executor_id);
                }
            }
            TransactionLocality::MigrateOut {
                owned_accounts_to_migrate: accounts,
                others_accounts_to_update_partition: others_accounts,
                dest_executor_id,
            } => {
                for acc in accounts {
                    self.states_schedule_partition
                        .insert(*acc, *dest_executor_id);
                }
                for acc in others_accounts {
                    self.states_schedule_partition
                        .insert(*acc, *dest_executor_id);
                }
            }
            TransactionLocality::Others { executor_id: target } => {
                for acc in required_accounts {
                    self.states_schedule_partition.insert(*acc, *target);
                }
            }
            TransactionLocality::Local => {
                // No partition update needed
            }
        }
    }

    /// Handles execution of a local transaction (all accounts present locally).
    async fn handle_local_transaction(
        &mut self,
        digest: &Digest,
        tx_bytes: &[u8],
        header: TransactionHeader,
        required_accounts: Vec<u64>,
    ) -> bool {
        if self.is_ready_to_execute(header.id, &required_accounts) {
            self.lock_stats.eager_executions += 1;
            self.execute_single_local_transaction(digest, tx_bytes, &header, "eager_local")
                .await;
            true
        } else {
            self.lock_and_enqueue_transaction(
                digest,
                tx_bytes,
                header,
                required_accounts,
                TransactionLocality::Local,
            );
            false
        }
    }

    /// Handles execution of a MigrateOut transaction (accounts leaving this executor).
    async fn handle_migrate_out_transaction(
        &mut self,
        digest: &Digest,
        tx_bytes: &[u8],
        header: TransactionHeader,
        accounts: Vec<u64>,
        others_accounts: Vec<u64>,
        dest_executor_id: u32,
    ) -> bool {
        if self.is_ready_to_execute(header.id, &accounts) {
            self.lock_stats.eager_executions += 1;
            let transaction = Transaction::new(tx_bytes);
            let sb_tx = transaction
                .parse_smallbank_payload()
                .expect("Failed to parse SmallBank payload");
            self.execute_migrate_out(&header, &sb_tx, digest, &accounts, dest_executor_id)
                .await;
            true
        } else {
            self.lock_and_enqueue_transaction(
                digest,
                tx_bytes,
                header,
                Vec::new(), // required_accounts not used for MigrateOut enqueuing
                TransactionLocality::MigrateOut {
                    owned_accounts_to_migrate: accounts,
                    others_accounts_to_update_partition: others_accounts,
                    dest_executor_id,
                },
            );
            false
        }
    }

    /// Routes transaction execution based on locality type.
    async fn handle_transaction_by_locality(
        &mut self,
        digest: &Digest,
        tx_bytes: &[u8],
        header: TransactionHeader,
        required_accounts: Vec<u64>,
        locality: TransactionLocality,
    ) -> bool {
        match locality {
            TransactionLocality::Local => {
                self.handle_local_transaction(digest, tx_bytes, header, required_accounts)
                    .await
            }
            TransactionLocality::MigrateOut {
                owned_accounts_to_migrate: accounts,
                others_accounts_to_update_partition: others_accounts,
                dest_executor_id,
            } => {
                self.handle_migrate_out_transaction(
                    digest,
                    tx_bytes,
                    header,
                    accounts,
                    others_accounts,
                    dest_executor_id,
                )
                .await
            }
            TransactionLocality::MigrateIn { .. } => {
                self.lock_and_enqueue_transaction(
                    digest,
                    tx_bytes,
                    header,
                    required_accounts,
                    locality,
                );
                false
            }
            TransactionLocality::Others { .. } => {
                self.lock_stats.total_transactions -= 1;
                false
            }
        }
    }

    /// Checks if the head of an account's pending queue is ready to execute.
    fn get_executable_queue_head(&self, account_id: u64) -> Option<ExecutableQueueHead> {
        let queue = match self.pending_queues.get(&account_id) {
            Some(q) if !q.is_empty() => q,
            _ => return None,
        };

        let pending_tx = queue.front().unwrap();

        match &pending_tx.locality {
            TransactionLocality::MigrateOut {
                owned_accounts_to_migrate: accounts,
                others_accounts_to_update_partition: others_accounts,
                dest_executor_id,
            } => {
                let accounts = accounts.clone();
                let others_accounts = others_accounts.clone();
                let dest_executor_id = *dest_executor_id;
                if self.is_ready_to_execute(pending_tx.tx_header.id, &accounts) {
                    Some(ExecutableQueueHead {
                        tx_id: pending_tx.tx_header.id,
                        required_accounts: pending_tx.required_accounts.clone(),
                        locality: TransactionLocality::MigrateOut {
                            owned_accounts_to_migrate: accounts,
                            others_accounts_to_update_partition: others_accounts,
                            dest_executor_id,
                        },
                    })
                } else {
                    None
                }
            }
            TransactionLocality::MigrateIn { .. } => None,
            TransactionLocality::Others { .. } => {
                unreachable!(
                    "Others transactions should never be enqueued, so should never be at queue head"
                )
            }
            TransactionLocality::Local => {
                if !self.is_ready_to_execute(
                    pending_tx.tx_header.id,
                    &pending_tx.required_accounts,
                ) {
                    None
                } else {
                    Some(ExecutableQueueHead {
                        tx_id: pending_tx.tx_header.id,
                        required_accounts: pending_tx.required_accounts.clone(),
                        locality: TransactionLocality::Local,
                    })
                }
            }
        }
    }

    /// Logs the sum of account IDs grouped by executor ID for partition consistency checks.
    fn consistency_check(&self) {
        let mut executor_account_id_sums: HashMap<u32, u64> = HashMap::new();
        let mut executor_partition_len: HashMap<u32, u64> = HashMap::new();

        for (&account_id, &executor_id) in self.states_schedule_partition.iter() {
            *executor_account_id_sums.entry(executor_id).or_insert(0) += account_id;
            *executor_partition_len.entry(executor_id).or_insert(0) += 1;
        }

        let mut executor_summary_list: Vec<_> = executor_account_id_sums.iter().collect();
        executor_summary_list.sort_by_key(|(exec_id, _)| *exec_id);

        let summary: String = executor_summary_list
            .iter()
            .map(|(exec_id, sum)| format!("E{}:{}", exec_id, sum))
            .collect::<Vec<_>>()
            .join(", ");

        let partition_lens = (0..self.num_executors)
            .map(|exec_id| {
                let len = executor_partition_len.get(&exec_id).unwrap_or(&0);
                format!("E{}:{}", exec_id, len)
            })
            .collect::<Vec<_>>();

        let partition_lens_sum: u64 = (0..self.num_executors)
            .map(|exec_id| *executor_partition_len.get(&exec_id).unwrap_or(&0))
            .sum();

        info!(
            "E{} ConsistencyCheck seq={}: partition_lens=[{}], partition_lens_sum={}, partition_sums=[{}]",
            self.executor_id,
            self.next_sequence,
            partition_lens.join(", "),
            partition_lens_sum,
            summary,
        );
    }

    fn log_account_store_size(&self) {
        info!(
            "E{} AccountStoreSize seq={}: size={}",
            self.executor_id, self.next_sequence, self.account_store.len(),
        );
    }
}
