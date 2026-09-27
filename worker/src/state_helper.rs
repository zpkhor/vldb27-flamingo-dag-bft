use crate::transaction::TxID;
use crate::workload::AccountState;
use bytes::Bytes;
use config::{Committee, ExecutorId};
use crypto::PublicKey;
use log::{debug, info};
use network::SimpleSender;
use primary::ExecutorPrimaryMessage;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::Receiver;

/// State transfer payload sent between executors for account migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateTransfer {
    pub tx_id: TxID,
    pub src_account_id: u64,
    pub src_account_state: AccountState,
}

/// Request from BatchExecutor to send StateTransfer to remote executor.
#[derive(Debug, Clone)]
pub struct StateTransferRequest {
    pub state_transfer: StateTransfer,
    pub dest_executor_id: ExecutorId,
}

/// Messages sent between executors for distributed transaction coordination.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ExecutorToExecutorMessage {
    /// State transfer: account migrates from source executor to destination executor.
    StateTransfer {
        tx_id: TxID,
        src_account_id: u64,
        src_account_state: AccountState,
    },
    /// State writeback: destination executor returns updated source account state after execution.
    /// Only used by the writeback executor path.
    StateWriteback {
        tx_id: TxID,
        src_account_id: u64,
        updated_state: AccountState,
        success: bool,
        dest_account_id: u64,
    },
}

/// StateHelper manages one-way executor-to-executor state migrations.
///
/// Responsibilities:
/// - Send StateTransfer messages to destination executors (migration out)
/// - Receive StateTransfer messages from network into shared buffer (migration in)
/// - Forward execution feedback from BatchExecutor to Primary for flow control
pub struct StateHelper {
    executor_id: ExecutorId,
    name: PublicKey,
    committee: Committee,
    network: SimpleSender,

    rx_send_state: Receiver<StateTransferRequest>,
    rx_feedback: Receiver<(ExecutorId, u64)>,
    rx_executor_message: Receiver<ExecutorToExecutorMessage>,

    /// Shared buffer: incoming StateTransfers (migration arrivals, multiple per tx)
    incoming_transfers: Arc<Mutex<HashMap<TxID, Vec<StateTransfer>>>>,

    accounts_sent_out: u64,
    accounts_received: u64,
}

impl StateHelper {
    /// Spawns StateHelper thread and returns the shared incoming transfers buffer.
    pub fn spawn(
        executor_id: ExecutorId,
        name: PublicKey,
        committee: Committee,
        rx_send_state: Receiver<StateTransferRequest>,
        rx_executor_message: Receiver<ExecutorToExecutorMessage>,
        rx_feedback: Receiver<(ExecutorId, u64)>,
    ) -> Arc<Mutex<HashMap<TxID, Vec<StateTransfer>>>> {
        let incoming_transfers = Arc::new(Mutex::new(HashMap::new()));
        let transfers_clone = incoming_transfers.clone();

        tokio::spawn(async move {
            Self {
                executor_id,
                name,
                committee,
                network: SimpleSender::new(),
                rx_send_state,
                rx_executor_message,
                rx_feedback,
                incoming_transfers: transfers_clone,
                accounts_sent_out: 0,
                accounts_received: 0,
            }
            .run()
            .await;
        });

        incoming_transfers
    }

    /// Main event loop handling StateTransfer requests and network messages.
    async fn run(&mut self) {
        info!("StateHelper {} started", self.executor_id);

        loop {
            tokio::select! {
                Some(request) = self.rx_send_state.recv() => {
                    self.handle_send_state_request(request).await;
                }
                Some(message) = self.rx_executor_message.recv() => {
                    self.handle_executor_message(message).await;
                }
                Some((executor_id, last_executed)) = self.rx_feedback.recv() => {
                    self.forward_feedback_to_primary(executor_id, last_executed).await;
                }
            }
        }
    }

    async fn forward_feedback_to_primary(&mut self, executor_id: ExecutorId, last_executed: u64) {
        let primary_addr = self
            .committee
            .primary(&self.name)
            .expect("Primary address not found")
            .executor_to_primary;

        let msg = ExecutorPrimaryMessage::ExecutionFeedback(executor_id, last_executed);

        let serialized =
            bincode::serialize(&msg).expect("Failed to serialize ExecutorPrimaryMessage");

        self.network
            .send(primary_addr, Bytes::from(serialized))
            .await;

        debug!(
            "StateHelper {}: Forwarded feedback to Primary (executor={}, last_executed={})",
            self.executor_id, executor_id, last_executed
        );
    }

    async fn handle_send_state_request(&mut self, request: StateTransferRequest) {
        let dest_addr = self
            .committee
            .executor(&self.name, &request.dest_executor_id)
            .expect("Dest executor address not found")
            .executor_to_executor;

        let msg = ExecutorToExecutorMessage::StateTransfer {
            tx_id: request.state_transfer.tx_id,
            src_account_id: request.state_transfer.src_account_id,
            src_account_state: request.state_transfer.src_account_state.clone(),
        };

        let serialized =
            bincode::serialize(&msg).expect("Failed to serialize StateTransfer");

        self.network.send(dest_addr, Bytes::from(serialized)).await;

        self.accounts_sent_out += 1;
        if self.accounts_sent_out % 1000 == 0 {
            info!(
                "E{} MigrationStats: accounts_out={}, accounts_in={}",
                self.executor_id, self.accounts_sent_out, self.accounts_received,
            );
        }
    }

    async fn handle_executor_message(&mut self, message: ExecutorToExecutorMessage) {
        match message {
            ExecutorToExecutorMessage::StateTransfer {
                tx_id,
                src_account_id,
                src_account_state,
            } => {
                let state_transfer = StateTransfer {
                    tx_id,
                    src_account_id,
                    src_account_state,
                };

                self.incoming_transfers
                    .lock()
                    .unwrap()
                    .entry(tx_id)
                    .or_insert_with(Vec::new)
                    .push(state_transfer);

                self.accounts_received += 1;
                if self.accounts_received % 1000 == 0 {
                    info!(
                        "E{} MigrationStats: accounts_out={}, accounts_in={}",
                        self.executor_id, self.accounts_sent_out, self.accounts_received,
                    );
                }
            }
            ExecutorToExecutorMessage::StateWriteback { .. } => {
                panic!("StateHelper (data-fusion mode) received unexpected StateWriteback, ignoring");
            }
        }
    }
}
