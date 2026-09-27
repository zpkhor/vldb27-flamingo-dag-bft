// Copyright(C) Facebook, Inc. and its affiliates.
// Adapted from origin/old-executor-patch:worker/src/state_helper.rs
use crate::state_helper::{ExecutorToExecutorMessage, StateTransfer, StateTransferRequest};
use crate::transaction::TxID;
use crate::workload::AccountState;
use bytes::Bytes;
use config::{Committee, ExecutorId};
use crypto::PublicKey;
use log::{debug, info};
use network::SimpleSender;
use primary::ExecutorPrimaryMessage;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::Receiver;

/// Metadata for outgoing StateTransfer (Source side).
#[derive(Debug, Clone)]
pub struct OutgoingStateInfo {
    pub state_transfer_request: StateTransferRequest,
    pub timestamp: std::time::Instant,
}

#[derive(Debug, Clone)]
pub struct StateWritebackArrival {
    pub tx_id: TxID,
    pub src_account_id: u64,
    pub account_state: AccountState,
    pub success: bool,
}

/// Request from BatchExecutor to send StateWriteback to remote executor.
#[derive(Debug, Clone)]
pub struct StateWritebackRequest {
    pub state_writeback: StateWritebackArrival,
    pub src_executor_id: ExecutorId,
}

/// WritebackStateHelper manages bidirectional executor-to-executor state transfers.
///
/// Responsibilities:
/// - Send StateTransfer messages to Dest executors from Source side
/// - Send StateWriteback messages to Source executors from Dest side
/// - Receive StateTransfer and StateWriteback messages from network, putting them into shared buffers
/// - Maintain ongoing_states_buffer for tracking outgoing states awaiting writeback
/// - Forward execution feedback from BatchExecutor to Primary for flow control
pub struct WritebackStateHelper {
    /// Executor ID (for logging and address lookup)
    executor_id: ExecutorId,
    /// Authority's public key
    name: PublicKey,
    /// Committee configuration (for address lookup)
    committee: Committee,
    /// Network sender for executor-to-executor communication
    network: SimpleSender,

    /// Receives requests from BatchExecutor to send StateTransfer
    rx_send_state: Receiver<StateTransferRequest>,

    /// Receives requests from BatchExecutor to send StateWriteback
    rx_state_writeback: Receiver<StateWritebackRequest>,

    /// Receives execution feedback from BatchExecutor to forward to Primary
    rx_feedback: Receiver<(ExecutorId, u64)>,

    /// Receives incoming ExecutorToExecutorMessage from network
    rx_executor_message: Receiver<ExecutorToExecutorMessage>,

    /// Shared state: outgoing states awaiting writeback (Source side) for tracking
    /// Key: TxID
    outgoing_states_buffer: Arc<Mutex<HashMap<TxID, OutgoingStateInfo>>>,

    /// Shared Buffer: Incoming StateTransfers (Dest side)
    /// Key: TxID (ensures no overwrites when multiple transfers arrive)
    incoming_transfers: Arc<Mutex<HashMap<TxID, StateTransfer>>>,

    /// Shared Buffer: Incoming StateWritebacks (Source side)
    /// Key: TxID (ensures no overwrites when multiple writebacks arrive)
    incoming_writebacks: Arc<Mutex<HashMap<TxID, StateWritebackArrival>>>,
}

impl WritebackStateHelper {
    /// Spawns StateHelper thread and returns shared state buffers.
    #[allow(clippy::type_complexity)]
    pub fn spawn(
        executor_id: ExecutorId,
        name: PublicKey,
        committee: Committee,
        rx_send_state: Receiver<StateTransferRequest>,
        rx_state_writeback: Receiver<StateWritebackRequest>,
        rx_executor_message: Receiver<ExecutorToExecutorMessage>,
        rx_feedback: Receiver<(ExecutorId, u64)>,
    ) -> (
        Arc<Mutex<HashMap<TxID, OutgoingStateInfo>>>,
        Arc<Mutex<HashMap<TxID, StateTransfer>>>,
        Arc<Mutex<HashMap<TxID, StateWritebackArrival>>>,
    ) {
        let outgoing_states_buffer = Arc::new(Mutex::new(HashMap::new()));
        let incoming_transfers = Arc::new(Mutex::new(HashMap::new()));
        let incoming_writebacks = Arc::new(Mutex::new(HashMap::new()));

        let outgoing_clone = outgoing_states_buffer.clone();
        let transfers_clone = incoming_transfers.clone();
        let writebacks_clone = incoming_writebacks.clone();

        tokio::spawn(async move {
            Self {
                executor_id,
                name,
                committee,
                network: SimpleSender::new(),
                rx_send_state,
                rx_state_writeback,
                rx_executor_message,
                rx_feedback,
                outgoing_states_buffer: outgoing_clone,
                incoming_transfers: transfers_clone,
                incoming_writebacks: writebacks_clone,
            }
            .run()
            .await;
        });

        (outgoing_states_buffer, incoming_transfers, incoming_writebacks)
    }

    /// Main event loop handling StateTransfer requests and network messages.
    async fn run(&mut self) {
        info!("WritebackStateHelper {} started", self.executor_id);

        loop {
            tokio::select! {
                Some(request) = self.rx_send_state.recv() => {
                    self.handle_send_state_request(request).await;
                }
                Some(request) = self.rx_state_writeback.recv() => {
                    self.handle_send_state_writeback_request(request).await;
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

    /// Forwards execution feedback from BatchExecutor to Primary.
    async fn forward_feedback_to_primary(&mut self, executor_id: ExecutorId, last_executed: u64) {
        let primary_addr = self
            .committee
            .primary(&self.name)
            .expect("Primary address not found")
            .executor_to_primary;

        let msg = ExecutorPrimaryMessage::ExecutionFeedback(
            executor_id,
            last_executed,
        );

        let serialized = bincode::serialize(&msg)
            .expect("Failed to serialize ExecutorPrimaryMessage");

        self.network
            .send(primary_addr, Bytes::from(serialized))
            .await;

        debug!(
            "StateHelper {}: Forwarded feedback to Primary (executor={}, last_executed={})",
            self.executor_id, executor_id, last_executed
        );
    }

    /// Handles outgoing StateTransfer requests from BatchExecutor.
    async fn handle_send_state_request(&mut self, request: StateTransferRequest) {

        // Look up destination executor address
        let dest_addr = self
            .committee
            .executor(&self.name, &request.dest_executor_id)
            .expect("Dest executor address not found")
            .executor_to_executor;

        // Serialize and send StateTransfer message
        let msg = ExecutorToExecutorMessage::StateTransfer {
            tx_id: request.state_transfer.tx_id,
            src_account_id: request.state_transfer.src_account_id,
            src_account_state: request.state_transfer.src_account_state.clone(),
        };

        let serialized =
            bincode::serialize(&msg).expect("Failed to serialize StateTransfer");

        self.network.send(dest_addr, Bytes::from(serialized)).await;

        // debug!(
        //     "StateHelper {}: Sent StateTransfer for tx {:?} to executor {}",
        //     self.executor_id, request.state_transfer.tx_id, request.dest_executor_id
        // );
    }

    /// Handles outgoing StateWriteback requests from BatchExecutor.
    async fn handle_send_state_writeback_request(&mut self, request: StateWritebackRequest) {
        // Look up source executor address
        let src_addr = self
            .committee
            .executor(&self.name, &request.src_executor_id)
            .expect("Source executor address not found")
            .executor_to_executor;

        // Serialize and send StateWriteback message
        let msg = ExecutorToExecutorMessage::StateWriteback {
            tx_id: request.state_writeback.tx_id,
            src_account_id: request.state_writeback.src_account_id,
            updated_state: request.state_writeback.account_state.clone(),
            success: request.state_writeback.success,
            dest_account_id: request.state_writeback.src_account_id,
        };

        let serialized =
            bincode::serialize(&msg).expect("Failed to serialize StateWriteback");

        self.network.send(src_addr, Bytes::from(serialized)).await;

        // debug!(
        //     "StateHelper {}: Sent StateWriteback for tx {:?} to executor {}",
        //     self.executor_id, request.state_writeback.tx_id, request.src_executor_id
        // );
    }

    /// Handles incoming ExecutorToExecutorMessage from network.
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

                // Insert by tx_id to prevent overwrites when multiple transfers arrive
                self.incoming_transfers
                    .lock()
                    .unwrap()
                    .insert(tx_id, state_transfer);

                // debug!(
                //     "StateHelper {}: Received StateTransfer for tx {:?}, src_account {}",
                //     self.executor_id, tx_id, src_account_id
                // );
            }
            ExecutorToExecutorMessage::StateWriteback {
                tx_id,
                src_account_id,
                updated_state,
                success,
                dest_account_id: _,
            } => {
                let state_writeback = StateWritebackArrival {
                    tx_id,
                    src_account_id,
                    account_state: updated_state,
                    success,
                };

                // Insert by tx_id to prevent overwrites when multiple writebacks arrive
                self.incoming_writebacks
                    .lock()
                    .unwrap()
                    .insert(tx_id, state_writeback);

                // debug!(
                //     "StateHelper {}: Received StateWriteback for tx {:?}, src_account {}, success={}",
                //     self.executor_id, tx_id, src_account_id, success
                // );
            }
        }
    }
}
