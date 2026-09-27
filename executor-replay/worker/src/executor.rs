use crate::batch_executor::BatchExecutor;
use crate::client_replier::ClientReplier;
use crate::state_helper::{ExecutorToExecutorMessage, StateHelper};
use crate::workload::WorkloadType;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, ExecutorId, Partition, ShardingStrategy};
use crypto::{Digest, PublicKey};
use log::{info, warn};
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{channel, Sender};

const CHANNEL_CAPACITY: usize = 1_000;

/// Messages from worker Router to executor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerToExecutorMessage {
    Execute(Digest, u64, Vec<Vec<u8>>),
    SequenceSync(u64),
}

pub struct Executor;

impl Executor {
    pub fn spawn(
        name: PublicKey,
        id: ExecutorId,
        committee: Committee,
        parameters: config::Parameters,
        _sharding_strategy: ShardingStrategy,
        initial_partition: Partition,
    ) {
        let num_executors = parameters.num_executors;

        // Channels
        let (tx_batch_executor, rx_batch_executor) = channel(CHANNEL_CAPACITY);
        let (tx_client_reply, rx_client_reply) = channel(CHANNEL_CAPACITY);
        let (tx_send_state, rx_send_state) = channel(CHANNEL_CAPACITY);
        let (tx_executor_message, rx_executor_message) = channel(CHANNEL_CAPACITY);
        let (tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);

        if parameters.use_writeback_executor {
            // Writeback path: bidirectional state helper + DistributedTxExecutor
            let (tx_state_writeback, rx_state_writeback) = channel(CHANNEL_CAPACITY);

            let (outgoing_states_buffer, incoming_transfers, incoming_writebacks) =
                crate::writeback_state_helper::WritebackStateHelper::spawn(
                    id,
                    name,
                    committee.clone(),
                    rx_send_state,
                    rx_state_writeback,
                    rx_executor_message,
                    rx_feedback,
                );

            crate::writeback_batch_executor::DistributedTxExecutor::spawn(
                rx_batch_executor,
                tx_client_reply,
                committee.clone(),
                WorkloadType::SmallBank,
                parameters.num_accounts,
                num_executors,
                parameters.min_balance,
                parameters.max_balance,
                _sharding_strategy,
                id,
                0,
                Some(initial_partition),
                name,
                tx_send_state,
                tx_state_writeback,
                outgoing_states_buffer,
                incoming_transfers,
                incoming_writebacks,
                tx_feedback,
            );
        } else {
            // Data fusion path: one-way state helper + BatchExecutor
            let incoming_transfers = StateHelper::spawn(
                id,
                name,
                committee.clone(),
                rx_send_state,
                rx_executor_message,
                rx_feedback,
            );

            BatchExecutor::spawn(
                rx_batch_executor,
                tx_client_reply,
                committee.clone(),
                parameters.num_accounts,
                num_executors,
                parameters.min_balance,
                parameters.max_balance,
                id,
                initial_partition,
                name,
                tx_send_state,
                incoming_transfers,
                tx_feedback,
                parameters.use_new_scheduler,
            );
        }

        // Client replier
        ClientReplier::spawn(name, id, rx_client_reply);

        // Network receivers
        let executor_addrs = committee
            .executor(&name, &id)
            .expect("Own executor not in committee");

        // Listen for batches from Router (worker_to_executor)
        let mut w2e_addr = executor_addrs.worker_to_executor;
        w2e_addr.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            w2e_addr,
            ExecutorReceiverHandler {
                tx_batch_executor,
            },
        );

        // Listen for state transfers from other executors (executor_to_executor)
        let mut e2e_addr = executor_addrs.executor_to_executor;
        e2e_addr.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            e2e_addr,
            ExecutorToExecutorHandler {
                tx_executor_message,
            },
        );

        // NOTE: This log entry is used to compute performance.
        info!(
            "Executor {} successfully booted on {} (e2e: {})",
            id, executor_addrs.worker_to_executor, executor_addrs.executor_to_executor
        );
    }
}

/// Handles incoming messages from the Router.
#[derive(Clone)]
struct ExecutorReceiverHandler {
    tx_batch_executor: Sender<(Digest, u64, Vec<Vec<u8>>)>,
}

#[async_trait]
impl MessageHandler for ExecutorReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        match bincode::deserialize::<WorkerToExecutorMessage>(&message) {
            Ok(WorkerToExecutorMessage::Execute(digest, sequence, transactions)) => {
                self.tx_batch_executor
                    .send((digest, sequence, transactions))
                    .await
                    .expect("Failed to send to batch executor");
            }
            Ok(WorkerToExecutorMessage::SequenceSync(sequence)) => {
                self.tx_batch_executor
                    .send((Digest::default(), sequence, Vec::new()))
                    .await
                    .expect("Failed to send sequence sync to batch executor");
            }
            Err(e) => {
                warn!("Failed to deserialize WorkerToExecutorMessage: {}", e);
            }
        }
        Ok(())
    }
}

/// Handles incoming state transfer messages from peer executors.
#[derive(Clone)]
struct ExecutorToExecutorHandler {
    tx_executor_message: Sender<ExecutorToExecutorMessage>,
}

#[async_trait]
impl MessageHandler for ExecutorToExecutorHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        match bincode::deserialize::<ExecutorToExecutorMessage>(&message) {
            Ok(msg) => {
                self.tx_executor_message
                    .send(msg)
                    .await
                    .expect("Failed to send to state helper");
            }
            Err(e) => {
                warn!("Failed to deserialize ExecutorToExecutorMessage: {}", e);
            }
        }
        Ok(())
    }
}
