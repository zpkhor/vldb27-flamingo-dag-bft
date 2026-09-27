use crate::batch_executor::ClientReplyRequest;
use crate::transaction::SAMPLE_TX_TYPE;
use crate::workload::AccountState;
use crypto::{Digest, PublicKey};
use log::info;
use network::SimpleSender;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Receiver;

/// Reply sent back to the benchmark client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientReply {
    pub primary_name: PublicKey,
    pub executor_id: u32,
    pub batch_digest: Digest,
    pub tx_type: u8,
    pub client_id: u8,
    pub tx_counter: u64,
    pub success: bool,
    pub account_id: u64,
    pub account_state: Option<AccountState>,
}

pub struct ClientReplier;

impl ClientReplier {
    pub fn spawn(
        primary_name: PublicKey,
        executor_id: u32,
        mut rx_client_reply: Receiver<ClientReplyRequest>,
    ) {
        tokio::spawn(async move {
            let mut network = SimpleSender::new();

            while let Some(request) = rx_client_reply.recv().await {
                let reply = ClientReply {
                    primary_name,
                    executor_id,
                    batch_digest: request.batch_digest,
                    tx_type: request.tx_type,
                    client_id: request.tx_id.client_id,
                    tx_counter: request.tx_id.tx_counter,
                    success: request.success,
                    account_id: request.account_id,
                    account_state: request.account_state.clone(),
                };

                let serialized =
                    bincode::serialize(&reply).expect("Failed to serialize client reply");
                network
                    .send(request.client_addr, bytes::Bytes::from(serialized))
                    .await;

                if request.tx_type == SAMPLE_TX_TYPE {
                    // NOTE: This log entry is used to compute performance.
                    info!(
                        "Sent reply for sample tx from client {} counter {} to {}",
                        request.tx_id.client_id, request.tx_id.tx_counter, request.client_addr
                    );
                }
            }
        });
    }
}
