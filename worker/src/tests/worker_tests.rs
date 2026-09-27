// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::{batch_digest, committee_with_base_port, keys, listener, transaction};
use futures::stream::StreamExt as _;
use network::SimpleSender;
use primary::WorkerPrimaryMessage;
use std::fs;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::test]
async fn handle_clients_transactions() {
    let (name, _) = keys().pop().unwrap();
    let id = 0;
    let committee = committee_with_base_port(11_000);
    let parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };

    // Create a new test store.
    let path = ".db_test_handle_clients_transactions";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Spawn a `Worker` instance.
    Worker::spawn(name, id, committee.clone(), parameters, store);

    // Spawn a network listener to receive our batch's digest.
    let primary_address = committee.primary(&name).unwrap().worker_to_primary;
    let (tx_received, rx_received) = oneshot::channel();
    let handle = tokio::spawn(async move {
        let listener = TcpListener::bind(&primary_address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let transport = Framed::new(socket, LengthDelimitedCodec::new());
        let (mut writer, mut reader) = transport.split();
        let received = reader
            .next()
            .await
            .expect("Failed to receive network message")
            .expect("Failed to decode network message")
            .freeze();
        writer.send(Bytes::from("Ack")).await.unwrap();
        tx_received.send(received.to_vec()).unwrap();
    });

    // Spawn enough workers' listeners to acknowledge our batches.
    for (_, addresses) in committee.others_workers(&name, &id) {
        let address = addresses.worker_to_worker;
        let _ = listener(address, /* expected */ None);
    }

    // Send enough transactions to create a batch.
    let mut network = SimpleSender::new();
    let address = committee.worker(&name, &id).unwrap().transactions;
    network.send(address, Bytes::from(transaction())).await;
    network.send(address, Bytes::from(transaction())).await;

    // Ensure the primary received the batch's digest with expected content.
    assert!(handle.await.is_ok());
    let payload = rx_received.await.unwrap();
    let decoded: WorkerPrimaryMessage = bincode::deserialize(&payload).unwrap();
    match decoded {
        WorkerPrimaryMessage::OurBatch(digest, worker_id, account_counts, _) => {
            assert_eq!(digest, batch_digest());
            assert_eq!(worker_id, id);
            assert_eq!(account_counts, std::collections::BTreeMap::from([(0u64, 2u64)]));
        }
        _ => panic!("Unexpected message type"),
    }
}
