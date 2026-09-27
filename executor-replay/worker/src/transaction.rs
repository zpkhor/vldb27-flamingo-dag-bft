// Copyright(C) Facebook, Inc. and its affiliates.
use crate::workload::SmallBankTransaction;
use std::convert::TryInto;
use serde::{Deserialize, Serialize};

/// Transaction header offsets (common to all workload types)
pub const TX_TYPE_OFFSET: usize = 0;
pub const CLIENT_ID_OFFSET: usize = 1;
pub const TX_COUNTER_OFFSET: usize = 2;
pub const TX_COUNTER_END: usize = 10;

/// SmallBank workload payload starts at offset 10 (unified 35-byte format)
pub const SMALLBANK_PAYLOAD_OFFSET: usize = 10;
pub const SMALLBANK_SRC_ACCOUNT_OFFSET: usize = 10;
pub const SMALLBANK_SRC_ACCOUNT_END: usize = 18;
pub const SMALLBANK_DEST_ACCOUNT_OFFSET: usize = 18;
pub const SMALLBANK_DEST_ACCOUNT_END: usize = 26;
pub const SMALLBANK_TX_TYPE_OFFSET: usize = 26;
pub const SMALLBANK_AMOUNT_OFFSET: usize = 27;
pub const SMALLBANK_PAYLOAD_END: usize = 35;

/// Minimum transaction size (header only)
pub const MIN_TRANSACTION_SIZE: usize = TX_COUNTER_END;

/// Transaction type identifiers
pub const SAMPLE_TX_TYPE: u8 = 0;
pub const REGULAR_TX_TYPE: u8 = 1;

/// Unique identifier for a transaction across the system
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TxID {
    pub client_id: u8,
    pub tx_counter: u64,
}

/// Parsed transaction header (first 10 bytes of any transaction)
///
/// Format: [tx_type:1][client_id:1][tx_counter:8]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionHeader {
    pub tx_type: u8,
    pub id: TxID,
}

impl TransactionHeader {
    pub fn parse(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < MIN_TRANSACTION_SIZE {
            return None;
        }

        let tx_type = bytes[TX_TYPE_OFFSET];
        let client_id = bytes[CLIENT_ID_OFFSET];
        let tx_counter = u64::from_be_bytes(
            bytes[TX_COUNTER_OFFSET..TX_COUNTER_END]
                .try_into()
                .ok()?,
        );

        Some(TransactionHeader {
            tx_type,
            id: TxID {
                client_id,
                tx_counter,
            },
        })
    }

    #[inline]
    pub fn is_sample(&self) -> bool {
        self.tx_type == SAMPLE_TX_TYPE
    }
}

/// Transaction wrapper providing convenient parsing methods
pub struct Transaction<'a> {
    raw: &'a [u8],
}

impl<'a> Transaction<'a> {
    pub fn new(raw: &'a [u8]) -> Self {
        Self { raw }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.raw
    }

    pub fn parse_header(&self) -> Option<TransactionHeader> {
        TransactionHeader::parse(self.raw)
    }

    pub fn extract_account_ids(&self) -> (Option<u64>, Option<u64>) {
        if self.raw.len() < SMALLBANK_DEST_ACCOUNT_END {
            return (None, None);
        }
        let src = self.raw[SMALLBANK_SRC_ACCOUNT_OFFSET..SMALLBANK_SRC_ACCOUNT_END]
            .try_into()
            .ok()
            .map(u64::from_be_bytes);
        let dest = self.raw[SMALLBANK_DEST_ACCOUNT_OFFSET..SMALLBANK_DEST_ACCOUNT_END]
            .try_into()
            .ok()
            .map(u64::from_be_bytes);
        (src, dest)
    }

    pub fn parse_smallbank_payload(&self) -> Option<SmallBankTransaction> {
        if self.raw.len() < SMALLBANK_PAYLOAD_END {
            return None;
        }
        SmallBankTransaction::from_bytes(&self.raw[SMALLBANK_PAYLOAD_OFFSET..SMALLBANK_PAYLOAD_END])
    }
}
