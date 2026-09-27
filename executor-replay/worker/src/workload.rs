use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryInto;

pub use config::compute_initial_shard_range;

/// Workload type for executor/router dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkloadType {
    SmallBank,
}

/// SmallBank transaction types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum SmallBankTxType {
    Balance = 0,
    DepositChecking = 1,
    TransactSavings = 2,
    WriteCheck = 3,
    SendPayment = 4,
}

impl SmallBankTxType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(SmallBankTxType::Balance),
            1 => Some(SmallBankTxType::DepositChecking),
            2 => Some(SmallBankTxType::TransactSavings),
            3 => Some(SmallBankTxType::WriteCheck),
            4 => Some(SmallBankTxType::SendPayment),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// Transaction amount constants from SmallBankConstants.java
pub const DEPOSIT_CHECKING_AMOUNT: f64 = 1.3;
pub const TRANSACT_SAVINGS_AMOUNT: f64 = 20.20;
pub const WRITE_CHECK_AMOUNT: f64 = 5.0;
pub const SEND_PAYMENT_AMOUNT: f64 = 5.0;

/// Account state in SmallBank workload
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct AccountState {
    pub checking_balance: i64,
    pub savings_balance: i64,
}

impl AccountState {
    pub fn new(checking_balance: i64, savings_balance: i64) -> Self {
        Self {
            checking_balance,
            savings_balance,
        }
    }

    pub fn total_balance(&self) -> i64 {
        self.checking_balance.saturating_add(self.savings_balance)
    }

    pub fn execute_balance(&self) -> i64 {
        self.total_balance()
    }

    pub fn execute_deposit_checking(&mut self, amount: f64) -> bool {
        self.checking_balance = self.checking_balance.saturating_add(amount as i64);
        true
    }

    pub fn execute_transact_savings(&mut self, amount: f64) -> bool {
        let new_balance = self.savings_balance - (amount as i64);
        if new_balance < 0 {
            return false;
        }
        self.savings_balance = new_balance;
        true
    }

    pub fn execute_write_check(&mut self, amount: f64) -> bool {
        let total = self.total_balance();
        let deduction = if total < amount as i64 {
            (amount - 1.0) as i64
        } else {
            amount as i64
        };
        self.checking_balance = self.checking_balance.saturating_sub(deduction);
        true
    }

    pub fn execute_send_payment_debit(&mut self, amount: f64) -> bool {
        let new_balance = self.checking_balance - (amount as i64);
        if new_balance < 0 {
            return false;
        }
        self.checking_balance = new_balance;
        true
    }

    pub fn execute_send_payment_credit(&mut self, amount: f64) {
        self.checking_balance = self.checking_balance.saturating_add(amount as i64);
    }
}

/// In-memory account store.
pub type AccountStore = HashMap<u64, AccountState>;

/// Create an account with deterministic balances based on account_id
pub fn create_account_with_seed(
    account_id: u64,
    min_balance: i64,
    max_balance: i64,
) -> AccountState {
    use rand::Rng;
    use rand::SeedableRng;

    let mut rng = rand::rngs::StdRng::seed_from_u64(account_id);
    let checking = rng.gen_range(min_balance, max_balance + 1);
    let savings = rng.gen_range(min_balance, max_balance + 1);

    AccountState::new(checking, savings)
}

/// SmallBank transaction representation (unified 35-byte format)
///
/// Payload format (25 bytes, after 10-byte standard header):
/// - Bytes 0-7: src_account_id (u64 BE)
/// - Bytes 8-15: dest_account_id (u64 BE, equals src for single-account txs)
/// - Byte 16: smallbank_tx_type
/// - Bytes 17-24: amount (f64 BE)
#[derive(Debug, Clone, PartialEq)]
pub struct SmallBankTransaction {
    pub tx_type: SmallBankTxType,
    pub account_id: u64,
    pub dest_account_id: u64,
    pub amount: f64,
}

impl SmallBankTransaction {
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 25 {
            return None;
        }
        let account_id = u64::from_be_bytes(bytes[0..8].try_into().ok()?);
        let dest_account_id = u64::from_be_bytes(bytes[8..16].try_into().ok()?);
        let tx_type = SmallBankTxType::from_u8(bytes[16])?;
        let amount = f64::from_be_bytes(bytes[17..25].try_into().ok()?);

        Some(SmallBankTransaction {
            tx_type,
            account_id,
            dest_account_id,
            amount,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(25);
        bytes.extend_from_slice(&self.account_id.to_be_bytes());
        bytes.extend_from_slice(&self.dest_account_id.to_be_bytes());
        bytes.push(self.tx_type.to_u8());
        bytes.extend_from_slice(&self.amount.to_be_bytes());
        bytes
    }
}
