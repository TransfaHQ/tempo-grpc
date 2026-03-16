use clickhouse::types::UInt256;
use clickhouse::Row;
use serde::{Deserialize, Serialize};

/// Data type for routing and table selection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    Block,
    Transaction,
    Receipt,
    Log,
    Withdrawal,
    Trace,
}

impl DataType {
    pub fn table_name(&self) -> &'static str {
        match self {
            Self::Block => "blocks",
            Self::Transaction => "transactions",
            Self::Receipt => "receipts",
            Self::Log => "logs",
            Self::Withdrawal => "withdrawals",
            Self::Trace => "traces",
        }
    }
}

pub type Hash = [u8; 32];
pub type Address = [u8; 20];

/// Block row for ClickHouse
#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct BlockRow {
    pub number: u64,
    pub hash: Hash,
    pub timestamp: u64,
    pub parent_hash: Hash,
    pub sha3_uncles: Hash,
    pub miner: Address,
    pub state_root: Hash,
    pub transactions_root: Hash,
    pub receipts_root: Hash,
    pub logs_bloom: String,
    pub difficulty: String,
    pub gas_limit: u64,
    pub gas_used: u64,
    pub extra_data: String,
    pub mix_hash: Hash,
    pub nonce: u64,
    pub base_fee_per_gas: Option<u64>,
    pub withdrawals_root: Option<Hash>,
    pub blob_gas_used: Option<u64>,
    pub excess_blob_gas: Option<u64>,
    pub parent_beacon_block_root: Option<Hash>,
    pub requests_hash: Option<Hash>,
    pub size: u64,
    // Tempo-specific fields
    pub general_gas_limit: u64,
    pub shared_gas_limit: u64,
    pub timestamp_millis_part: u64,
}

/// Transaction row for ClickHouse
#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct TransactionRow {
    pub hash: Hash,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub tx_index: u64,
    pub from_address: Address,
    pub to_address: Option<Address>,
    pub value: u8,
    pub input: String,
    pub gas: u64,
    pub gas_price: Option<u128>,
    pub max_fee_per_gas: u128,
    pub max_priority_fee_per_gas: Option<u128>,
    pub nonce: u64,
    pub tx_type: u8,
}

/// Receipt row for ClickHouse
#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct ReceiptRow {
    pub block_number: u64,
    pub tx_index: u64,
    pub from_address: Address,
    pub to_address: Option<Address>,
    pub status: bool,
    pub gas_used: u64,
    pub cumulative_gas_used: u64,
    pub effective_gas_price: u128,
    pub contract_address: Option<Address>,
    pub fee_payer: Address,
    pub fee_token: Option<Address>,
}

/// Log row for ClickHouse
#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct LogRow {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_index: u64,
    pub log_index: u64,
    pub address: Address,
    pub topics: Vec<Hash>,
    pub data: String,
}

/// Withdrawal row for ClickHouse (EIP-4895)
#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct WithdrawalRow {
    pub withdrawal_index: u64,
    pub validator_index: u64,
    pub address: Address,
    pub amount: u64, // In Gwei
    pub block_number: u64,
}

/// Trace row for ClickHouse (debug_traceBlock callTracer output)
#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct TraceRow {
    pub block_number: u64,
    pub transaction_index: u64,
    pub trace_type: String,
    pub from_address: Address,
    pub to_address: Option<Address>,
    pub value: String,
    pub gas: u64,
    pub gas_used: u64,
    pub input: String,
    pub output: Option<String>,
    pub depth: u32,
    pub call_index: u32,
    pub error: Option<String>,
}

/// Combined result for transactions and withdrawals fetched from the same blocks
#[derive(Debug, Clone, Default)]
pub struct TransactionsWithWithdrawals {
    pub transactions: Vec<TransactionRow>,
    pub withdrawals: Vec<WithdrawalRow>,
}

/// Row for the tip20_tokens ClickHouse table
#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct Tip20TokenRow {
    /// Block number where the token was created
    pub block_number: u64,
    /// Block timestamp
    pub block_timestamp: u64,
    /// Transaction hash that created the token
    /// Log index within the transaction
    pub log_index: u64,
    /// Token contract address (indexed in event)
    pub token: Address,
    /// Token name
    pub name: String,
    /// Token symbol
    pub symbol: String,
    /// Currency identifier
    pub currency: String,
    /// Quote token address for pricing
    pub quote_token: Address,
    /// Admin address with special privileges
    pub admin: Address,
    /// Salt used for CREATE2 deployment
    pub salt: Hash,
}
/// Change type enum matching ClickHouse Enum8 schema
/// ClickHouse Enum8 is serialized as i8 in RowBinary format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeType {
    Credit = 1,
    Debit = 2,
}

impl ChangeType {
    fn as_i8(&self) -> i8 {
        match self {
            ChangeType::Credit => 1,
            ChangeType::Debit => 2,
        }
    }
}

/// Custom serde for ChangeType - serialize as i8 for ClickHouse Enum8 RowBinary format
mod change_type_serde {
    use super::ChangeType;
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(change_type: &ChangeType, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // ClickHouse Enum8 expects i8 in RowBinary format
        serializer.serialize_i8(change_type.as_i8())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<ChangeType, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = i8::deserialize(deserializer)?;
        match v {
            1 => Ok(ChangeType::Credit),
            2 => Ok(ChangeType::Debit),
            _ => Err(serde::de::Error::custom(format!(
                "unknown change type value: {}",
                v
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct BalanceChangeRow {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_index: u64,
    pub log_index: u64,
    pub address: Address,
    pub from_address: Address,
    pub to_address: Address,
    pub token_address: Address,
    pub balance_before: UInt256,
    pub balance_after: UInt256,
    pub change_amount: UInt256,
    #[serde(with = "change_type_serde")]
    pub change_type: ChangeType,
    pub memo: Hash,
}

/// Allowance change type enum matching ClickHouse Enum8 schema
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllowanceChangeType {
    Increase = 1,
    Decrease = 2,
}

impl AllowanceChangeType {
    fn as_i8(&self) -> i8 {
        match self {
            AllowanceChangeType::Increase => 1,
            AllowanceChangeType::Decrease => 2,
        }
    }
}

mod allowance_change_type_serde {
    use super::AllowanceChangeType;
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(change_type: &AllowanceChangeType, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i8(change_type.as_i8())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<AllowanceChangeType, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = i8::deserialize(deserializer)?;
        match v {
            1 => Ok(AllowanceChangeType::Increase),
            2 => Ok(AllowanceChangeType::Decrease),
            _ => Err(serde::de::Error::custom(format!(
                "unknown allowance change type value: {}",
                v
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct AllowanceChangeRow {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub transaction_index: u64,
    pub log_index: u64,
    pub owner: Address,
    pub spender: Address,
    pub token_address: Address,
    pub allowance_before: UInt256,
    pub allowance_after: UInt256,
    pub change_amount: UInt256,
    #[serde(with = "allowance_change_type_serde")]
    pub change_type: AllowanceChangeType,
}
