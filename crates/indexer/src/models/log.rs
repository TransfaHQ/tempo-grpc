use crate::models::error::ParseError;

use super::{Address, Hash};
use alloy_primitives::{Bytes, FixedBytes};
use clickhouse::Row;
use clickhouse::types::UInt256;
use serde::{Deserialize, Serialize};
use shared::proto::{self, RpcBlock};
use tempo_primitives::TempoTxType;

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct LogRow {
    pub block_number: u64,
    pub block_timestamp: u64,
    pub log_index: u64,
    pub transaction_index: u64,
    pub transaction_hash: Hash,
    pub address: Address,
    pub topics: Vec<Hash>,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

pub fn log_to_row(
    log: &proto::Log,
    index: u64,
    block: &RpcBlock,
    tx_index: u64,
    tx_hash: &Vec<u8>,
) -> Result<LogRow, ParseError> {
    let data = log
        .data
        .as_ref()
        .ok_or(ParseError::MissingField("log.data"))?;
    Ok(LogRow {
        block_number: block.number,
        block_timestamp: block.timestamp,
        transaction_index: tx_index,
        transaction_hash: FixedBytes::try_from(tx_hash.as_slice())?.into(),
        log_index: index,
        address: FixedBytes::try_from(log.address.as_slice())?.into(),
        topics: data
            .topics
            .iter()
            .map(|topic| Ok(FixedBytes::try_from(topic.as_slice())?.into()))
            .collect::<Result<Vec<_>, ParseError>>()?,
        data: data.data.clone(),
    })
}
