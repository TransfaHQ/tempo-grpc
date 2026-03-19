use crate::models::error::ParseError;

use super::{Address, Hash};
use alloy_primitives::{B64, FixedBytes};
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use shared::proto;

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct BlockRow {
    pub number: u64,
    pub hash: Hash,
    pub timestamp: u64,
    pub miner: Address,
    pub state_root: Hash,
    pub receipts_root: Hash,
    pub gas_limit: u64,
    pub gas_used: u64,
    #[serde(with = "serde_bytes")]
    pub extra_data: Vec<u8>,
    pub nonce: [u8; 8],
    pub size: u64,
}

impl TryFrom<&proto::Block> for BlockRow {
    type Error = ParseError;

    fn try_from(block: &proto::Block) -> Result<Self, Self::Error> {
        Ok(BlockRow {
            number: block.number,
            hash: FixedBytes::try_from(block.hash.as_slice())?.into(),
            timestamp: block.timestamp,
            miner: FixedBytes::try_from(block.miner.as_slice())?.into(),
            state_root: FixedBytes::try_from(block.state_root.as_slice())?.into(),
            receipts_root: FixedBytes::try_from(block.receipts_root.as_slice())?.into(),
            gas_limit: block.gas_limit,
            gas_used: block.gas_used,
            extra_data: block.extra_data.clone(),
            nonce: B64::try_from(block.nonce.as_slice())?.into(),
            size: block.size,
        })
    }
}
