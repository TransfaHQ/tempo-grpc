use crate::models::error::ParseError;

use super::{Address, Hash};
use alloy_primitives::{Bytes, FixedBytes};
use clickhouse::Row;
use clickhouse::types::UInt256;
use serde::{Deserialize, Serialize};
use shared::proto::{self, BlockStatus};
use tempo_primitives::TempoTxType;

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct TransactionRow {
    pub hash: Hash,
    pub block_number: u64,
    pub block_timestamp: u64,
    pub tx_index: u64,
    pub from_address: Address,
    pub to_address: Option<Address>,
    pub value: UInt256,
    #[serde(with = "serde_bytes")]
    pub input: Vec<u8>,
    pub gas: u64,
    pub gas_price: Option<u128>,
    pub nonce: u64,
    pub tx_type: u8,
    // (input, value, to)
    pub calls: Vec<(String, UInt256, Option<Address>)>,
    pub fee_token: Option<Address>,
    pub is_deleted: bool,
}

fn bytes_to_u128(bytes: &[u8]) -> u128 {
    let mut buf = [0u8; 16];
    let len = bytes.len().min(16);
    buf[..len].copy_from_slice(&bytes[..len]);
    u128::from_le_bytes(buf)
}

fn bytes_to_uint256(bytes: &[u8]) -> UInt256 {
    let mut buf = [0u8; 32];
    let len = bytes.len().min(32);
    buf[..len].copy_from_slice(&bytes[..len]);
    UInt256::from_le_bytes(buf)
}

fn tx_kind_to_address(kind: &Option<proto::TxKind>) -> Result<Option<Address>, ParseError> {
    match kind {
        Some(tk) => match &tk.kind {
            Some(proto::tx_kind::Kind::Call(addr)) => {
                Ok(Some(FixedBytes::try_from(addr.as_slice())?.into()))
            }
            Some(proto::tx_kind::Kind::Create(_)) => Ok(None),
            None => Ok(None),
        },
        None => Ok(None),
    }
}

pub fn txn_to_row(
    block: &proto::Block,
    envelope: &proto::TransactionEnvelope,
) -> Result<TransactionRow, ParseError> {
    let txn = envelope
        .transaction
        .as_ref()
        .ok_or(ParseError::MissingField("transaction"))?;

    let receipt = envelope
        .receipt
        .as_ref()
        .ok_or(ParseError::MissingField("receipt"))?;

    let hash: Hash = FixedBytes::try_from(envelope.hash.as_slice())?.into();
    let from_address: Address = FixedBytes::try_from(envelope.sender.as_slice())?.into();

    match txn {
        proto::transaction_envelope::Transaction::Legacy(t) => Ok(TransactionRow {
            hash,
            block_number: block.number,
            block_timestamp: block.timestamp,
            tx_index: envelope.index,
            from_address,
            to_address: tx_kind_to_address(&t.to)?,
            value: bytes_to_uint256(&t.value),
            input: t.input.clone(),
            gas: receipt.gas_used,
            gas_price: Some(bytes_to_u128(&receipt.effective_gas_price)),
            nonce: t.nonce,
            tx_type: TempoTxType::Legacy.into(),
            fee_token: None,
            calls: vec![],
            is_deleted: block.status != BlockStatus::Committed as i32,
        }),
        proto::transaction_envelope::Transaction::Eip2930(t) => Ok(TransactionRow {
            hash,
            block_number: block.number,
            block_timestamp: block.timestamp,
            tx_index: envelope.index,
            from_address,
            to_address: tx_kind_to_address(&t.to)?,
            value: bytes_to_uint256(&t.value),
            input: t.input.clone(),
            gas: receipt.gas_used,
            gas_price: Some(bytes_to_u128(&receipt.effective_gas_price)),
            nonce: t.nonce,
            fee_token: None,
            calls: vec![],
            tx_type: TempoTxType::Eip2930.into(),
            is_deleted: block.status != BlockStatus::Committed as i32,
        }),
        proto::transaction_envelope::Transaction::Eip1559(t) => Ok(TransactionRow {
            hash,
            block_number: block.number,
            block_timestamp: block.timestamp,
            tx_index: envelope.index,
            from_address,
            to_address: tx_kind_to_address(&t.to)?,
            value: bytes_to_uint256(&t.value),
            input: t.input.clone(),
            gas: receipt.gas_used,
            gas_price: Some(bytes_to_u128(&receipt.effective_gas_price)),
            nonce: t.nonce,
            fee_token: None,
            calls: vec![],
            tx_type: TempoTxType::Eip1559.into(),
            is_deleted: block.status != BlockStatus::Committed as i32,
        }),
        proto::transaction_envelope::Transaction::Eip7702(t) => Ok(TransactionRow {
            hash,
            block_number: block.number,
            block_timestamp: block.timestamp,
            tx_index: envelope.index,
            from_address,
            to_address: Some(FixedBytes::try_from(t.to.as_slice())?.into()),
            value: bytes_to_uint256(&t.value),
            input: t.input.clone(),
            gas: receipt.gas_used,
            gas_price: Some(bytes_to_u128(&receipt.effective_gas_price)),
            nonce: t.nonce,
            fee_token: None,
            calls: vec![],
            tx_type: TempoTxType::Eip7702.into(),
            is_deleted: block.status != BlockStatus::Committed as i32,
        }),
        proto::transaction_envelope::Transaction::Tempo(t) => Ok(TransactionRow {
            hash,
            block_number: block.number,
            block_timestamp: block.timestamp,
            tx_index: envelope.index,
            from_address,
            to_address: match t.calls.first() {
                Some(call) => tx_kind_to_address(&call.to)?,
                None => None,
            },
            value: UInt256::ZERO,
            input: match t.calls.first() {
                Some(call) => call.input.clone(),
                None => vec![],
            },
            gas: receipt.gas_used,
            gas_price: Some(bytes_to_u128(&receipt.effective_gas_price)),
            nonce: t.nonce,
            fee_token: t
                .fee_token
                .as_ref()
                .map(|ft| FixedBytes::try_from(ft.as_slice()))
                .transpose()?
                .map(Into::into),
            calls: t
                .calls
                .iter()
                .map(|c| {
                    Ok((
                        Bytes::from(c.input.clone()).to_string(),
                        bytes_to_uint256(&c.value),
                        tx_kind_to_address(&c.to)?,
                    ))
                })
                .collect::<Result<Vec<_>, ParseError>>()?,
            tx_type: TempoTxType::AA.into(),
            is_deleted: block.status != BlockStatus::Committed as i32,
        }),
    }
}
