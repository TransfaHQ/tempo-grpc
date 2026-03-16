use alloy_consensus::{Header, Signed, TxEip1559, TxLegacy};
use alloy_primitives::{Address, B64, B256, Bloom, Bytes, LogData, Signature, TxKind, U256};
use reth::primitives::{BlockBody, RecoveredBlock, SealedBlock, SealedHeader};
use reth::providers::{Chain, ExecutionOutcome};
use std::collections::BTreeMap;
use tempo_primitives::{
    Block, TempoHeader, TempoPrimitives, TempoReceipt, TempoTxEnvelope, TempoTxType,
};

use crate::proto::RpcBlock;

pub fn make_header(number: u64, gas_used: u64, base_fee: Option<u64>) -> TempoHeader {
    TempoHeader {
        inner: Header {
            parent_hash: B256::with_last_byte(1),
            ommers_hash: B256::with_last_byte(2),
            beneficiary: Address::with_last_byte(3),
            state_root: B256::with_last_byte(4),
            transactions_root: B256::with_last_byte(5),
            receipts_root: B256::with_last_byte(6),
            withdrawals_root: None,
            logs_bloom: Bloom::random(),
            difficulty: U256::from(1000),
            number,
            gas_limit: 30_000_000,
            gas_used,
            timestamp: 1_700_000_000,
            mix_hash: B256::with_last_byte(9),
            nonce: B64::ZERO,
            base_fee_per_gas: base_fee,
            blob_gas_used: None,
            excess_blob_gas: None,
            parent_beacon_block_root: None,
            extra_data: Bytes::from_static(&[0xca, 0xfe]),
            requests_hash: None,
        },
        general_gas_limit: 15_000_000,
        shared_gas_limit: 15_000_000,
        timestamp_millis_part: 500,
    }
}

pub fn make_block(
    transactions: Vec<TempoTxEnvelope>,
    senders: Vec<Address>,
) -> RecoveredBlock<Block> {
    let header = make_header(1, 0, None);
    let sealed_header = SealedHeader::new(header, B256::random());
    let body = BlockBody {
        transactions: transactions,
        ommers: vec![],
        withdrawals: None,
    };
    SealedBlock::<Block>::from_sealed_parts(sealed_header, body).with_senders(senders)
}

pub fn make_legacy_tx(nonce: u64, gas_price: u128, to: TxKind) -> TempoTxEnvelope {
    let tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price,
        gas_limit: 21_000,
        to,
        value: U256::from(1000),
        input: Bytes::new(),
    };
    let sig = Signature::new(U256::from(1), U256::from(2), false);
    let hash = B256::random();
    Signed::new_unchecked(tx, sig, hash).into()
}

pub fn make_eip1559_tx(
    nonce: u64,
    max_fee_per_gas: u128,
    max_priority_fee_per_gas: u128,
    to: alloy_primitives::TxKind,
) -> TempoTxEnvelope {
    let tx = TxEip1559 {
        chain_id: 1,
        nonce,
        gas_limit: 21_000,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        to,
        value: U256::from(1000),
        access_list: Default::default(),
        input: Bytes::new(),
    };
    let sig = Signature::new(U256::from(3), U256::from(4), false);
    let hash = B256::random();
    Signed::new_unchecked(tx, sig, hash).into()
}

pub fn make_receipt(
    tx_type: TempoTxType,
    success: bool,
    cumulative_gas_used: u64,
    logs: Vec<alloy_primitives::Log>,
) -> TempoReceipt {
    TempoReceipt {
        tx_type,
        success,
        cumulative_gas_used,
        logs,
    }
}

pub fn make_log(address: Address) -> alloy_primitives::Log {
    alloy_primitives::Log {
        address,
        data: LogData::new_unchecked(
            vec![B256::with_last_byte(0xaa)],
            Bytes::from_static(&[0x01, 0x02]),
        ),
    }
}

/// Creates a valid `RpcBlock` with correct byte lengths for all fields.
/// All optional fields are `None` by default.
pub fn make_rpc_block(number: u64) -> RpcBlock {
    RpcBlock {
        hash: B256::with_last_byte(0xff).to_vec(),
        parent_hash: B256::with_last_byte(1).to_vec(),
        ommers_hash: B256::with_last_byte(2).to_vec(),
        miner: Address::with_last_byte(3).to_vec(),
        state_root: B256::with_last_byte(4).to_vec(),
        transactions_root: B256::with_last_byte(5).to_vec(),
        receipts_root: B256::with_last_byte(6).to_vec(),
        logs_bloom: Bloom::ZERO.to_vec(),
        difficulty: U256::from(1000).to_le_bytes_vec(),
        number,
        gas_limit: 30_000_000,
        gas_used: 21_000,
        timestamp: 1_700_000_000,
        mix_hash: B256::with_last_byte(9).to_vec(),
        nonce: B64::random().to_vec(),
        base_fee_per_gas: Some(1_000_000_000),
        blob_gas_used: None,
        excess_blob_gas: None,
        parent_beacon_block_root: None,
        general_gas_limit: 15_000_000,
        shared_gas_limit: 15_000_000,
        timestamp_millis_part: 500,
        extra_data: vec![0xca, 0xfe],
        requests_hash: None,
        size: 1000,
        timestamp_millis: 1_700_000_000_500,
        withdrawals_root: None,
        uncles: vec![],
        transactions: vec![],
        withdrawals: None,
        status: 0,
    }
}

pub fn make_chain(
    blocks: Vec<(
        TempoHeader,
        B256,                            // block hash
        Vec<(TempoTxEnvelope, Address)>, // (tx, sender)
        Vec<TempoReceipt>,
    )>,
) -> Chain<TempoPrimitives> {
    let first_block = blocks
        .first()
        .map(|(h, _, _, _)| h.inner.number)
        .unwrap_or(0);
    let mut recovered_blocks = Vec::new();
    let mut all_receipts = Vec::new();

    for (header, hash, txs_with_senders, receipts) in blocks {
        let (transactions, senders): (Vec<_>, Vec<_>) = txs_with_senders.into_iter().unzip();
        let sealed_header = SealedHeader::new(header, hash);
        let body = BlockBody {
            transactions: transactions,
            ommers: vec![],
            withdrawals: None,
        };
        let block =
            SealedBlock::<Block>::from_sealed_parts(sealed_header, body).with_senders(senders);
        recovered_blocks.push(block);
        all_receipts.push(receipts);
    }

    Chain::<TempoPrimitives>::new(
        recovered_blocks,
        ExecutionOutcome {
            bundle: Default::default(),
            receipts: all_receipts,
            first_block,
            requests: Default::default(),
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
}
