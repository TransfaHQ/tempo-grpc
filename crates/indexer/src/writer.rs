use alloy_primitives::{B64, Bloom, Bytes, FixedBytes, U256};
use clickhouse::{Client, inserter::Inserter};
use shared::proto::RpcBlock;
use tracing::info;

use crate::{error::IndexerError, models::BlockRow};

pub struct Writer {
    // pub clickhouse_client: &Client,
}

pub fn block_to_row(block: &RpcBlock) -> Result<BlockRow, IndexerError> {
    Ok(BlockRow {
        number: block.number,
        hash: FixedBytes::try_from(block.hash.as_slice())?.into(),
        timestamp: block.timestamp,
        parent_hash: FixedBytes::try_from(block.parent_hash.as_slice())?.into(),
        sha3_uncles: FixedBytes::try_from(block.ommers_hash.as_slice())?.into(),
        miner: FixedBytes::try_from(block.miner.as_slice())?.into(),
        state_root: FixedBytes::try_from(block.state_root.as_slice())?.into(),
        transactions_root: FixedBytes::try_from(block.transactions_root.as_slice())?.into(),
        receipts_root: FixedBytes::try_from(block.receipts_root.as_slice())?.into(),
        logs_bloom: Bloom::try_from(block.logs_bloom.as_slice())?.to_string(),
        difficulty: U256::try_from_le_slice(block.difficulty.as_slice())
            .unwrap_or_default()
            .to_string(),
        gas_limit: block.gas_limit,
        gas_used: block.gas_used,
        extra_data: Bytes::from(block.extra_data.clone()).to_string(),
        mix_hash: FixedBytes::try_from(block.mix_hash.as_slice())?.into(),
        nonce: B64::try_from(block.nonce.as_slice())?.into(),
        base_fee_per_gas: block.base_fee_per_gas,
        withdrawals_root: block
            .withdrawals_root
            .as_ref()
            .map(|root| FixedBytes::try_from(root.as_slice()))
            .transpose()?
            .map(Into::into),
        blob_gas_used: block.blob_gas_used,
        excess_blob_gas: block.excess_blob_gas,
        parent_beacon_block_root: block
            .parent_beacon_block_root
            .as_ref()
            .map(|root| FixedBytes::try_from(root.as_slice()))
            .transpose()?
            .map(Into::into),
        requests_hash: block
            .requests_hash
            .as_ref()
            .map(|root| FixedBytes::try_from(root.as_slice()))
            .transpose()?
            .map(Into::into),
        size: block.size,
        general_gas_limit: block.general_gas_limit,
        shared_gas_limit: block.shared_gas_limit,
        timestamp_millis_part: block.timestamp_millis_part,
    })
}

pub async fn process_block(
    inserter: &mut Inserter<BlockRow>,
    block: &RpcBlock,
) -> Result<(), IndexerError> {
    let row = block_to_row(block)?;
    inserter.write(&row).await?;
    // info!(block = &block.number, "inserted block");
    Ok(())
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256, Bloom, U256};
    use shared::test_utils::make_rpc_block;

    use super::*;

    // ==================== Happy path ====================

    #[test]
    fn test_all_fields_populated() {
        let mut block = make_rpc_block(42);
        block.withdrawals_root = Some(B256::with_last_byte(0xaa).to_vec());
        block.parent_beacon_block_root = Some(B256::with_last_byte(0xbb).to_vec());
        block.requests_hash = Some(B256::with_last_byte(0xcc).to_vec());

        let row = block_to_row(&block).unwrap();

        assert_eq!(row.number, 42);
        assert_eq!(row.hash, B256::with_last_byte(0xff).0);
        assert_eq!(row.timestamp, 1_700_000_000);
        assert_eq!(row.parent_hash, B256::with_last_byte(1).0);
        assert_eq!(row.sha3_uncles, B256::with_last_byte(2).0);
        assert_eq!(row.miner, Address::with_last_byte(3).0);
        assert_eq!(row.state_root, B256::with_last_byte(4).0);
        assert_eq!(row.transactions_root, B256::with_last_byte(5).0);
        assert_eq!(row.receipts_root, B256::with_last_byte(6).0);
        assert_eq!(row.mix_hash, B256::with_last_byte(9).0);
        assert_eq!(row.gas_limit, 30_000_000);
        assert_eq!(row.gas_used, 21_000);
        assert_eq!(row.base_fee_per_gas, Some(1_000_000_000));
        assert_eq!(row.general_gas_limit, 15_000_000);
        assert_eq!(row.shared_gas_limit, 15_000_000);
        assert_eq!(row.timestamp_millis_part, 500);
        assert_eq!(row.size, 1000);
        assert_eq!(row.nonce, 0);
        assert_eq!(row.extra_data, "0xcafe");
        assert_eq!(row.difficulty, U256::from(1000).to_string());
        assert_eq!(row.logs_bloom, Bloom::ZERO.to_string());
        assert_eq!(row.withdrawals_root, Some(B256::with_last_byte(0xaa).0));
        assert_eq!(
            row.parent_beacon_block_root,
            Some(B256::with_last_byte(0xbb).0)
        );
        assert_eq!(row.requests_hash, Some(B256::with_last_byte(0xcc).0));
    }

    #[test]
    fn test_optional_fields_none() {
        let block = make_rpc_block(1);

        let row = block_to_row(&block).unwrap();

        assert!(row.withdrawals_root.is_none());
        assert!(row.parent_beacon_block_root.is_none());
        assert!(row.requests_hash.is_none());
        assert!(row.blob_gas_used.is_none());
        assert!(row.excess_blob_gas.is_none());
    }

    // ==================== Error cases — wrong byte lengths ====================

    #[test]
    fn test_hash_wrong_length() {
        let mut block = make_rpc_block(1);
        block.hash = vec![0u8; 31]; // 31 instead of 32
        assert!(block_to_row(&block).is_err());
    }

    #[test]
    fn test_miner_wrong_length() {
        let mut block = make_rpc_block(1);
        block.miner = vec![0u8; 33]; // 33 instead of 20
        assert!(block_to_row(&block).is_err());
    }

    #[test]
    fn test_logs_bloom_wrong_length() {
        let mut block = make_rpc_block(1);
        block.logs_bloom = vec![0u8; 32]; // 32 instead of 256
        assert!(block_to_row(&block).is_err());
    }

    #[test]
    fn test_empty_hash_errors() {
        let mut block = make_rpc_block(1);
        block.hash = vec![];
        assert!(block_to_row(&block).is_err());
    }

    #[test]
    fn test_optional_hash_wrong_length() {
        let mut block = make_rpc_block(1);
        block.withdrawals_root = Some(vec![0u8; 10]);
        assert!(block_to_row(&block).is_err());
    }

    #[test]
    fn test_parent_beacon_block_root_wrong_length() {
        let mut block = make_rpc_block(1);
        block.parent_beacon_block_root = Some(vec![0u8; 5]);
        assert!(block_to_row(&block).is_err());
    }

    #[test]
    fn test_requests_hash_wrong_length() {
        let mut block = make_rpc_block(1);
        block.requests_hash = Some(vec![0u8; 64]);
        assert!(block_to_row(&block).is_err());
    }

    // ==================== Boundary values ====================

    #[test]
    fn test_block_number_zero() {
        let block = make_rpc_block(0);
        let row = block_to_row(&block).unwrap();
        assert_eq!(row.number, 0);
    }

    #[test]
    fn test_block_number_max() {
        let block = make_rpc_block(u64::MAX);
        let row = block_to_row(&block).unwrap();
        assert_eq!(row.number, u64::MAX);
    }

    #[test]
    fn test_difficulty_empty_slice() {
        let mut block = make_rpc_block(1);
        block.difficulty = vec![];
        let row = block_to_row(&block).unwrap();
        assert_eq!(row.difficulty, U256::ZERO.to_string());
    }

    #[test]
    fn test_difficulty_oversized_slice() {
        let mut block = make_rpc_block(1);
        block.difficulty = vec![0xff; 33]; // > 32 bytes
        let row = block_to_row(&block).unwrap();
        // try_from_le_slice returns None for >32 bytes, unwrap_or_default gives ZERO
        assert_eq!(row.difficulty, U256::ZERO.to_string());
    }

    #[test]
    fn test_zero_gas_values() {
        let mut block = make_rpc_block(1);
        block.gas_limit = 0;
        block.gas_used = 0;
        let row = block_to_row(&block).unwrap();
        assert_eq!(row.gas_limit, 0);
        assert_eq!(row.gas_used, 0);
    }
}
