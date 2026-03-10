use alloy_consensus::{BlockHeader, Transaction, TxReceipt};
use alloy_primitives::TxKind;
use reth::providers::Chain;
use reth_ethereum::primitives::InMemorySize;
use tempo_primitives::{TempoPrimitives, TempoReceipt};

use crate::server::proto;

pub fn chain_to_rpc_blocks(
    chain: &Chain<TempoPrimitives>,
    status: proto::BlockStatus,
) -> eyre::Result<Vec<proto::RpcBlock>> {
    chain
        .blocks_and_receipts()
        .map(|(block, receipts)| {
            let receipts_gas_used = compute_gas_used(receipts);
            Ok(proto::RpcBlock {
                hash: block.hash().to_vec(),
                parent_hash: block.parent_hash().to_vec(),
                ommers_hash: block.ommers_hash().to_vec(),
                miner: block.beneficiary().to_vec(),
                state_root: block.state_root().to_vec(),
                transactions_root: block.transactions_root().to_vec(),
                receipts_root: block.receipts_root().to_vec(),
                logs_bloom: block.logs_bloom().to_vec(),
                difficulty: block.difficulty().to_le_bytes_vec(),
                number: block.number(),
                gas_limit: block.gas_limit(),
                gas_used: block.gas_used(),
                timestamp: block.timestamp(),
                mix_hash: block.inner.mix_hash.to_vec(),
                nonce: block.inner.nonce.to_vec(),
                base_fee_per_gas: block.base_fee_per_gas(),
                blob_gas_used: block.blob_gas_used(),
                excess_blob_gas: block.excess_blob_gas(),
                parent_beacon_block_root: block.parent_beacon_block_root().map(|i| i.to_vec()),
                general_gas_limit: block.general_gas_limit,
                shared_gas_limit: block.shared_gas_limit,
                timestamp_millis_part: block.timestamp_millis_part,
                extra_data: block.extra_data().to_vec(),
                requests_hash: block.requests_hash().map(|i| i.to_vec()),
                size: block.size() as u64,
                timestamp_millis: block.timestamp_millis(),
                withdrawals_root: block.withdrawals_root().map(|i| i.to_vec()),
                status: status.into(),
                uncles: block
                    .body()
                    .ommers
                    .iter()
                    .map(|h| h.inner.hash_slow().as_slice().to_vec())
                    .collect(),
                withdrawals: block.body().withdrawals.as_ref().map(|withdrawals| {
                    proto::Withdrawals {
                        items: withdrawals
                            .iter()
                            .map(|withdrawal| proto::Withdrawal {
                                index: withdrawal.index,
                                validator_index: withdrawal.validator_index,
                                address: withdrawal.address.to_vec(),
                                amount: withdrawal.amount,
                            })
                            .collect(),
                    }
                }),
                transactions: block
                    .body()
                    .transactions()
                    .zip(receipts)
                    .zip(block.senders_iter())
                    .enumerate()
                    .map(|(i, ((tx, receipt), sender))| (i, tx, receipt, sender))
                    .map(|(index, tx, receipt, sender)| {
                        let contract_address = match tx.kind() {
                            TxKind::Create => Some(sender.create(tx.nonce())),
                            TxKind::Call(_) => None,
                        };
                        let effective_gas_price = tx.effective_gas_price(block.base_fee_per_gas());
                        let gas_used = receipts_gas_used[index];
                        // See: https://github.com/tempoxyz/tempo/blob/0f8b2ae8ba3b8164d84324e29aaeb2c8e118c476/crates/node/src/rpc/mod.rs#L471
                        let fee_token = if gas_used == 0 || effective_gas_price == 0 {
                            receipt.logs().last().map(|log| log.address.to_vec())
                        } else {
                            None
                        };
                        Ok(proto::RpcTransaction {
                            index: index as u64,
                            transaction: Some(tx.try_into()?),
                            sender: sender.to_vec(),
                            receipt: Some(proto::RpcReceipt {
                                contract_address: contract_address.map(|a| a.to_vec()),
                                cumulative_gas_used: receipt.cumulative_gas_used,
                                effective_gas_price: effective_gas_price.to_le_bytes().to_vec(),
                                gas_used,
                                fee_token,
                                fee_payer: tx.fee_payer(sender.to_owned())?.to_vec(),
                                success: receipt.success,
                                tx_type: match receipt.tx_type {
                                    tempo_primitives::TempoTxType::Legacy => {
                                        proto::TxType::Legacy.into()
                                    }
                                    tempo_primitives::TempoTxType::Eip2930 => {
                                        proto::TxType::Eip2930.into()
                                    }
                                    tempo_primitives::TempoTxType::Eip1559 => {
                                        proto::TxType::Eip1559.into()
                                    }
                                    tempo_primitives::TempoTxType::Eip7702 => {
                                        proto::TxType::Eip7702.into()
                                    }
                                    tempo_primitives::TempoTxType::AA => {
                                        proto::TxType::Tempo.into()
                                    }
                                },
                                logs_bloom: receipt.bloom().to_vec(),
                                logs: receipt
                                    .logs
                                    .iter()
                                    .map(|log| proto::Log {
                                        address: log.address.to_vec(),
                                        data: Some(proto::LogData {
                                            topics: log
                                                .data
                                                .topics()
                                                .iter()
                                                .map(|topic| topic.to_vec())
                                                .collect(),
                                            data: log.data.data.to_vec(),
                                        }),
                                    })
                                    .collect(),
                            }),
                        })
                    })
                    .collect::<eyre::Result<_>>()?,
            })
        })
        .collect()
}

fn compute_gas_used(receipts: &[TempoReceipt]) -> Vec<u64> {
    let mut prev: Option<&TempoReceipt> = None;
    let mut result = Vec::with_capacity(receipts.len());
    for curr in receipts {
        if let Some(prev) = prev {
            result.push(curr.cumulative_gas_used - prev.cumulative_gas_used);
        } else {
            result.push(curr.cumulative_gas_used);
        }
        prev = Some(curr);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{Header, Signed, TxEip1559, TxLegacy};
    use alloy_eips::eip4895::Withdrawal;
    use alloy_primitives::{Address, B256, B64, Bloom, Bytes, LogData, Signature, U256};
    use reth::primitives::{BlockBody, SealedBlock, SealedHeader};
    use reth::providers::{Chain, ExecutionOutcome};
    use std::collections::BTreeMap;
    use tempo_primitives::{Block, TempoHeader, TempoTxEnvelope, TempoTxType};

    fn make_header(number: u64, gas_used: u64, base_fee: Option<u64>) -> TempoHeader {
        TempoHeader {
            inner: Header {
                parent_hash: B256::with_last_byte(1),
                ommers_hash: B256::with_last_byte(2),
                beneficiary: Address::with_last_byte(3),
                state_root: B256::with_last_byte(4),
                transactions_root: B256::with_last_byte(5),
                receipts_root: B256::with_last_byte(6),
                withdrawals_root: None,
                logs_bloom: Bloom::ZERO,
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

    fn make_legacy_tx(nonce: u64, gas_price: u128, to: TxKind) -> TempoTxEnvelope {
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

    fn make_eip1559_tx(
        nonce: u64,
        max_fee_per_gas: u128,
        max_priority_fee_per_gas: u128,
        to: TxKind,
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

    fn make_receipt(
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

    fn make_log(address: Address) -> alloy_primitives::Log {
        alloy_primitives::Log {
            address,
            data: LogData::new_unchecked(
                vec![B256::with_last_byte(0xaa)],
                Bytes::from_static(&[0x01, 0x02]),
            ),
        }
    }

    fn make_chain(
        blocks: Vec<(
            TempoHeader,
            B256,                              // block hash
            Vec<(TempoTxEnvelope, Address)>,   // (tx, sender)
            Vec<TempoReceipt>,
        )>,
    ) -> Chain<TempoPrimitives> {
        let first_block = blocks.first().map(|(h, _, _, _)| h.inner.number).unwrap_or(0);
        let mut recovered_blocks = Vec::new();
        let mut all_receipts = Vec::new();

        for (header, hash, txs_with_senders, receipts) in blocks {
            let (transactions, senders): (Vec<_>, Vec<_>) = txs_with_senders.into_iter().unzip();
            let sealed_header = SealedHeader::new(header, hash);
            let body = BlockBody {
                transactions,
                ommers: vec![],
                withdrawals: None,
            };
            let block = SealedBlock::<Block>::from_sealed_parts(sealed_header, body)
                .with_senders(senders);
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

    // ==================== compute_gas_used tests ====================

    #[test]
    fn test_compute_gas_used_empty() {
        assert!(compute_gas_used(&[]).is_empty());
    }

    #[test]
    fn test_compute_gas_used_single() {
        let receipts = vec![make_receipt(TempoTxType::Legacy, true, 21_000, vec![])];
        assert_eq!(compute_gas_used(&receipts), vec![21_000]);
    }

    #[test]
    fn test_compute_gas_used_multiple() {
        let receipts = vec![
            make_receipt(TempoTxType::Legacy, true, 21_000, vec![]),
            make_receipt(TempoTxType::Legacy, true, 63_000, vec![]),
            make_receipt(TempoTxType::Legacy, true, 100_000, vec![]),
        ];
        assert_eq!(compute_gas_used(&receipts), vec![21_000, 42_000, 37_000]);
    }

    // ==================== Header field mapping ====================

    #[test]
    fn test_single_block_header_fields() {
        let header = make_header(42, 21_000, Some(1_000_000_000));
        let block_hash = B256::with_last_byte(0xff);
        let chain = make_chain(vec![(header, block_hash, vec![], vec![])]);

        let result = chain_to_rpc_blocks(&chain, proto::BlockStatus::Committed).unwrap();
        assert_eq!(result.len(), 1);
        let b = &result[0];

        assert_eq!(b.hash, block_hash.to_vec());
        assert_eq!(b.parent_hash, B256::with_last_byte(1).to_vec());
        assert_eq!(b.ommers_hash, B256::with_last_byte(2).to_vec());
        assert_eq!(b.miner, Address::with_last_byte(3).to_vec());
        assert_eq!(b.state_root, B256::with_last_byte(4).to_vec());
        assert_eq!(b.transactions_root, B256::with_last_byte(5).to_vec());
        assert_eq!(b.receipts_root, B256::with_last_byte(6).to_vec());
        assert_eq!(b.difficulty, U256::from(1000).to_le_bytes_vec());
        assert_eq!(b.number, 42);
        assert_eq!(b.gas_limit, 30_000_000);
        assert_eq!(b.gas_used, 21_000);
        assert_eq!(b.timestamp, 1_700_000_000);
        assert_eq!(b.mix_hash, B256::with_last_byte(9).to_vec());
        assert_eq!(b.nonce, B64::ZERO.to_vec());
        assert_eq!(b.base_fee_per_gas, Some(1_000_000_000));
        assert_eq!(b.general_gas_limit, 15_000_000);
        assert_eq!(b.shared_gas_limit, 15_000_000);
        assert_eq!(b.timestamp_millis_part, 500);
        assert_eq!(b.extra_data, vec![0xca, 0xfe]);
    }

    // ==================== Block status ====================

    #[test]
    fn test_block_status_committed() {
        let chain = make_chain(vec![(make_header(1, 0, None), B256::ZERO, vec![], vec![])]);
        let result = chain_to_rpc_blocks(&chain, proto::BlockStatus::Committed).unwrap();
        assert_eq!(result[0].status, proto::BlockStatus::Committed as i32);
    }

    #[test]
    fn test_block_status_reorged() {
        let chain = make_chain(vec![(make_header(1, 0, None), B256::ZERO, vec![], vec![])]);
        let result = chain_to_rpc_blocks(&chain, proto::BlockStatus::Reorged).unwrap();
        assert_eq!(result[0].status, proto::BlockStatus::Reorged as i32);
    }

    #[test]
    fn test_block_status_reverted() {
        let chain = make_chain(vec![(make_header(1, 0, None), B256::ZERO, vec![], vec![])]);
        let result = chain_to_rpc_blocks(&chain, proto::BlockStatus::Reverted).unwrap();
        assert_eq!(result[0].status, proto::BlockStatus::Reverted as i32);
    }

    // ==================== Empty block ====================

    #[test]
    fn test_empty_block() {
        let chain = make_chain(vec![(make_header(1, 0, None), B256::ZERO, vec![], vec![])]);
        let result = chain_to_rpc_blocks(&chain, proto::BlockStatus::Committed).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].transactions.is_empty());
        assert!(result[0].uncles.is_empty());
        assert!(result[0].withdrawals.is_none());
    }

    // ==================== Gas used computation ====================

    #[test]
    fn test_gas_used_per_transaction() {
        let sender = Address::with_last_byte(0x01);
        let to = TxKind::Call(Address::with_last_byte(0x02));
        let tx1 = make_legacy_tx(0, 20_000_000_000, to);
        let tx2 = make_legacy_tx(1, 20_000_000_000, to);
        let tx3 = make_legacy_tx(2, 20_000_000_000, to);

        let r1 = make_receipt(TempoTxType::Legacy, true, 21_000, vec![]);
        let r2 = make_receipt(TempoTxType::Legacy, true, 50_000, vec![]);
        let r3 = make_receipt(TempoTxType::Legacy, true, 80_000, vec![]);

        let chain = make_chain(vec![(
            make_header(1, 80_000, Some(1_000_000_000)),
            B256::ZERO,
            vec![(tx1, sender), (tx2, sender), (tx3, sender)],
            vec![r1, r2, r3],
        )]);

        let result = chain_to_rpc_blocks(&chain, proto::BlockStatus::Committed).unwrap();
        let txs = &result[0].transactions;
        assert_eq!(txs.len(), 3);
        assert_eq!(txs[0].receipt.as_ref().unwrap().gas_used, 21_000);
        assert_eq!(txs[1].receipt.as_ref().unwrap().gas_used, 29_000);
        assert_eq!(txs[2].receipt.as_ref().unwrap().gas_used, 30_000);
    }

    // ==================== Transaction index & sender ====================

    #[test]
    fn test_transaction_index_and_sender() {
        let sender1 = Address::with_last_byte(0x01);
        let sender2 = Address::with_last_byte(0x02);
        let to = TxKind::Call(Address::with_last_byte(0x03));
        let tx1 = make_legacy_tx(0, 20_000_000_000, to);
        let tx2 = make_legacy_tx(1, 20_000_000_000, to);
        let r1 = make_receipt(TempoTxType::Legacy, true, 21_000, vec![]);
        let r2 = make_receipt(TempoTxType::Legacy, true, 42_000, vec![]);

        let chain = make_chain(vec![(
            make_header(1, 42_000, Some(1_000_000_000)),
            B256::ZERO,
            vec![(tx1, sender1), (tx2, sender2)],
            vec![r1, r2],
        )]);

        let result = chain_to_rpc_blocks(&chain, proto::BlockStatus::Committed).unwrap();
        let txs = &result[0].transactions;
        assert_eq!(txs[0].index, 0);
        assert_eq!(txs[0].sender, sender1.to_vec());
        assert_eq!(txs[1].index, 1);
        assert_eq!(txs[1].sender, sender2.to_vec());
    }

    // ==================== Contract creation ====================

    #[test]
    fn test_contract_creation_address() {
        let sender = Address::with_last_byte(0x01);
        let tx = make_legacy_tx(5, 20_000_000_000, TxKind::Create);
        let receipt = make_receipt(TempoTxType::Legacy, true, 21_000, vec![]);

        let chain = make_chain(vec![(
            make_header(1, 21_000, Some(1_000_000_000)),
            B256::ZERO,
            vec![(tx, sender)],
            vec![receipt],
        )]);

        let result = chain_to_rpc_blocks(&chain, proto::BlockStatus::Committed).unwrap();
        let rpc_receipt = result[0].transactions[0].receipt.as_ref().unwrap();
        let expected = sender.create(5);
        assert_eq!(rpc_receipt.contract_address, Some(expected.to_vec()));
    }

    #[test]
    fn test_call_has_no_contract_address() {
        let sender = Address::with_last_byte(0x01);
        let to = TxKind::Call(Address::with_last_byte(0x02));
        let tx = make_legacy_tx(0, 20_000_000_000, to);
        let receipt = make_receipt(TempoTxType::Legacy, true, 21_000, vec![]);

        let chain = make_chain(vec![(
            make_header(1, 21_000, Some(1_000_000_000)),
            B256::ZERO,
            vec![(tx, sender)],
            vec![receipt],
        )]);

        let result = chain_to_rpc_blocks(&chain, proto::BlockStatus::Committed).unwrap();
        let rpc_receipt = result[0].transactions[0].receipt.as_ref().unwrap();
        assert!(rpc_receipt.contract_address.is_none());
    }

    // ==================== Fee token detection ====================

    #[test]
    fn test_fee_token_when_zero_gas_used() {
        let sender = Address::with_last_byte(0x01);
        let to = TxKind::Call(Address::with_last_byte(0x02));
        let log_addr = Address::with_last_byte(0xaa);
        let tx = make_legacy_tx(0, 20_000_000_000, to);
        // cumulative_gas_used = 0 => gas_used = 0 for first tx
        let receipt = make_receipt(TempoTxType::Legacy, true, 0, vec![make_log(log_addr)]);

        let chain = make_chain(vec![(
            make_header(1, 0, Some(1_000_000_000)),
            B256::ZERO,
            vec![(tx, sender)],
            vec![receipt],
        )]);

        let result = chain_to_rpc_blocks(&chain, proto::BlockStatus::Committed).unwrap();
        let rpc_receipt = result[0].transactions[0].receipt.as_ref().unwrap();
        assert_eq!(rpc_receipt.fee_token, Some(log_addr.to_vec()));
    }

    #[test]
    fn test_fee_token_when_zero_effective_gas_price() {
        let sender = Address::with_last_byte(0x01);
        let to = TxKind::Call(Address::with_last_byte(0x02));
        let log_addr = Address::with_last_byte(0xbb);
        // gas_price = 0 => effective_gas_price = 0
        let tx = make_legacy_tx(0, 0, to);
        let receipt = make_receipt(TempoTxType::Legacy, true, 21_000, vec![make_log(log_addr)]);

        let chain = make_chain(vec![(
            make_header(1, 21_000, Some(0)),
            B256::ZERO,
            vec![(tx, sender)],
            vec![receipt],
        )]);

        let result = chain_to_rpc_blocks(&chain, proto::BlockStatus::Committed).unwrap();
        let rpc_receipt = result[0].transactions[0].receipt.as_ref().unwrap();
        assert_eq!(rpc_receipt.fee_token, Some(log_addr.to_vec()));
    }

    #[test]
    fn test_no_fee_token_normal_tx() {
        let sender = Address::with_last_byte(0x01);
        let to = TxKind::Call(Address::with_last_byte(0x02));
        let tx = make_legacy_tx(0, 20_000_000_000, to);
        let receipt = make_receipt(
            TempoTxType::Legacy,
            true,
            21_000,
            vec![make_log(Address::with_last_byte(0xcc))],
        );

        let chain = make_chain(vec![(
            make_header(1, 21_000, Some(1_000_000_000)),
            B256::ZERO,
            vec![(tx, sender)],
            vec![receipt],
        )]);

        let result = chain_to_rpc_blocks(&chain, proto::BlockStatus::Committed).unwrap();
        let rpc_receipt = result[0].transactions[0].receipt.as_ref().unwrap();
        assert!(rpc_receipt.fee_token.is_none());
    }

    #[test]
    fn test_fee_token_none_when_no_logs() {
        let sender = Address::with_last_byte(0x01);
        let to = TxKind::Call(Address::with_last_byte(0x02));
        let tx = make_legacy_tx(0, 20_000_000_000, to);
        // gas_used = 0 but no logs => fee_token = None
        let receipt = make_receipt(TempoTxType::Legacy, true, 0, vec![]);

        let chain = make_chain(vec![(
            make_header(1, 0, Some(1_000_000_000)),
            B256::ZERO,
            vec![(tx, sender)],
            vec![receipt],
        )]);

        let result = chain_to_rpc_blocks(&chain, proto::BlockStatus::Committed).unwrap();
        let rpc_receipt = result[0].transactions[0].receipt.as_ref().unwrap();
        assert!(rpc_receipt.fee_token.is_none());
    }

    // ==================== Receipt fields ====================

    #[test]
    fn test_receipt_fields() {
        let sender = Address::with_last_byte(0x01);
        let to = TxKind::Call(Address::with_last_byte(0x02));
        let log_addr = Address::with_last_byte(0x03);
        let tx = make_legacy_tx(0, 20_000_000_000, to);
        let receipt = make_receipt(
            TempoTxType::Legacy,
            true,
            21_000,
            vec![make_log(log_addr)],
        );

        let chain = make_chain(vec![(
            make_header(1, 21_000, Some(1_000_000_000)),
            B256::ZERO,
            vec![(tx, sender)],
            vec![receipt],
        )]);

        let result = chain_to_rpc_blocks(&chain, proto::BlockStatus::Committed).unwrap();
        let rpc_receipt = result[0].transactions[0].receipt.as_ref().unwrap();
        assert!(rpc_receipt.success);
        assert_eq!(rpc_receipt.cumulative_gas_used, 21_000);
        assert_eq!(rpc_receipt.gas_used, 21_000);
        assert_eq!(rpc_receipt.tx_type, proto::TxType::Legacy as i32);
        assert_eq!(rpc_receipt.fee_payer, sender.to_vec());
        assert_eq!(rpc_receipt.logs.len(), 1);
        assert_eq!(rpc_receipt.logs[0].address, log_addr.to_vec());
        let log_data = rpc_receipt.logs[0].data.as_ref().unwrap();
        assert_eq!(log_data.topics, vec![B256::with_last_byte(0xaa).to_vec()]);
        assert_eq!(log_data.data, vec![0x01, 0x02]);
    }

    #[test]
    fn test_failed_receipt() {
        let sender = Address::with_last_byte(0x01);
        let to = TxKind::Call(Address::with_last_byte(0x02));
        let tx = make_legacy_tx(0, 20_000_000_000, to);
        let receipt = make_receipt(TempoTxType::Legacy, false, 21_000, vec![]);

        let chain = make_chain(vec![(
            make_header(1, 21_000, Some(1_000_000_000)),
            B256::ZERO,
            vec![(tx, sender)],
            vec![receipt],
        )]);

        let result = chain_to_rpc_blocks(&chain, proto::BlockStatus::Committed).unwrap();
        assert!(!result[0].transactions[0].receipt.as_ref().unwrap().success);
    }

    // ==================== Tx type mapping ====================

    #[test]
    fn test_tx_type_mapping() {
        let cases = [
            (TempoTxType::Legacy, proto::TxType::Legacy),
            (TempoTxType::Eip2930, proto::TxType::Eip2930),
            (TempoTxType::Eip1559, proto::TxType::Eip1559),
            (TempoTxType::Eip7702, proto::TxType::Eip7702),
            (TempoTxType::AA, proto::TxType::Tempo),
        ];

        for (tempo_type, expected_proto_type) in cases {
            let sender = Address::with_last_byte(0x01);
            let to = TxKind::Call(Address::with_last_byte(0x02));
            let tx = make_legacy_tx(0, 20_000_000_000, to);
            let receipt = make_receipt(tempo_type, true, 21_000, vec![]);

            let chain = make_chain(vec![(
                make_header(1, 21_000, Some(1_000_000_000)),
                B256::ZERO,
                vec![(tx, sender)],
                vec![receipt],
            )]);

            let result = chain_to_rpc_blocks(&chain, proto::BlockStatus::Committed).unwrap();
            assert_eq!(
                result[0].transactions[0].receipt.as_ref().unwrap().tx_type,
                expected_proto_type as i32,
                "mismatch for {tempo_type:?}"
            );
        }
    }

    // ==================== EIP-1559 effective gas price ====================

    #[test]
    fn test_eip1559_effective_gas_price() {
        let sender = Address::with_last_byte(0x01);
        let to = TxKind::Call(Address::with_last_byte(0x02));
        let base_fee: u64 = 1_000_000_000;
        let max_fee: u128 = 3_000_000_000;
        let max_priority: u128 = 500_000_000;
        let tx = make_eip1559_tx(0, max_fee, max_priority, to);
        let receipt = make_receipt(TempoTxType::Eip1559, true, 21_000, vec![]);

        let chain = make_chain(vec![(
            make_header(1, 21_000, Some(base_fee)),
            B256::ZERO,
            vec![(tx, sender)],
            vec![receipt],
        )]);

        let result = chain_to_rpc_blocks(&chain, proto::BlockStatus::Committed).unwrap();
        let rpc_receipt = result[0].transactions[0].receipt.as_ref().unwrap();
        // effective = min(max_fee, base_fee + max_priority) = min(3B, 1.5B) = 1.5B
        let expected: u128 = base_fee as u128 + max_priority;
        assert_eq!(rpc_receipt.effective_gas_price, expected.to_le_bytes().to_vec());
    }

    // ==================== Multi-block chain ====================

    #[test]
    fn test_multi_block_chain() {
        let chain = make_chain(vec![
            (make_header(10, 0, None), B256::with_last_byte(0x10), vec![], vec![]),
            (make_header(11, 0, None), B256::with_last_byte(0x11), vec![], vec![]),
            (make_header(12, 0, None), B256::with_last_byte(0x12), vec![], vec![]),
        ]);

        let result = chain_to_rpc_blocks(&chain, proto::BlockStatus::Committed).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].number, 10);
        assert_eq!(result[1].number, 11);
        assert_eq!(result[2].number, 12);
        assert_eq!(result[0].hash, B256::with_last_byte(0x10).to_vec());
        assert_eq!(result[1].hash, B256::with_last_byte(0x11).to_vec());
        assert_eq!(result[2].hash, B256::with_last_byte(0x12).to_vec());
    }

    // ==================== Withdrawals ====================

    #[test]
    fn test_block_with_withdrawals() {
        let addr = Address::with_last_byte(0x42);
        let header = make_header(1, 0, None);
        let block_hash = B256::ZERO;

        let sealed_header = SealedHeader::new(header, block_hash);
        let body = BlockBody {
            transactions: vec![],
            ommers: vec![],
            withdrawals: Some(
                vec![Withdrawal {
                    index: 0,
                    validator_index: 100,
                    address: addr,
                    amount: 32_000_000_000,
                }]
                .into(),
            ),
        };
        let block = SealedBlock::<Block>::from_sealed_parts(sealed_header, body)
            .with_senders(vec![]);

        let chain = Chain::<TempoPrimitives>::new(
            vec![block],
            ExecutionOutcome {
                bundle: Default::default(),
                receipts: vec![vec![]],
                first_block: 1,
                requests: Default::default(),
            },
            BTreeMap::new(),
            BTreeMap::new(),
        );

        let result = chain_to_rpc_blocks(&chain, proto::BlockStatus::Committed).unwrap();
        let withdrawals = result[0].withdrawals.as_ref().unwrap();
        assert_eq!(withdrawals.items.len(), 1);
        assert_eq!(withdrawals.items[0].index, 0);
        assert_eq!(withdrawals.items[0].validator_index, 100);
        assert_eq!(withdrawals.items[0].address, addr.to_vec());
        assert_eq!(withdrawals.items[0].amount, 32_000_000_000);
    }

    #[test]
    fn test_block_without_withdrawals() {
        let chain = make_chain(vec![(make_header(1, 0, None), B256::ZERO, vec![], vec![])]);
        let result = chain_to_rpc_blocks(&chain, proto::BlockStatus::Committed).unwrap();
        assert!(result[0].withdrawals.is_none());
    }

    // ==================== Ommers / uncles ====================

    #[test]
    fn test_block_with_ommers() {
        let ommer_header = make_header(0, 0, None);
        let block_hash = B256::ZERO;
        let header = make_header(1, 0, None);

        let sealed_header = SealedHeader::new(header, block_hash);
        let body = BlockBody {
            transactions: vec![],
            ommers: vec![ommer_header],
            withdrawals: None,
        };
        let block = SealedBlock::<Block>::from_sealed_parts(sealed_header, body)
            .with_senders(vec![]);

        let chain = Chain::<TempoPrimitives>::new(
            vec![block],
            ExecutionOutcome {
                bundle: Default::default(),
                receipts: vec![vec![]],
                first_block: 1,
                requests: Default::default(),
            },
            BTreeMap::new(),
            BTreeMap::new(),
        );

        let result = chain_to_rpc_blocks(&chain, proto::BlockStatus::Committed).unwrap();
        assert_eq!(result[0].uncles.len(), 1);
        assert_eq!(result[0].uncles[0].len(), 32); // B256 hash
    }
}
