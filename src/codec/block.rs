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
