use crate::proto;
use alloy_consensus::Signed;
use alloy_primitives::{Address, B64, B256, BlockHash, Bloom, Bytes, TxHash, U256};
use eyre::{OptionExt, eyre};
use std::{collections::BTreeMap, sync::Arc};
use tempo_primitives::{
    AASigned, Block, TempoPrimitives, TempoSignature, TempoTxEnvelope,
    transaction::{
        Call, KeyAuthorization, KeychainSignature, KeychainVersion, PrimitiveSignature,
        TempoSignedAuthorization, TokenLimit,
        tt_signature::{P256SignatureWithPreHash, WebAuthnSignature},
    },
};

impl TryFrom<&reth_exex::ExExNotification<TempoPrimitives>> for proto::ExExNotification {
    type Error = eyre::Error;

    fn try_from(
        notification: &reth_exex::ExExNotification<TempoPrimitives>,
    ) -> Result<Self, Self::Error> {
        let notification = match notification {
            reth_exex::ExExNotification::ChainCommitted { new } => {
                proto::ex_ex_notification::Notification::ChainCommitted(proto::ChainCommitted {
                    new: Some(new.as_ref().try_into()?),
                })
            }
            reth_exex::ExExNotification::ChainReorged { old, new } => {
                proto::ex_ex_notification::Notification::ChainReorged(proto::ChainReorged {
                    old: Some(old.as_ref().try_into()?),
                    new: Some(new.as_ref().try_into()?),
                })
            }
            reth_exex::ExExNotification::ChainReverted { old } => {
                proto::ex_ex_notification::Notification::ChainReverted(proto::ChainReverted {
                    old: Some(old.as_ref().try_into()?),
                })
            }
        };

        Ok(proto::ExExNotification {
            notification: Some(notification),
        })
    }
}

impl TryFrom<&reth::providers::Chain<TempoPrimitives>> for proto::Chain {
    type Error = eyre::Error;

    fn try_from(chain: &reth::providers::Chain<TempoPrimitives>) -> Result<Self, Self::Error> {
        let bundle_state = chain.execution_outcome().state();
        Ok(proto::Chain {
            blocks: chain
                .blocks_iter()
                .map(|block| {
                    Ok(proto::Block {
                        header: Some(proto::SealedHeader {
                            hash: block.hash().to_vec(),
                            header: Some(block.header().into()),
                        }),
                        body: block
                            .body()
                            .transactions
                            .iter()
                            .map(TryInto::try_into)
                            .collect::<eyre::Result<_>>()?,
                        ommers: block.body().ommers.iter().map(Into::into).collect(),
                        senders: block
                            .senders()
                            .iter()
                            .map(|sender| sender.to_vec())
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
                    })
                })
                .collect::<eyre::Result<_>>()?,
            execution_outcome: Some(proto::ExecutionOutcome {
                bundle: Some(proto::BundleState {
                    state: bundle_state
                        .state
                        .iter()
                        .map(|(address, account)| (*address, account).try_into())
                        .collect::<eyre::Result<_>>()?,
                    contracts: bundle_state
                        .contracts
                        .iter()
                        .map(|(hash, bytecode)| {
                            Ok(proto::ContractBytecode {
                                hash: hash.to_vec(),
                                bytecode: Some(bytecode.try_into()?),
                            })
                        })
                        .collect::<eyre::Result<_>>()?,
                    reverts: bundle_state
                        .reverts
                        .iter()
                        .map(|block_reverts| {
                            Ok(proto::BlockReverts {
                                reverts: block_reverts
                                    .iter()
                                    .map(|(address, revert)| (*address, revert).try_into())
                                    .collect::<eyre::Result<_>>()?,
                            })
                        })
                        .collect::<eyre::Result<_>>()?,
                    state_size: bundle_state.state_size as u64,
                    reverts_size: bundle_state.reverts_size as u64,
                }),
                receipts: chain
                    .execution_outcome()
                    .receipts()
                    .iter()
                    .map(|block_receipts| {
                        Ok(proto::BlockReceipts {
                            receipts: block_receipts
                                .iter()
                                .map(TryInto::try_into)
                                .collect::<eyre::Result<_>>()?,
                        })
                    })
                    .collect::<eyre::Result<_>>()?,
                first_block: chain.execution_outcome().first_block,
                requests: chain
                    .execution_outcome()
                    .requests
                    .iter()
                    .map(|requests| proto::Requests {
                        requests: requests.iter().map(|b| b.to_vec()).collect(),
                    })
                    .collect(),
            }),
        })
    }
}

impl From<&tempo_primitives::TempoHeader> for proto::Header {
    fn from(header: &tempo_primitives::TempoHeader) -> Self {
        let inner = &header.inner;
        proto::Header {
            parent_hash: inner.parent_hash.to_vec(),
            ommers_hash: inner.ommers_hash.to_vec(),
            beneficiary: inner.beneficiary.to_vec(),
            state_root: inner.state_root.to_vec(),
            transactions_root: inner.transactions_root.to_vec(),
            receipts_root: inner.receipts_root.to_vec(),
            withdrawals_root: inner.withdrawals_root.map(|root| root.to_vec()),
            logs_bloom: inner.logs_bloom.to_vec(),
            difficulty: inner.difficulty.to_le_bytes_vec(),
            number: inner.number,
            gas_limit: inner.gas_limit,
            gas_used: inner.gas_used,
            timestamp: inner.timestamp,
            mix_hash: inner.mix_hash.to_vec(),
            nonce: inner.nonce.to_vec(),
            base_fee_per_gas: inner.base_fee_per_gas,
            blob_gas_used: inner.blob_gas_used,
            excess_blob_gas: inner.excess_blob_gas,
            parent_beacon_block_root: inner.parent_beacon_block_root.map(|root| root.to_vec()),
            extra_data: inner.extra_data.to_vec(),
            general_gas_limit: header.general_gas_limit,
            shared_gas_limit: header.shared_gas_limit,
            timestamp_millis_part: header.timestamp_millis_part,
        }
    }
}

fn eth_tx_to_proto(
    hash: &TxHash,
    signature: &alloy_primitives::Signature,
    transaction: proto::transaction::Transaction,
) -> proto::Transaction {
    proto::Transaction {
        transaction: Some(transaction),
        hash: hash.to_vec(),
        signature: Some(proto::transaction::Signature::EthSignature(
            signature.into(),
        )),
    }
}

impl TryFrom<&tempo_primitives::TempoTxEnvelope> for proto::Transaction {
    type Error = eyre::Error;

    fn try_from(transaction: &tempo_primitives::TempoTxEnvelope) -> Result<Self, Self::Error> {
        match transaction {
            TempoTxEnvelope::Legacy(signed) => {
                let alloy_consensus::TxLegacy {
                    chain_id,
                    nonce,
                    gas_limit,
                    gas_price,
                    to,
                    value,
                    input,
                } = signed.tx();
                Ok(eth_tx_to_proto(
                    signed.hash(),
                    signed.signature(),
                    proto::transaction::Transaction::Legacy(proto::TransactionLegacy {
                        chain_id: *chain_id,
                        nonce: *nonce,
                        gas_price: gas_price.to_le_bytes().to_vec(),
                        gas_limit: gas_limit.to_le_bytes().to_vec(),
                        to: Some(to.into()),
                        value: value.to_le_bytes_vec(),
                        input: input.to_vec(),
                    }),
                ))
            }
            TempoTxEnvelope::Eip2930(signed) => {
                let alloy_consensus::TxEip2930 {
                    chain_id,
                    nonce,
                    gas_price,
                    gas_limit,
                    to,
                    value,
                    access_list,
                    input,
                } = signed.tx();
                Ok(eth_tx_to_proto(
                    signed.hash(),
                    signed.signature(),
                    proto::transaction::Transaction::Eip2930(proto::TransactionEip2930 {
                        chain_id: *chain_id,
                        nonce: *nonce,
                        gas_price: gas_price.to_le_bytes().to_vec(),
                        gas_limit: gas_limit.to_le_bytes().to_vec(),
                        to: Some(to.into()),
                        value: value.to_le_bytes_vec(),
                        access_list: access_list.iter().map(Into::into).collect(),
                        input: input.to_vec(),
                    }),
                ))
            }
            TempoTxEnvelope::Eip1559(signed) => {
                let alloy_consensus::TxEip1559 {
                    chain_id,
                    nonce,
                    gas_limit,
                    max_fee_per_gas,
                    max_priority_fee_per_gas,
                    to,
                    value,
                    access_list,
                    input,
                } = signed.tx();
                Ok(eth_tx_to_proto(
                    signed.hash(),
                    signed.signature(),
                    proto::transaction::Transaction::Eip1559(proto::TransactionEip1559 {
                        chain_id: *chain_id,
                        nonce: *nonce,
                        gas_limit: gas_limit.to_le_bytes().to_vec(),
                        max_fee_per_gas: max_fee_per_gas.to_le_bytes().to_vec(),
                        max_priority_fee_per_gas: max_priority_fee_per_gas.to_le_bytes().to_vec(),
                        to: Some(to.into()),
                        value: value.to_le_bytes_vec(),
                        access_list: access_list.iter().map(Into::into).collect(),
                        input: input.to_vec(),
                    }),
                ))
            }
            TempoTxEnvelope::Eip7702(signed) => {
                let alloy_consensus::TxEip7702 {
                    chain_id,
                    nonce,
                    gas_limit,
                    max_fee_per_gas,
                    max_priority_fee_per_gas,
                    to,
                    value,
                    access_list,
                    authorization_list,
                    input,
                } = signed.tx();
                let authorization_list = authorization_list
                    .iter()
                    .map(|authorization| proto::AuthorizationListItem {
                        authorization: Some(proto::Authorization {
                            chain_id: authorization.chain_id().to_le_bytes_vec(),
                            address: authorization.address().to_vec(),
                            nonce: authorization.nonce(),
                        }),
                        signature: Some(proto::Signature {
                            r: authorization.r().to_le_bytes_vec(),
                            s: authorization.s().to_le_bytes_vec(),
                            y_parity: authorization.y_parity() as u32,
                        }),
                    })
                    .collect();
                Ok(eth_tx_to_proto(
                    signed.hash(),
                    signed.signature(),
                    proto::transaction::Transaction::Eip7702(proto::TransactionEip7702 {
                        chain_id: *chain_id,
                        nonce: *nonce,
                        gas_limit: gas_limit.to_le_bytes().to_vec(),
                        max_fee_per_gas: max_fee_per_gas.to_le_bytes().to_vec(),
                        max_priority_fee_per_gas: max_priority_fee_per_gas.to_le_bytes().to_vec(),
                        to: to.to_vec(),
                        value: value.to_le_bytes_vec(),
                        access_list: access_list.iter().map(Into::into).collect(),
                        authorization_list,
                        input: input.to_vec(),
                    }),
                ))
            }
            TempoTxEnvelope::AA(signed) => {
                let tx = signed.tx();

                let hash = signed.hash().to_vec();
                let signature: proto::TempoSignature = signed.signature().into();
                let transaction = proto::transaction::Transaction::Tempo(proto::TransactionTempo {
                    chain_id: tx.chain_id,
                    max_fee_per_gas: tx.max_fee_per_gas.to_le_bytes().to_vec(),
                    max_priority_fee_per_gas: tx.max_priority_fee_per_gas.to_le_bytes().to_vec(),
                    gas_limit: tx.gas_limit,
                    calls: tx
                        .calls
                        .iter()
                        .map(|call| proto::Call {
                            to: Some((&call.to).into()),
                            value: call.value.to_le_bytes::<32>().to_vec(),
                            input: call.input.to_vec(),
                        })
                        .collect(),
                    access_list: tx.access_list.iter().map(Into::into).collect(),
                    nonce_key: tx.nonce_key.to_le_bytes::<32>().to_vec(),
                    nonce: tx.nonce,
                    fee_token: tx.fee_token.map(|ft| ft.to_vec()),
                    fee_payer_signature: tx.fee_payer_signature.map(|sig| (&sig).into()),
                    valid_before: tx.valid_before,
                    valid_after: tx.valid_after,
                    key_authorization: tx.key_authorization.as_ref().map(|ka| {
                        proto::KeyAuthorization {
                            chain_id: ka.chain_id,
                            key_type: (ka.key_type as u8).into(),
                            key_id: ka.key_id.to_vec(),
                            expiry: ka.expiry,
                            limits: ka.limits.as_ref().map(|limits| proto::TokenLimits {
                                items: limits
                                    .iter()
                                    .map(|limit| proto::TokenLimit {
                                        token: limit.token.to_vec(),
                                        limit: limit.limit.to_le_bytes::<32>().to_vec(),
                                    })
                                    .collect(),
                            }),
                            signature: Some((&ka.signature).into()),
                        }
                    }),
                    aa_authorization_list: tx
                        .tempo_authorization_list
                        .iter()
                        .map(|item| proto::TempoAuthorization {
                            chain_id: item.chain_id.to_le_bytes_vec(),
                            address: item.address.to_vec(),
                            nonce: item.nonce,
                            signature: Some(item.signature().into()),
                        })
                        .collect(),
                });
                Ok(Self {
                    transaction: Some(transaction),
                    hash,
                    signature: Some(proto::transaction::Signature::TempoSignature(signature)),
                })
            }
        }
    }
}

impl From<&alloy_primitives::Signature> for proto::Signature {
    fn from(value: &alloy_primitives::Signature) -> Self {
        proto::Signature {
            r: value.r().to_le_bytes_vec(),
            s: value.s().to_le_bytes_vec(),
            y_parity: value.v() as u8 as u32,
        }
    }
}

impl From<KeychainVersion> for proto::KeychainVersion {
    fn from(value: KeychainVersion) -> Self {
        match value {
            KeychainVersion::V1 => proto::KeychainVersion::V1,
            KeychainVersion::V2 => proto::KeychainVersion::V2,
        }
    }
}

impl From<&PrimitiveSignature> for proto::PrimitiveSignature {
    fn from(value: &PrimitiveSignature) -> Self {
        let signature = match value {
            PrimitiveSignature::Secp256k1(s) => {
                proto::primitive_signature::Signature::Secp256k1(s.into())
            }
            PrimitiveSignature::P256(s) => {
                let inner_signature = proto::P256SignatureWithPreHash {
                    r: s.r.to_vec(),
                    s: s.s.to_vec(),
                    pub_key_x: s.pub_key_x.to_vec(),
                    pub_key_y: s.pub_key_y.to_vec(),
                    pre_hash: s.pre_hash,
                };
                proto::primitive_signature::Signature::P256(inner_signature)
            }
            PrimitiveSignature::WebAuthn(s) => {
                let inner_signature = proto::WebAuthnSignature {
                    r: s.r.to_vec(),
                    s: s.s.to_vec(),
                    pub_key_x: s.pub_key_x.to_vec(),
                    pub_key_y: s.pub_key_y.to_vec(),
                    webauthn_data: s.webauthn_data.to_vec(),
                };
                proto::primitive_signature::Signature::Webauthn(inner_signature)
            }
        };

        Self {
            signature: Some(signature),
        }
    }
}

impl From<&tempo_primitives::TempoSignature> for proto::TempoSignature {
    fn from(value: &tempo_primitives::TempoSignature) -> Self {
        match value {
            TempoSignature::Primitive(s) => proto::TempoSignature {
                signature: Some(proto::tempo_signature::Signature::Primitive(s.into())),
            },
            TempoSignature::Keychain(s) => proto::TempoSignature {
                signature: Some(proto::tempo_signature::Signature::Keychain(
                    proto::KeychainSignature {
                        user_address: s.user_address.to_vec(),
                        signature: Some((&s.signature).into()),
                        version: proto::KeychainVersion::from(s.version).into(),
                    },
                )),
            },
        }
    }
}

impl From<&alloy_primitives::TxKind> for proto::TxKind {
    fn from(kind: &alloy_primitives::TxKind) -> Self {
        proto::TxKind {
            kind: match kind {
                alloy_primitives::TxKind::Create => Some(proto::tx_kind::Kind::Create(())),
                alloy_primitives::TxKind::Call(address) => {
                    Some(proto::tx_kind::Kind::Call(address.to_vec()))
                }
            },
        }
    }
}

impl From<&alloy_eips::eip2930::AccessListItem> for proto::AccessListItem {
    fn from(item: &alloy_eips::eip2930::AccessListItem) -> Self {
        proto::AccessListItem {
            address: item.address.to_vec(),
            storage_keys: item.storage_keys.iter().map(|key| key.to_vec()).collect(),
        }
    }
}

impl TryFrom<(Address, &reth::revm::db::BundleAccount)> for proto::BundleAccount {
    type Error = eyre::Error;

    fn try_from(
        (address, account): (Address, &reth::revm::db::BundleAccount),
    ) -> Result<Self, Self::Error> {
        Ok(proto::BundleAccount {
            address: address.to_vec(),
            info: account.info.as_ref().map(TryInto::try_into).transpose()?,
            original_info: account
                .original_info
                .as_ref()
                .map(TryInto::try_into)
                .transpose()?,
            storage: account
                .storage
                .iter()
                .map(|(key, slot)| proto::StorageSlot {
                    key: key.to_le_bytes_vec(),
                    previous_or_original_value: slot.previous_or_original_value.to_le_bytes_vec(),
                    present_value: slot.present_value.to_le_bytes_vec(),
                })
                .collect(),
            status: proto::AccountStatus::from(account.status) as i32,
        })
    }
}

impl TryFrom<&reth::revm::state::AccountInfo> for proto::AccountInfo {
    type Error = eyre::Error;

    fn try_from(account_info: &reth::revm::state::AccountInfo) -> Result<Self, Self::Error> {
        Ok(proto::AccountInfo {
            balance: account_info.balance.to_le_bytes_vec(),
            nonce: account_info.nonce,
            code_hash: account_info.code_hash.to_vec(),
            code: account_info
                .code
                .as_ref()
                .map(TryInto::try_into)
                .transpose()?,
        })
    }
}

impl TryFrom<&reth::revm::bytecode::Bytecode> for proto::Bytecode {
    type Error = eyre::Error;

    fn try_from(bytecode: &reth::revm::state::Bytecode) -> Result<Self, Self::Error> {
        let bytecode = match bytecode {
            reth::revm::state::Bytecode::LegacyAnalyzed(legacy_analyzed) => {
                proto::bytecode::Bytecode::LegacyAnalyzed(proto::LegacyAnalyzedBytecode {
                    bytecode: legacy_analyzed.bytecode().to_vec(),
                    original_len: legacy_analyzed.original_len() as u64,
                    jump_table: legacy_analyzed.jump_table().as_slice().to_vec(),
                    jump_table_len: legacy_analyzed.jump_table().len() as u64,
                })
            }
            reth::revm::state::Bytecode::Eip7702(eip7702) => {
                proto::bytecode::Bytecode::Eip7702(proto::Eip7702Bytecode {
                    delegated_address: eip7702.delegated_address.to_vec(),
                    version: eip7702.version as u64,
                    raw: eip7702.raw.to_vec(),
                })
            }
        };
        Ok(proto::Bytecode {
            bytecode: Some(bytecode),
        })
    }
}

impl From<reth::revm::db::AccountStatus> for proto::AccountStatus {
    fn from(status: reth::revm::db::AccountStatus) -> Self {
        match status {
            reth::revm::db::AccountStatus::LoadedNotExisting => {
                proto::AccountStatus::LoadedNotExisting
            }
            reth::revm::db::AccountStatus::Loaded => proto::AccountStatus::Loaded,
            reth::revm::db::AccountStatus::LoadedEmptyEIP161 => {
                proto::AccountStatus::LoadedEmptyEip161
            }
            reth::revm::db::AccountStatus::InMemoryChange => proto::AccountStatus::InMemoryChange,
            reth::revm::db::AccountStatus::Changed => proto::AccountStatus::Changed,
            reth::revm::db::AccountStatus::Destroyed => proto::AccountStatus::Destroyed,
            reth::revm::db::AccountStatus::DestroyedChanged => {
                proto::AccountStatus::DestroyedChanged
            }
            reth::revm::db::AccountStatus::DestroyedAgain => proto::AccountStatus::DestroyedAgain,
        }
    }
}

impl TryFrom<(Address, &reth::revm::db::states::reverts::AccountRevert)> for proto::Revert {
    type Error = eyre::Error;

    fn try_from(
        (address, revert): (Address, &reth::revm::db::states::reverts::AccountRevert),
    ) -> Result<Self, Self::Error> {
        Ok(proto::Revert {
            address: address.to_vec(),
            account: Some(proto::AccountInfoRevert {
                revert: Some(match &revert.account {
                    reth::revm::db::states::reverts::AccountInfoRevert::DoNothing => {
                        proto::account_info_revert::Revert::DoNothing(())
                    }
                    reth::revm::db::states::reverts::AccountInfoRevert::DeleteIt => {
                        proto::account_info_revert::Revert::DeleteIt(())
                    }
                    reth::revm::db::states::reverts::AccountInfoRevert::RevertTo(account_info) => {
                        proto::account_info_revert::Revert::RevertTo(account_info.try_into()?)
                    }
                }),
            }),
            storage: revert
                .storage
                .iter()
                .map(|(key, slot)| {
                    Ok(proto::RevertToSlot {
                        key: key.to_le_bytes_vec(),
                        revert: Some(match slot {
                            reth::revm::db::RevertToSlot::Some(value) => {
                                proto::revert_to_slot::Revert::Some(value.to_le_bytes_vec())
                            }
                            reth::revm::db::RevertToSlot::Destroyed => {
                                proto::revert_to_slot::Revert::Destroyed(())
                            }
                        }),
                    })
                })
                .collect::<eyre::Result<_>>()?,
            previous_status: proto::AccountStatus::from(revert.previous_status) as i32,
            wipe_storage: revert.wipe_storage,
        })
    }
}

impl TryFrom<&tempo_primitives::TempoReceipt> for proto::Receipt {
    type Error = eyre::Error;

    fn try_from(receipt: &tempo_primitives::TempoReceipt) -> Result<Self, Self::Error> {
        Ok(proto::Receipt {
            receipt: Some(proto::receipt::Receipt::NonEmpty(receipt.try_into()?)),
        })
    }
}

impl TryFrom<&tempo_primitives::TempoReceipt> for proto::NonEmptyReceipt {
    type Error = eyre::Error;

    fn try_from(receipt: &tempo_primitives::TempoReceipt) -> Result<Self, Self::Error> {
        Ok(proto::NonEmptyReceipt {
            tx_type: match receipt.tx_type {
                tempo_primitives::TempoTxType::Legacy => proto::TxType::Legacy,
                tempo_primitives::TempoTxType::Eip2930 => proto::TxType::Eip2930,
                tempo_primitives::TempoTxType::Eip1559 => proto::TxType::Eip1559,
                tempo_primitives::TempoTxType::Eip7702 => proto::TxType::Eip7702,
                tempo_primitives::TempoTxType::AA => proto::TxType::Tempo,
            } as i32,
            success: receipt.success,
            cumulative_gas_used: receipt.cumulative_gas_used,
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
        })
    }
}

impl TryFrom<&proto::ExExNotification> for reth_exex::ExExNotification<TempoPrimitives> {
    type Error = eyre::Error;

    fn try_from(notification: &proto::ExExNotification) -> Result<Self, Self::Error> {
        Ok(
            match notification
                .notification
                .as_ref()
                .ok_or_eyre("no notification")?
            {
                proto::ex_ex_notification::Notification::ChainCommitted(
                    proto::ChainCommitted { new },
                ) => reth_exex::ExExNotification::ChainCommitted {
                    new: Arc::new(new.as_ref().ok_or_eyre("no new chain")?.try_into()?),
                },
                proto::ex_ex_notification::Notification::ChainReorged(proto::ChainReorged {
                    old,
                    new,
                }) => reth_exex::ExExNotification::ChainReorged {
                    old: Arc::new(old.as_ref().ok_or_eyre("no old chain")?.try_into()?),
                    new: Arc::new(new.as_ref().ok_or_eyre("no new chain")?.try_into()?),
                },
                proto::ex_ex_notification::Notification::ChainReverted(proto::ChainReverted {
                    old,
                }) => reth_exex::ExExNotification::ChainReverted {
                    old: Arc::new(old.as_ref().ok_or_eyre("no old chain")?.try_into()?),
                },
            },
        )
    }
}

impl TryFrom<&proto::Chain> for reth::providers::Chain<TempoPrimitives> {
    type Error = eyre::Error;

    fn try_from(chain: &proto::Chain) -> Result<Self, Self::Error> {
        let execution_outcome = chain
            .execution_outcome
            .as_ref()
            .ok_or_eyre("no execution outcome")?;
        let bundle = execution_outcome.bundle.as_ref().ok_or_eyre("no bundle")?;
        Ok(reth::providers::Chain::<TempoPrimitives>::new(
            chain
                .blocks
                .iter()
                .map(TryInto::try_into)
                .collect::<eyre::Result<Vec<_>>>()?,
            reth::providers::ExecutionOutcome {
                bundle: reth::revm::db::BundleState {
                    state: bundle
                        .state
                        .iter()
                        .map(TryInto::try_into)
                        .collect::<eyre::Result<_>>()?,
                    contracts: bundle
                        .contracts
                        .iter()
                        .map(|contract| {
                            Ok((
                                B256::try_from(contract.hash.as_slice())?,
                                contract
                                    .bytecode
                                    .as_ref()
                                    .ok_or_eyre("no bytecode")?
                                    .try_into()?,
                            ))
                        })
                        .collect::<eyre::Result<_>>()?,
                    reverts: reth::revm::db::states::reverts::Reverts::new(
                        bundle
                            .reverts
                            .iter()
                            .map(|block_reverts| {
                                block_reverts
                                    .reverts
                                    .iter()
                                    .map(TryInto::try_into)
                                    .collect::<eyre::Result<_>>()
                            })
                            .collect::<eyre::Result<_>>()?,
                    ),
                    state_size: bundle.state_size as usize,
                    reverts_size: bundle.reverts_size as usize,
                },
                receipts: execution_outcome
                    .receipts
                    .iter()
                    .map(|block_receipts| {
                        block_receipts
                            .receipts
                            .iter()
                            .map(|receipt| receipt.try_into())
                            .collect::<eyre::Result<_>>()
                    })
                    .collect::<eyre::Result<Vec<_>>>()?,
                first_block: execution_outcome.first_block,
                requests: Default::default(),
            },
            BTreeMap::new(),
            BTreeMap::new(),
        ))
    }
}

impl TryFrom<&proto::Block> for reth::primitives::RecoveredBlock<Block> {
    type Error = eyre::Error;

    fn try_from(block: &proto::Block) -> Result<Self, Self::Error> {
        let sealed_header = block.header.as_ref().ok_or_eyre("no sealed header")?;
        let header = sealed_header
            .header
            .as_ref()
            .ok_or_eyre("no header")?
            .try_into()?;
        let sealed_header = reth::primitives::SealedHeader::new(
            header,
            BlockHash::try_from(sealed_header.hash.as_slice())?,
        );

        let transactions = block
            .body
            .iter()
            .map(TryInto::try_into)
            .collect::<eyre::Result<_>>()?;
        let ommers = block
            .ommers
            .iter()
            .map(TryInto::try_into)
            .collect::<eyre::Result<_>>()?;
        let senders = block
            .senders
            .iter()
            .map(|sender| Address::try_from(sender.as_slice()))
            .collect::<Result<_, _>>()?;

        Ok(reth::primitives::SealedBlock::<Block>::from_sealed_parts(
            sealed_header,
            reth::primitives::BlockBody {
                transactions,
                ommers,
                withdrawals: Default::default(),
            },
        )
        .with_senders(senders))
    }
}

impl TryFrom<&proto::Header> for tempo_primitives::TempoHeader {
    type Error = eyre::Error;

    fn try_from(header: &proto::Header) -> Result<Self, Self::Error> {
        Ok(tempo_primitives::TempoHeader {
            inner: alloy_consensus::Header {
                parent_hash: B256::try_from(header.parent_hash.as_slice())?,
                ommers_hash: B256::try_from(header.ommers_hash.as_slice())?,
                beneficiary: Address::try_from(header.beneficiary.as_slice())?,
                state_root: B256::try_from(header.state_root.as_slice())?,
                transactions_root: B256::try_from(header.transactions_root.as_slice())?,
                receipts_root: B256::try_from(header.receipts_root.as_slice())?,
                withdrawals_root: header
                    .withdrawals_root
                    .as_ref()
                    .map(|root| B256::try_from(root.as_slice()))
                    .transpose()?,
                logs_bloom: Bloom::try_from(header.logs_bloom.as_slice())?,
                difficulty: U256::try_from_le_slice(&header.difficulty)
                    .ok_or_eyre("failed to parse difficulty")?,
                number: header.number,
                gas_limit: header.gas_limit,
                gas_used: header.gas_used,
                timestamp: header.timestamp,
                mix_hash: B256::try_from(header.mix_hash.as_slice())?,
                nonce: B64::try_from(header.nonce.as_slice())?,
                base_fee_per_gas: header.base_fee_per_gas,
                blob_gas_used: header.blob_gas_used,
                excess_blob_gas: header.excess_blob_gas,
                parent_beacon_block_root: header
                    .parent_beacon_block_root
                    .as_ref()
                    .map(|root| B256::try_from(root.as_slice()))
                    .transpose()?,
                requests_hash: None,
                extra_data: header.extra_data.as_slice().to_vec().into(),
            },
            general_gas_limit: header.general_gas_limit,
            shared_gas_limit: header.shared_gas_limit,
            timestamp_millis_part: header.timestamp_millis_part,
        })
    }
}

impl TryFrom<&proto::Transaction> for tempo_primitives::TempoTxEnvelope {
    type Error = eyre::Error;

    fn try_from(value: &proto::Transaction) -> Result<Self, Self::Error> {
        let hash = TxHash::try_from(value.hash.as_slice())?;
        let transaction = value.transaction.as_ref().ok_or_eyre("no transaction")?;

        let eth_signature = || -> eyre::Result<alloy_primitives::Signature> {
            let s = match value.signature.as_ref().ok_or_eyre("no signature")? {
                proto::transaction::Signature::EthSignature(s) => s,
                proto::transaction::Signature::TempoSignature(_) => {
                    return Err(eyre!("Invalid signature for non tempo transaction"));
                }
            };
            if s.y_parity > 1 {
                return Err(eyre::eyre!(
                    alloy_primitives::SignatureError::InvalidParity(s.y_parity as u64)
                ));
            }
            Ok(alloy_primitives::Signature::new(
                U256::try_from_le_slice(s.r.as_slice()).ok_or_eyre("failed to parse r")?,
                U256::try_from_le_slice(s.s.as_slice()).ok_or_eyre("failed to parse s")?,
                s.y_parity == 1,
            ))
        };

        match transaction {
            proto::transaction::Transaction::Tempo(t) => {
                let tempo_transaction = tempo_primitives::TempoTransaction {
                    chain_id: t.chain_id,
                    max_fee_per_gas: u128::from_le_bytes(t.max_fee_per_gas.as_slice().try_into()?),
                    max_priority_fee_per_gas: u128::from_le_bytes(
                        t.max_priority_fee_per_gas.as_slice().try_into()?,
                    ),
                    fee_token: t
                        .fee_token
                        .as_ref()
                        .map(|token| Address::try_from(token.as_slice()))
                        .transpose()?,
                    gas_limit: t.gas_limit,
                    calls: t
                        .calls
                        .iter()
                        .map(|call| {
                            let to = call.to.as_ref().ok_or_eyre("no `to` for call")?;
                            let call = Call {
                                to: to.try_into()?,
                                value: U256::try_from_le_slice(call.value.as_slice())
                                    .ok_or_eyre("call.value can't be converted to U256")?,
                                input: Bytes::copy_from_slice(&call.input),
                            };
                            Ok(call)
                        })
                        .collect::<eyre::Result<Vec<_>>>()?,
                    access_list: t
                        .access_list
                        .iter()
                        .map(|item| item.try_into())
                        .collect::<eyre::Result<Vec<_>>>()?
                        .into(),
                    nonce: t.nonce,
                    nonce_key: U256::try_from_le_slice(t.nonce_key.as_slice())
                        .ok_or_eyre("nonce key can't be converted to U256")?,
                    fee_payer_signature: t
                        .fee_payer_signature
                        .as_ref()
                        .map(TryInto::try_into)
                        .transpose()?,
                    valid_before: t.valid_before,
                    valid_after: t.valid_after,
                    key_authorization: t
                        .key_authorization
                        .as_ref()
                        .map(|ka| {
                            let authorization = KeyAuthorization {
                                chain_id: ka.chain_id,
                                key_type: proto::SignatureType::try_from(ka.key_type)?.into(),
                                key_id: ka.key_id.as_slice().try_into()?,
                                expiry: ka.expiry,
                                limits: ka
                                    .limits
                                    .as_ref()
                                    .map(|item| {
                                        item.items
                                            .iter()
                                            .map(|limit| {
                                                Ok(TokenLimit {
                                                    token: limit.token.as_slice().try_into()?,
                                                    limit: U256::from_le_slice(
                                                        limit.limit.as_slice(),
                                                    ),
                                                })
                                            })
                                            .collect::<eyre::Result<Vec<_>>>()
                                    })
                                    .transpose()?,
                            };
                            let signature = ka
                                .signature
                                .as_ref()
                                .ok_or_eyre("no signature in key authorization")?;
                            eyre::Ok(tempo_primitives::transaction::SignedKeyAuthorization {
                                authorization,
                                signature: signature.try_into()?,
                            })
                        })
                        .transpose()?,
                    tempo_authorization_list: t
                        .aa_authorization_list
                        .iter()
                        .map(|auth| {
                            let signature = auth
                                .signature
                                .as_ref()
                                .ok_or_eyre("no signature in tempo authorization list item")?;
                            Ok(TempoSignedAuthorization::new_unchecked(
                                alloy_eips::eip7702::Authorization {
                                    chain_id: U256::try_from_le_slice(&auth.chain_id)
                                        .ok_or_eyre("failed to parse chain_id")?,
                                    address: auth.address.as_slice().try_into()?,
                                    nonce: auth.nonce,
                                },
                                signature.try_into()?,
                            ))
                        })
                        .collect::<eyre::Result<_>>()?,
                };
                let signature: TempoSignature =
                    match value.signature.as_ref().ok_or_eyre("no signature")? {
                        proto::transaction::Signature::TempoSignature(s) => s.try_into()?,
                        proto::transaction::Signature::EthSignature(_) => {
                            return Err(eyre!("invalid signature for tempo transaction"));
                        }
                    };
                let signed = AASigned::new_unchecked(tempo_transaction, signature, hash);
                Ok(TempoTxEnvelope::AA(signed))
            }
            proto::transaction::Transaction::Legacy(proto::TransactionLegacy {
                chain_id,
                nonce,
                gas_price,
                gas_limit,
                to,
                value,
                input,
            }) => Ok(Signed::new_unchecked(
                alloy_consensus::TxLegacy {
                    chain_id: *chain_id,
                    nonce: *nonce,
                    gas_price: u128::from_le_bytes(gas_price.as_slice().try_into()?),
                    gas_limit: u64::from_le_bytes(gas_limit.as_slice().try_into()?),
                    to: to.as_ref().ok_or_eyre("no to")?.try_into()?,
                    value: U256::try_from_le_slice(value.as_slice())
                        .ok_or_eyre("failed to parse value")?,
                    input: input.to_vec().into(),
                },
                eth_signature()?,
                hash,
            )
            .into()),
            proto::transaction::Transaction::Eip2930(proto::TransactionEip2930 {
                chain_id,
                nonce,
                gas_price,
                gas_limit,
                to,
                value,
                access_list,
                input,
            }) => Ok(Signed::new_unchecked(
                alloy_consensus::TxEip2930 {
                    chain_id: *chain_id,
                    nonce: *nonce,
                    gas_price: u128::from_le_bytes(gas_price.as_slice().try_into()?),
                    gas_limit: u64::from_le_bytes(gas_limit.as_slice().try_into()?),
                    to: to.as_ref().ok_or_eyre("no to")?.try_into()?,
                    value: U256::try_from_le_slice(value.as_slice())
                        .ok_or_eyre("failed to parse value")?,
                    access_list: access_list
                        .iter()
                        .map(TryInto::try_into)
                        .collect::<eyre::Result<Vec<_>>>()?
                        .into(),
                    input: input.to_vec().into(),
                },
                eth_signature()?,
                hash,
            )
            .into()),
            proto::transaction::Transaction::Eip1559(proto::TransactionEip1559 {
                chain_id,
                nonce,
                gas_limit,
                max_fee_per_gas,
                max_priority_fee_per_gas,
                to,
                value,
                access_list,
                input,
            }) => Ok(Signed::new_unchecked(
                alloy_consensus::TxEip1559 {
                    chain_id: *chain_id,
                    nonce: *nonce,
                    gas_limit: u64::from_le_bytes(gas_limit.as_slice().try_into()?),
                    max_fee_per_gas: u128::from_le_bytes(max_fee_per_gas.as_slice().try_into()?),
                    max_priority_fee_per_gas: u128::from_le_bytes(
                        max_priority_fee_per_gas.as_slice().try_into()?,
                    ),
                    to: to.as_ref().ok_or_eyre("no to")?.try_into()?,
                    value: U256::try_from_le_slice(value.as_slice())
                        .ok_or_eyre("failed to parse value")?,
                    access_list: access_list
                        .iter()
                        .map(TryInto::try_into)
                        .collect::<eyre::Result<Vec<_>>>()?
                        .into(),
                    input: input.to_vec().into(),
                },
                eth_signature()?,
                hash,
            )
            .into()),
            proto::transaction::Transaction::Eip7702(proto::TransactionEip7702 {
                chain_id,
                nonce,
                gas_limit,
                max_fee_per_gas,
                max_priority_fee_per_gas,
                to,
                value,
                access_list,
                authorization_list,
                input,
            }) => Ok(Signed::new_unchecked(
                alloy_consensus::TxEip7702 {
                    chain_id: *chain_id,
                    nonce: *nonce,
                    gas_limit: u64::from_le_bytes(gas_limit.as_slice().try_into()?),
                    max_fee_per_gas: u128::from_le_bytes(max_fee_per_gas.as_slice().try_into()?),
                    max_priority_fee_per_gas: u128::from_le_bytes(
                        max_priority_fee_per_gas.as_slice().try_into()?,
                    ),
                    to: Address::try_from(to.as_slice())?,
                    value: U256::try_from_le_slice(value.as_slice())
                        .ok_or_eyre("failed to parse value")?,
                    access_list: access_list
                        .iter()
                        .map(TryInto::try_into)
                        .collect::<eyre::Result<Vec<_>>>()?
                        .into(),
                    authorization_list: authorization_list
                        .iter()
                        .map(|authorization| {
                            let signature = authorization
                                .signature
                                .as_ref()
                                .ok_or_eyre("no signature")?;

                            let r = U256::try_from_le_slice(signature.r.as_slice())
                                .ok_or_eyre("failed to parse r")?;
                            let s = U256::try_from_le_slice(signature.s.as_slice())
                                .ok_or_eyre("failed to parse s")?;
                            let y_parity = signature.y_parity as u8;

                            let authorization = authorization
                                .authorization
                                .as_ref()
                                .ok_or_eyre("no authorization")?;

                            let chain_id =
                                U256::from_le_bytes::<{ U256::BYTES }>(
                                    authorization.chain_id.as_slice().try_into().map_err(|_| {
                                        eyre::eyre!("chain_id must be exactly 8 bytes")
                                    })?,
                                );
                            let authorization = alloy_eips::eip7702::Authorization {
                                chain_id,
                                address: Address::try_from(authorization.address.as_slice())?,
                                nonce: authorization.nonce,
                            };

                            Ok(alloy_eips::eip7702::SignedAuthorization::new_unchecked(
                                authorization,
                                y_parity,
                                r,
                                s,
                            ))
                        })
                        .collect::<eyre::Result<Vec<_>>>()?,
                    input: input.to_vec().into(),
                },
                eth_signature()?,
                hash,
            )
            .into()),
        }
    }
}

impl From<proto::SignatureType> for tempo_primitives::SignatureType {
    fn from(value: proto::SignatureType) -> Self {
        match value {
            proto::SignatureType::Secp256k1 => Self::Secp256k1,
            proto::SignatureType::P256 => Self::P256,
            proto::SignatureType::Webauthn => Self::WebAuthn,
        }
    }
}

impl TryFrom<&proto::Signature> for alloy_primitives::Signature {
    type Error = eyre::Error;

    fn try_from(value: &proto::Signature) -> Result<Self, Self::Error> {
        let signature = alloy_primitives::Signature::new(
            U256::try_from_le_slice(value.r.as_slice()).ok_or_eyre("failed to parse r")?,
            U256::try_from_le_slice(value.s.as_slice()).ok_or_eyre("failed to parse s")?,
            value.y_parity == 1,
        );
        Ok(signature)
    }
}

impl TryFrom<&proto::PrimitiveSignature> for PrimitiveSignature {
    type Error = eyre::Error;

    fn try_from(value: &proto::PrimitiveSignature) -> Result<Self, Self::Error> {
        let signature = value
            .signature
            .as_ref()
            .ok_or_eyre("no primitive signature")?;

        let signature = match signature {
            proto::primitive_signature::Signature::Secp256k1(s) => {
                PrimitiveSignature::Secp256k1(s.try_into()?)
            }
            proto::primitive_signature::Signature::P256(s) => {
                PrimitiveSignature::P256(P256SignatureWithPreHash {
                    r: s.r.as_slice().try_into()?,
                    s: s.s.as_slice().try_into()?,
                    pub_key_x: s.pub_key_x.as_slice().try_into()?,
                    pub_key_y: s.pub_key_y.as_slice().try_into()?,
                    pre_hash: s.pre_hash,
                })
            }
            proto::primitive_signature::Signature::Webauthn(s) => {
                PrimitiveSignature::WebAuthn(WebAuthnSignature {
                    r: s.r.as_slice().try_into()?,
                    s: s.s.as_slice().try_into()?,
                    pub_key_x: s.pub_key_x.as_slice().try_into()?,
                    pub_key_y: s.pub_key_y.as_slice().try_into()?,
                    webauthn_data: Bytes::copy_from_slice(&s.webauthn_data),
                })
            }
        };
        Ok(signature)
    }
}

impl From<proto::KeychainVersion> for KeychainVersion {
    fn from(value: proto::KeychainVersion) -> Self {
        match value {
            proto::KeychainVersion::V1 => KeychainVersion::V1,
            proto::KeychainVersion::V2 => KeychainVersion::V2,
        }
    }
}

impl TryFrom<&proto::TempoSignature> for tempo_primitives::TempoSignature {
    type Error = eyre::Error;

    fn try_from(value: &proto::TempoSignature) -> Result<Self, Self::Error> {
        let signature = value.signature.as_ref().ok_or_eyre("no tempo signature")?;
        let signature = match signature {
            proto::tempo_signature::Signature::Primitive(s) => {
                tempo_primitives::TempoSignature::Primitive(s.try_into()?)
            }
            proto::tempo_signature::Signature::Keychain(s) => {
                let version: KeychainVersion = proto::KeychainVersion::try_from(s.version)?.into();
                let user_address = s.user_address.as_slice().try_into()?;
                let keychain_signature =
                    s.signature.as_ref().ok_or_eyre("no keychain signature")?;
                let keychain_signature = keychain_signature.try_into()?;
                let keychain = match version {
                    KeychainVersion::V1 => {
                        KeychainSignature::new_v1(user_address, keychain_signature)
                    }
                    KeychainVersion::V2 => KeychainSignature::new(user_address, keychain_signature),
                };
                tempo_primitives::TempoSignature::Keychain(keychain)
            }
        };
        Ok(signature)
    }
}

impl TryFrom<&proto::TxKind> for alloy_primitives::TxKind {
    type Error = eyre::Error;

    fn try_from(tx_kind: &proto::TxKind) -> Result<Self, Self::Error> {
        Ok(match tx_kind.kind.as_ref().ok_or_eyre("no kind")? {
            proto::tx_kind::Kind::Create(()) => alloy_primitives::TxKind::Create,
            proto::tx_kind::Kind::Call(address) => {
                alloy_primitives::TxKind::Call(Address::try_from(address.as_slice())?)
            }
        })
    }
}

impl TryFrom<&proto::AccessListItem> for alloy_eips::eip2930::AccessListItem {
    type Error = eyre::Error;

    fn try_from(item: &proto::AccessListItem) -> Result<Self, Self::Error> {
        Ok(alloy_eips::eip2930::AccessListItem {
            address: Address::try_from(item.address.as_slice())?,
            storage_keys: item
                .storage_keys
                .iter()
                .map(|key| B256::try_from(key.as_slice()))
                .collect::<Result<_, _>>()?,
        })
    }
}

impl TryFrom<&proto::AccountInfo> for reth::revm::state::AccountInfo {
    type Error = eyre::Error;

    fn try_from(account_info: &proto::AccountInfo) -> Result<Self, Self::Error> {
        Ok(reth::revm::state::AccountInfo {
            account_id: None,
            balance: U256::try_from_le_slice(account_info.balance.as_slice())
                .ok_or_eyre("failed to parse balance")?,
            nonce: account_info.nonce,
            code_hash: B256::try_from(account_info.code_hash.as_slice())?,
            code: account_info
                .code
                .as_ref()
                .map(TryInto::try_into)
                .transpose()?,
        })
    }
}

impl TryFrom<&proto::Bytecode> for reth::revm::state::Bytecode {
    type Error = eyre::Error;

    fn try_from(bytecode: &proto::Bytecode) -> Result<Self, Self::Error> {
        Ok(
            match bytecode.bytecode.as_ref().ok_or_eyre("no bytecode")? {
                proto::bytecode::Bytecode::LegacyAnalyzed(legacy_analyzed) => {
                    reth::revm::state::Bytecode::LegacyAnalyzed(Arc::new(
                        reth::revm::state::bytecode::LegacyAnalyzedBytecode::new(
                            legacy_analyzed.bytecode.clone().into(),
                            legacy_analyzed.original_len as usize,
                            reth::revm::state::bytecode::JumpTable::from_slice(
                                legacy_analyzed.jump_table.to_vec().as_slice(),
                                legacy_analyzed.jump_table_len as usize,
                            ),
                        ),
                    ))
                }
                proto::bytecode::Bytecode::Eip7702(eip7702) => {
                    reth::revm::bytecode::Bytecode::Eip7702(Arc::new(
                        reth::revm::bytecode::eip7702::Eip7702Bytecode {
                            delegated_address: Address::try_from(
                                eip7702.delegated_address.as_slice(),
                            )?,
                            version: eip7702.version as u8,
                            raw: eip7702.raw.as_slice().to_vec().into(),
                        },
                    ))
                }
            },
        )
    }
}

impl From<proto::AccountStatus> for reth::revm::db::AccountStatus {
    fn from(status: proto::AccountStatus) -> Self {
        match status {
            proto::AccountStatus::LoadedNotExisting => {
                reth::revm::db::AccountStatus::LoadedNotExisting
            }
            proto::AccountStatus::Loaded => reth::revm::db::AccountStatus::Loaded,
            proto::AccountStatus::LoadedEmptyEip161 => {
                reth::revm::db::AccountStatus::LoadedEmptyEIP161
            }
            proto::AccountStatus::InMemoryChange => reth::revm::db::AccountStatus::InMemoryChange,
            proto::AccountStatus::Changed => reth::revm::db::AccountStatus::Changed,
            proto::AccountStatus::Destroyed => reth::revm::db::AccountStatus::Destroyed,
            proto::AccountStatus::DestroyedChanged => {
                reth::revm::db::AccountStatus::DestroyedChanged
            }
            proto::AccountStatus::DestroyedAgain => reth::revm::db::AccountStatus::DestroyedAgain,
        }
    }
}

impl TryFrom<&proto::BundleAccount> for (Address, reth::revm::db::BundleAccount) {
    type Error = eyre::Error;

    fn try_from(account: &proto::BundleAccount) -> Result<Self, Self::Error> {
        Ok((
            Address::try_from(account.address.as_slice())?,
            reth::revm::db::BundleAccount {
                info: account.info.as_ref().map(TryInto::try_into).transpose()?,
                original_info: account
                    .original_info
                    .as_ref()
                    .map(TryInto::try_into)
                    .transpose()?,
                storage: account
                    .storage
                    .iter()
                    .map(|slot| {
                        Ok((
                            U256::try_from_le_slice(slot.key.as_slice())
                                .ok_or_eyre("failed to parse key")?,
                            reth::revm::db::states::StorageSlot {
                                previous_or_original_value: U256::try_from_le_slice(
                                    slot.previous_or_original_value.as_slice(),
                                )
                                .ok_or_eyre("failed to parse previous or original value")?,
                                present_value: U256::try_from_le_slice(
                                    slot.present_value.as_slice(),
                                )
                                .ok_or_eyre("failed to parse present value")?,
                            },
                        ))
                    })
                    .collect::<eyre::Result<_>>()?,
                status: proto::AccountStatus::try_from(account.status)?.into(),
            },
        ))
    }
}

impl TryFrom<&proto::Revert> for (Address, reth::revm::db::states::reverts::AccountRevert) {
    type Error = eyre::Error;

    fn try_from(revert: &proto::Revert) -> Result<Self, Self::Error> {
        Ok((
            Address::try_from(revert.address.as_slice())?,
            reth::revm::db::states::reverts::AccountRevert {
                account: match revert
                    .account
                    .as_ref()
                    .ok_or_eyre("no revert account")?
                    .revert
                    .as_ref()
                    .ok_or_eyre("no revert account revert")?
                {
                    proto::account_info_revert::Revert::DoNothing(()) => {
                        reth::revm::db::states::reverts::AccountInfoRevert::DoNothing
                    }
                    proto::account_info_revert::Revert::DeleteIt(()) => {
                        reth::revm::db::states::reverts::AccountInfoRevert::DeleteIt
                    }
                    proto::account_info_revert::Revert::RevertTo(account_info) => {
                        reth::revm::db::states::reverts::AccountInfoRevert::RevertTo(
                            account_info.try_into()?,
                        )
                    }
                },
                storage: revert
                    .storage
                    .iter()
                    .map(|slot| {
                        Ok((
                            U256::try_from_le_slice(slot.key.as_slice())
                                .ok_or_eyre("failed to parse slot key")?,
                            match slot.revert.as_ref().ok_or_eyre("no slot revert")? {
                                proto::revert_to_slot::Revert::Some(value) => {
                                    reth::revm::db::states::reverts::RevertToSlot::Some(
                                        U256::try_from_le_slice(value.as_slice())
                                            .ok_or_eyre("failed to parse slot revert")?,
                                    )
                                }
                                proto::revert_to_slot::Revert::Destroyed(()) => {
                                    reth::revm::db::states::reverts::RevertToSlot::Destroyed
                                }
                            },
                        ))
                    })
                    .collect::<eyre::Result<_>>()?,
                previous_status: proto::AccountStatus::try_from(revert.previous_status)?.into(),
                wipe_storage: revert.wipe_storage,
            },
        ))
    }
}

impl TryFrom<&proto::Receipt> for tempo_primitives::TempoReceipt {
    type Error = eyre::Error;

    fn try_from(receipt: &proto::Receipt) -> Result<Self, Self::Error> {
        match receipt.receipt.as_ref().ok_or_eyre("no receipt")? {
            proto::receipt::Receipt::Empty(()) => Err(eyre::eyre!("empty")),
            proto::receipt::Receipt::NonEmpty(receipt) => Ok(receipt.try_into()?),
        }
    }
}

impl TryFrom<&proto::NonEmptyReceipt> for tempo_primitives::TempoReceipt {
    type Error = eyre::Error;

    fn try_from(receipt: &proto::NonEmptyReceipt) -> Result<Self, Self::Error> {
        Ok(reth::primitives::Receipt {
            tx_type: match proto::TxType::try_from(receipt.tx_type)? {
                proto::TxType::Legacy => tempo_primitives::TempoTxType::Legacy,
                proto::TxType::Eip2930 => tempo_primitives::TempoTxType::Eip2930,
                proto::TxType::Eip1559 => tempo_primitives::TempoTxType::Eip1559,
                proto::TxType::Eip7702 => tempo_primitives::TempoTxType::Eip7702,
                proto::TxType::Tempo => tempo_primitives::TempoTxType::AA,
            },
            success: receipt.success,
            cumulative_gas_used: receipt.cumulative_gas_used,
            logs: receipt
                .logs
                .iter()
                .map(|log| {
                    let data = log.data.as_ref().ok_or_eyre("no log data")?;
                    Ok(reth::primitives::Log {
                        address: Address::try_from(log.address.as_slice())?,
                        data: reth::primitives::LogData::new_unchecked(
                            data.topics
                                .iter()
                                .map(|topic| Ok(B256::try_from(topic.as_slice())?))
                                .collect::<eyre::Result<_>>()?,
                            data.data.clone().into(),
                        ),
                    })
                })
                .collect::<eyre::Result<_>>()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HASH_SIZE: usize = 32;
    const ADDR_SIZE: usize = 20;
    const BLOOM_SIZE: usize = 256;
    const NONCE_SIZE: usize = 8;

    fn dummy_bytes(len: usize, seed: u8) -> Vec<u8> {
        (0..len).map(|i| seed.wrapping_add(i as u8)).collect()
    }

    fn test_header() -> proto::Header {
        proto::Header {
            parent_hash: dummy_bytes(HASH_SIZE, 1),
            ommers_hash: dummy_bytes(HASH_SIZE, 2),
            beneficiary: dummy_bytes(ADDR_SIZE, 3),
            state_root: dummy_bytes(HASH_SIZE, 4),
            transactions_root: dummy_bytes(HASH_SIZE, 5),
            receipts_root: dummy_bytes(HASH_SIZE, 6),
            withdrawals_root: Some(dummy_bytes(HASH_SIZE, 7)),
            logs_bloom: dummy_bytes(BLOOM_SIZE, 8),
            difficulty: U256::from(1000u64).to_le_bytes_vec(),
            number: 42,
            gas_limit: 30_000_000,
            gas_used: 21_000,
            timestamp: 1_700_000_000,
            mix_hash: dummy_bytes(HASH_SIZE, 9),
            nonce: dummy_bytes(NONCE_SIZE, 10),
            base_fee_per_gas: Some(1_000_000_000),
            blob_gas_used: Some(131072),
            excess_blob_gas: Some(0),
            parent_beacon_block_root: Some(dummy_bytes(HASH_SIZE, 11)),
            extra_data: vec![0xca, 0xfe],
            general_gas_limit: 15_000_000,
            shared_gas_limit: 15_000_000,
            timestamp_millis_part: 500,
        }
    }

    fn test_eth_signature() -> proto::Signature {
        proto::Signature {
            r: U256::from(12345u64).to_le_bytes_vec(),
            s: U256::from(67890u64).to_le_bytes_vec(),
            y_parity: 1,
        }
    }

    fn test_access_list() -> Vec<proto::AccessListItem> {
        vec![proto::AccessListItem {
            address: dummy_bytes(ADDR_SIZE, 50),
            storage_keys: vec![dummy_bytes(HASH_SIZE, 60), dummy_bytes(HASH_SIZE, 70)],
        }]
    }

    fn test_tx_kind_call() -> proto::TxKind {
        proto::TxKind {
            kind: Some(proto::tx_kind::Kind::Call(dummy_bytes(ADDR_SIZE, 20))),
        }
    }

    // ==================== Header tests ====================

    #[test]
    fn test_header_roundtrip() {
        let original = test_header();
        let domain: tempo_primitives::TempoHeader = (&original).try_into().unwrap();
        let roundtrip: proto::Header = (&domain).into();
        assert_eq!(original, roundtrip);
    }

    #[test]
    fn test_header_roundtrip_without_optionals() {
        let original = proto::Header {
            withdrawals_root: None,
            parent_beacon_block_root: None,
            base_fee_per_gas: None,
            blob_gas_used: None,
            excess_blob_gas: None,
            ..test_header()
        };
        let domain: tempo_primitives::TempoHeader = (&original).try_into().unwrap();
        let roundtrip: proto::Header = (&domain).into();
        assert_eq!(original, roundtrip);
    }

    // ==================== Transaction tests ====================

    #[test]
    fn test_legacy_tx_roundtrip() {
        let original = proto::Transaction {
            hash: dummy_bytes(HASH_SIZE, 100),
            signature: Some(proto::transaction::Signature::EthSignature(
                test_eth_signature(),
            )),
            transaction: Some(proto::transaction::Transaction::Legacy(
                proto::TransactionLegacy {
                    chain_id: Some(1),
                    nonce: 5,
                    gas_price: 1_000_000_000u128.to_le_bytes().to_vec(),
                    gas_limit: 21_000u64.to_le_bytes().to_vec(),
                    to: Some(test_tx_kind_call()),
                    value: U256::from(1_000_000u64).to_le_bytes_vec(),
                    input: vec![0xab, 0xcd],
                },
            )),
        };
        let domain: TempoTxEnvelope = (&original).try_into().unwrap();
        let roundtrip: proto::Transaction = (&domain).try_into().unwrap();
        assert_eq!(original, roundtrip);
    }

    #[test]
    fn test_eip2930_tx_roundtrip() {
        let original = proto::Transaction {
            hash: dummy_bytes(HASH_SIZE, 101),
            signature: Some(proto::transaction::Signature::EthSignature(
                test_eth_signature(),
            )),
            transaction: Some(proto::transaction::Transaction::Eip2930(
                proto::TransactionEip2930 {
                    chain_id: 1,
                    nonce: 10,
                    gas_price: 2_000_000_000u128.to_le_bytes().to_vec(),
                    gas_limit: 50_000u64.to_le_bytes().to_vec(),
                    to: Some(test_tx_kind_call()),
                    value: U256::from(500_000u64).to_le_bytes_vec(),
                    access_list: test_access_list(),
                    input: vec![0x01, 0x02, 0x03],
                },
            )),
        };
        let domain: TempoTxEnvelope = (&original).try_into().unwrap();
        let roundtrip: proto::Transaction = (&domain).try_into().unwrap();
        assert_eq!(original, roundtrip);
    }

    #[test]
    fn test_eip1559_tx_roundtrip() {
        let original = proto::Transaction {
            hash: dummy_bytes(HASH_SIZE, 102),
            signature: Some(proto::transaction::Signature::EthSignature(
                test_eth_signature(),
            )),
            transaction: Some(proto::transaction::Transaction::Eip1559(
                proto::TransactionEip1559 {
                    chain_id: 1,
                    nonce: 15,
                    gas_limit: 100_000u64.to_le_bytes().to_vec(),
                    max_fee_per_gas: 50_000_000_000u128.to_le_bytes().to_vec(),
                    max_priority_fee_per_gas: 2_000_000_000u128.to_le_bytes().to_vec(),
                    to: Some(test_tx_kind_call()),
                    value: U256::from(1_000_000_000u64).to_le_bytes_vec(),
                    access_list: test_access_list(),
                    input: vec![],
                },
            )),
        };
        let domain: TempoTxEnvelope = (&original).try_into().unwrap();
        let roundtrip: proto::Transaction = (&domain).try_into().unwrap();
        assert_eq!(original, roundtrip);
    }

    #[test]
    fn test_eip7702_tx_roundtrip() {
        let original = proto::Transaction {
            hash: dummy_bytes(HASH_SIZE, 103),
            signature: Some(proto::transaction::Signature::EthSignature(
                test_eth_signature(),
            )),
            transaction: Some(proto::transaction::Transaction::Eip7702(
                proto::TransactionEip7702 {
                    chain_id: 1,
                    nonce: 20,
                    gas_limit: 200_000u64.to_le_bytes().to_vec(),
                    max_fee_per_gas: 60_000_000_000u128.to_le_bytes().to_vec(),
                    max_priority_fee_per_gas: 3_000_000_000u128.to_le_bytes().to_vec(),
                    to: dummy_bytes(ADDR_SIZE, 30),
                    value: U256::from(0u64).to_le_bytes_vec(),
                    access_list: test_access_list(),
                    authorization_list: vec![proto::AuthorizationListItem {
                        authorization: Some(proto::Authorization {
                            chain_id: U256::from(1u64).to_le_bytes_vec(),
                            address: dummy_bytes(ADDR_SIZE, 40),
                            nonce: 0,
                        }),
                        signature: Some(test_eth_signature()),
                    }],
                    input: vec![0xff],
                },
            )),
        };
        let domain: TempoTxEnvelope = (&original).try_into().unwrap();
        let roundtrip: proto::Transaction = (&domain).try_into().unwrap();
        assert_eq!(original, roundtrip);
    }

    #[test]
    fn test_tempo_tx_roundtrip() {
        let secp_sig = proto::PrimitiveSignature {
            signature: Some(proto::primitive_signature::Signature::Secp256k1(
                test_eth_signature(),
            )),
        };
        let tempo_sig = proto::TempoSignature {
            signature: Some(proto::tempo_signature::Signature::Primitive(
                secp_sig.clone(),
            )),
        };
        let original = proto::Transaction {
            hash: dummy_bytes(HASH_SIZE, 104),
            signature: Some(proto::transaction::Signature::TempoSignature(
                tempo_sig.clone(),
            )),
            transaction: Some(proto::transaction::Transaction::Tempo(
                proto::TransactionTempo {
                    chain_id: 1,
                    max_fee_per_gas: 50_000_000_000u128.to_le_bytes().to_vec(),
                    max_priority_fee_per_gas: 2_000_000_000u128.to_le_bytes().to_vec(),
                    gas_limit: 100_000,
                    calls: vec![proto::Call {
                        to: Some(test_tx_kind_call()),
                        value: U256::from(1000u64).to_le_bytes::<32>().to_vec(),
                        input: vec![0xde, 0xad],
                    }],
                    access_list: test_access_list(),
                    nonce_key: U256::from(0u64).to_le_bytes::<32>().to_vec(),
                    nonce: 1,
                    fee_token: Some(dummy_bytes(ADDR_SIZE, 80)),
                    fee_payer_signature: Some(test_eth_signature()),
                    valid_before: Some(2_000_000_000),
                    valid_after: Some(1_600_000_000),
                    key_authorization: Some(proto::KeyAuthorization {
                        chain_id: 1,
                        key_type: proto::SignatureType::Secp256k1 as i32,
                        key_id: dummy_bytes(ADDR_SIZE, 90),
                        expiry: Some(2_000_000_000),
                        limits: Some(proto::TokenLimits {
                            items: vec![proto::TokenLimit {
                                token: dummy_bytes(ADDR_SIZE, 91),
                                limit: U256::from(1_000_000u64).to_le_bytes::<32>().to_vec(),
                            }],
                        }),
                        signature: Some(secp_sig),
                    }),
                    aa_authorization_list: vec![proto::TempoAuthorization {
                        chain_id: U256::from(1u64).to_le_bytes_vec(),
                        address: dummy_bytes(ADDR_SIZE, 95),
                        nonce: 0,
                        signature: Some(tempo_sig),
                    }],
                },
            )),
        };
        let domain: TempoTxEnvelope = (&original).try_into().unwrap();
        let roundtrip: proto::Transaction = (&domain).try_into().unwrap();
        assert_eq!(original, roundtrip);
    }

    // ==================== Signature tests ====================

    #[test]
    fn test_secp256k1_signature_roundtrip() {
        let original = proto::PrimitiveSignature {
            signature: Some(proto::primitive_signature::Signature::Secp256k1(
                test_eth_signature(),
            )),
        };
        let domain: PrimitiveSignature = (&original).try_into().unwrap();
        let roundtrip: proto::PrimitiveSignature = (&domain).into();
        assert_eq!(original, roundtrip);
    }

    #[test]
    fn test_p256_signature_roundtrip() {
        let original = proto::PrimitiveSignature {
            signature: Some(proto::primitive_signature::Signature::P256(
                proto::P256SignatureWithPreHash {
                    r: dummy_bytes(HASH_SIZE, 1),
                    s: dummy_bytes(HASH_SIZE, 2),
                    pub_key_x: dummy_bytes(HASH_SIZE, 3),
                    pub_key_y: dummy_bytes(HASH_SIZE, 4),
                    pre_hash: true,
                },
            )),
        };
        let domain: PrimitiveSignature = (&original).try_into().unwrap();
        let roundtrip: proto::PrimitiveSignature = (&domain).into();
        assert_eq!(original, roundtrip);
    }

    #[test]
    fn test_webauthn_signature_roundtrip() {
        let original = proto::PrimitiveSignature {
            signature: Some(proto::primitive_signature::Signature::Webauthn(
                proto::WebAuthnSignature {
                    r: dummy_bytes(HASH_SIZE, 10),
                    s: dummy_bytes(HASH_SIZE, 20),
                    pub_key_x: dummy_bytes(HASH_SIZE, 30),
                    pub_key_y: dummy_bytes(HASH_SIZE, 40),
                    webauthn_data: vec![0x01, 0x02, 0x03, 0x04],
                },
            )),
        };
        let domain: PrimitiveSignature = (&original).try_into().unwrap();
        let roundtrip: proto::PrimitiveSignature = (&domain).into();
        assert_eq!(original, roundtrip);
    }

    #[test]
    fn test_tempo_signature_primitive_roundtrip() {
        let original = proto::TempoSignature {
            signature: Some(proto::tempo_signature::Signature::Primitive(
                proto::PrimitiveSignature {
                    signature: Some(proto::primitive_signature::Signature::Secp256k1(
                        test_eth_signature(),
                    )),
                },
            )),
        };
        let domain: TempoSignature = (&original).try_into().unwrap();
        let roundtrip: proto::TempoSignature = (&domain).into();
        assert_eq!(original, roundtrip);
    }

    #[test]
    fn test_tempo_signature_keychain_v1_roundtrip() {
        let original = proto::TempoSignature {
            signature: Some(proto::tempo_signature::Signature::Keychain(
                proto::KeychainSignature {
                    user_address: dummy_bytes(ADDR_SIZE, 1),
                    signature: Some(proto::PrimitiveSignature {
                        signature: Some(proto::primitive_signature::Signature::Secp256k1(
                            test_eth_signature(),
                        )),
                    }),
                    version: proto::KeychainVersion::V1 as i32,
                },
            )),
        };
        let domain: TempoSignature = (&original).try_into().unwrap();
        let roundtrip: proto::TempoSignature = (&domain).into();
        assert_eq!(original, roundtrip);
    }

    #[test]
    fn test_tempo_signature_keychain_v2_roundtrip() {
        let original = proto::TempoSignature {
            signature: Some(proto::tempo_signature::Signature::Keychain(
                proto::KeychainSignature {
                    user_address: dummy_bytes(ADDR_SIZE, 1),
                    signature: Some(proto::PrimitiveSignature {
                        signature: Some(proto::primitive_signature::Signature::Secp256k1(
                            test_eth_signature(),
                        )),
                    }),
                    version: proto::KeychainVersion::V2 as i32,
                },
            )),
        };
        let domain: TempoSignature = (&original).try_into().unwrap();
        let roundtrip: proto::TempoSignature = (&domain).into();
        assert_eq!(original, roundtrip);
    }

    // ==================== Receipt tests ====================

    #[test]
    fn test_receipt_roundtrip() {
        let original = proto::NonEmptyReceipt {
            tx_type: proto::TxType::Eip1559 as i32,
            success: true,
            cumulative_gas_used: 21_000,
            logs: vec![proto::Log {
                address: dummy_bytes(ADDR_SIZE, 1),
                data: Some(proto::LogData {
                    topics: vec![dummy_bytes(HASH_SIZE, 10), dummy_bytes(HASH_SIZE, 20)],
                    data: vec![0xaa, 0xbb, 0xcc],
                }),
            }],
        };
        let domain: tempo_primitives::TempoReceipt = (&original).try_into().unwrap();
        let roundtrip: proto::NonEmptyReceipt = (&domain).try_into().unwrap();
        assert_eq!(original, roundtrip);
    }

    #[test]
    fn test_receipt_all_tx_types() {
        for (proto_type, expected_back) in [
            (proto::TxType::Legacy, proto::TxType::Legacy),
            (proto::TxType::Eip2930, proto::TxType::Eip2930),
            (proto::TxType::Eip1559, proto::TxType::Eip1559),
            (proto::TxType::Eip7702, proto::TxType::Eip7702),
            (proto::TxType::Tempo, proto::TxType::Tempo),
        ] {
            let original = proto::NonEmptyReceipt {
                tx_type: proto_type as i32,
                success: true,
                cumulative_gas_used: 0,
                logs: vec![],
            };
            let domain: tempo_primitives::TempoReceipt = (&original).try_into().unwrap();
            let roundtrip: proto::NonEmptyReceipt = (&domain).try_into().unwrap();
            assert_eq!(roundtrip.tx_type, expected_back as i32);
        }
    }

    // ==================== Supporting type tests ====================

    #[test]
    fn test_tx_kind_create_roundtrip() {
        let original = proto::TxKind {
            kind: Some(proto::tx_kind::Kind::Create(())),
        };
        let domain: alloy_primitives::TxKind = (&original).try_into().unwrap();
        let roundtrip: proto::TxKind = (&domain).into();
        assert_eq!(original, roundtrip);
    }

    #[test]
    fn test_tx_kind_call_roundtrip() {
        let original = test_tx_kind_call();
        let domain: alloy_primitives::TxKind = (&original).try_into().unwrap();
        let roundtrip: proto::TxKind = (&domain).into();
        assert_eq!(original, roundtrip);
    }

    #[test]
    fn test_access_list_item_roundtrip() {
        let original = proto::AccessListItem {
            address: dummy_bytes(ADDR_SIZE, 1),
            storage_keys: vec![dummy_bytes(HASH_SIZE, 2), dummy_bytes(HASH_SIZE, 3)],
        };
        let domain: alloy_eips::eip2930::AccessListItem = (&original).try_into().unwrap();
        let roundtrip: proto::AccessListItem = (&domain).into();
        assert_eq!(original, roundtrip);
    }

    #[test]
    fn test_account_info_roundtrip() {
        let original = proto::AccountInfo {
            balance: U256::from(1_000_000u64).to_le_bytes_vec(),
            nonce: 5,
            code_hash: dummy_bytes(HASH_SIZE, 1),
            code: None,
        };
        let domain: reth::revm::state::AccountInfo = (&original).try_into().unwrap();
        let roundtrip: proto::AccountInfo = (&domain).try_into().unwrap();
        assert_eq!(original, roundtrip);
    }

    #[test]
    fn test_account_status_roundtrip() {
        let statuses = [
            proto::AccountStatus::LoadedNotExisting,
            proto::AccountStatus::Loaded,
            proto::AccountStatus::LoadedEmptyEip161,
            proto::AccountStatus::InMemoryChange,
            proto::AccountStatus::Changed,
            proto::AccountStatus::Destroyed,
            proto::AccountStatus::DestroyedChanged,
            proto::AccountStatus::DestroyedAgain,
        ];
        for status in statuses {
            let domain: reth::revm::db::AccountStatus = status.into();
            let roundtrip = proto::AccountStatus::from(domain);
            assert_eq!(status, roundtrip);
        }
    }

    #[test]
    fn test_bundle_account_roundtrip() {
        let original = proto::BundleAccount {
            address: dummy_bytes(ADDR_SIZE, 1),
            info: Some(proto::AccountInfo {
                balance: U256::from(500u64).to_le_bytes_vec(),
                nonce: 1,
                code_hash: dummy_bytes(HASH_SIZE, 2),
                code: None,
            }),
            original_info: None,
            storage: vec![proto::StorageSlot {
                key: U256::from(0u64).to_le_bytes_vec(),
                previous_or_original_value: U256::from(0u64).to_le_bytes_vec(),
                present_value: U256::from(42u64).to_le_bytes_vec(),
            }],
            status: proto::AccountStatus::Changed as i32,
        };
        let (address, account): (Address, reth::revm::db::BundleAccount) =
            (&original).try_into().unwrap();
        let roundtrip: proto::BundleAccount = (address, &account).try_into().unwrap();
        assert_eq!(original, roundtrip);
    }

    #[test]
    fn test_revert_do_nothing_roundtrip() {
        let original = proto::Revert {
            address: dummy_bytes(ADDR_SIZE, 1),
            account: Some(proto::AccountInfoRevert {
                revert: Some(proto::account_info_revert::Revert::DoNothing(())),
            }),
            storage: vec![],
            previous_status: proto::AccountStatus::Loaded as i32,
            wipe_storage: false,
        };
        let (address, revert): (Address, reth::revm::db::states::reverts::AccountRevert) =
            (&original).try_into().unwrap();
        let roundtrip: proto::Revert = (address, &revert).try_into().unwrap();
        assert_eq!(original, roundtrip);
    }

    #[test]
    fn test_revert_delete_roundtrip() {
        let original = proto::Revert {
            address: dummy_bytes(ADDR_SIZE, 1),
            account: Some(proto::AccountInfoRevert {
                revert: Some(proto::account_info_revert::Revert::DeleteIt(())),
            }),
            storage: vec![proto::RevertToSlot {
                key: U256::from(1u64).to_le_bytes_vec(),
                revert: Some(proto::revert_to_slot::Revert::Destroyed(())),
            }],
            previous_status: proto::AccountStatus::Destroyed as i32,
            wipe_storage: true,
        };
        let (address, revert): (Address, reth::revm::db::states::reverts::AccountRevert) =
            (&original).try_into().unwrap();
        let roundtrip: proto::Revert = (address, &revert).try_into().unwrap();
        assert_eq!(original, roundtrip);
    }

    #[test]
    fn test_revert_to_account_roundtrip() {
        let original = proto::Revert {
            address: dummy_bytes(ADDR_SIZE, 1),
            account: Some(proto::AccountInfoRevert {
                revert: Some(proto::account_info_revert::Revert::RevertTo(
                    proto::AccountInfo {
                        balance: U256::from(100u64).to_le_bytes_vec(),
                        nonce: 0,
                        code_hash: dummy_bytes(HASH_SIZE, 5),
                        code: None,
                    },
                )),
            }),
            storage: vec![proto::RevertToSlot {
                key: U256::from(0u64).to_le_bytes_vec(),
                revert: Some(proto::revert_to_slot::Revert::Some(
                    U256::from(99u64).to_le_bytes_vec(),
                )),
            }],
            previous_status: proto::AccountStatus::Changed as i32,
            wipe_storage: false,
        };
        let (address, revert): (Address, reth::revm::db::states::reverts::AccountRevert) =
            (&original).try_into().unwrap();
        let roundtrip: proto::Revert = (address, &revert).try_into().unwrap();
        assert_eq!(original, roundtrip);
    }

    #[test]
    fn test_bytecode_eip7702_roundtrip() {
        let original = proto::Bytecode {
            bytecode: Some(proto::bytecode::Bytecode::Eip7702(proto::Eip7702Bytecode {
                delegated_address: dummy_bytes(ADDR_SIZE, 1),
                version: 0,
                raw: vec![0xef, 0x01, 0x00],
            })),
        };
        let domain: reth::revm::state::Bytecode = (&original).try_into().unwrap();
        let roundtrip: proto::Bytecode = (&domain).try_into().unwrap();
        assert_eq!(original, roundtrip);
    }

    // ==================== Error case tests ====================

    #[test]
    fn test_invalid_signature_y_parity() {
        let original = proto::Transaction {
            hash: dummy_bytes(HASH_SIZE, 100),
            signature: Some(proto::transaction::Signature::EthSignature(
                proto::Signature {
                    r: U256::from(1u64).to_le_bytes_vec(),
                    s: U256::from(2u64).to_le_bytes_vec(),
                    y_parity: 5,
                },
            )),
            transaction: Some(proto::transaction::Transaction::Legacy(
                proto::TransactionLegacy {
                    chain_id: Some(1),
                    nonce: 0,
                    gas_price: 1u128.to_le_bytes().to_vec(),
                    gas_limit: 21_000u64.to_le_bytes().to_vec(),
                    to: Some(test_tx_kind_call()),
                    value: U256::from(0u64).to_le_bytes_vec(),
                    input: vec![],
                },
            )),
        };
        let result: eyre::Result<TempoTxEnvelope> = (&original).try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_transaction() {
        let original = proto::Transaction {
            hash: dummy_bytes(HASH_SIZE, 100),
            signature: Some(proto::transaction::Signature::EthSignature(
                test_eth_signature(),
            )),
            transaction: None,
        };
        let result: eyre::Result<TempoTxEnvelope> = (&original).try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_wrong_signature_type_for_tempo_tx() {
        let original = proto::Transaction {
            hash: dummy_bytes(HASH_SIZE, 104),
            signature: Some(proto::transaction::Signature::EthSignature(
                test_eth_signature(),
            )),
            transaction: Some(proto::transaction::Transaction::Tempo(
                proto::TransactionTempo {
                    chain_id: 1,
                    max_fee_per_gas: 1u128.to_le_bytes().to_vec(),
                    max_priority_fee_per_gas: 1u128.to_le_bytes().to_vec(),
                    gas_limit: 21_000,
                    calls: vec![],
                    access_list: vec![],
                    nonce_key: U256::from(0u64).to_le_bytes::<32>().to_vec(),
                    nonce: 0,
                    fee_token: None,
                    fee_payer_signature: None,
                    valid_before: None,
                    valid_after: None,
                    key_authorization: None,
                    aa_authorization_list: vec![],
                },
            )),
        };
        let result: eyre::Result<TempoTxEnvelope> = (&original).try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_wrong_signature_type_for_eth_tx() {
        let tempo_sig = proto::TempoSignature {
            signature: Some(proto::tempo_signature::Signature::Primitive(
                proto::PrimitiveSignature {
                    signature: Some(proto::primitive_signature::Signature::Secp256k1(
                        test_eth_signature(),
                    )),
                },
            )),
        };
        let original = proto::Transaction {
            hash: dummy_bytes(HASH_SIZE, 100),
            signature: Some(proto::transaction::Signature::TempoSignature(tempo_sig)),
            transaction: Some(proto::transaction::Transaction::Legacy(
                proto::TransactionLegacy {
                    chain_id: Some(1),
                    nonce: 0,
                    gas_price: 1u128.to_le_bytes().to_vec(),
                    gas_limit: 21_000u64.to_le_bytes().to_vec(),
                    to: Some(test_tx_kind_call()),
                    value: U256::from(0u64).to_le_bytes_vec(),
                    input: vec![],
                },
            )),
        };
        let result: eyre::Result<TempoTxEnvelope> = (&original).try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_receipt_error() {
        let original = proto::Receipt {
            receipt: Some(proto::receipt::Receipt::Empty(())),
        };
        let result: eyre::Result<tempo_primitives::TempoReceipt> = (&original).try_into();
        assert!(result.is_err());
    }
}
