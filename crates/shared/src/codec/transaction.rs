use crate::proto;
use tempo_primitives::{
    TempoSignature, TempoTxEnvelope,
    transaction::{KeychainVersion, PrimitiveSignature},
};

impl From<&tempo_primitives::TempoTxEnvelope> for proto::transaction_envelope::Transaction {
    fn from(transaction: &tempo_primitives::TempoTxEnvelope) -> Self {
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
                proto::transaction_envelope::Transaction::Legacy(proto::TransactionLegacy {
                    chain_id: *chain_id,
                    nonce: *nonce,
                    gas_price: gas_price.to_le_bytes().to_vec(),
                    gas_limit: gas_limit.to_le_bytes().to_vec(),
                    to: Some(to.into()),
                    value: value.to_le_bytes_vec(),
                    input: input.to_vec(),
                })
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
                proto::transaction_envelope::Transaction::Eip2930(proto::TransactionEip2930 {
                    chain_id: *chain_id,
                    nonce: *nonce,
                    gas_price: gas_price.to_le_bytes().to_vec(),
                    gas_limit: gas_limit.to_le_bytes().to_vec(),
                    to: Some(to.into()),
                    value: value.to_le_bytes_vec(),
                    access_list: access_list.iter().map(Into::into).collect(),
                    input: input.to_vec(),
                })
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
                proto::transaction_envelope::Transaction::Eip1559(proto::TransactionEip1559 {
                    chain_id: *chain_id,
                    nonce: *nonce,
                    gas_limit: gas_limit.to_le_bytes().to_vec(),
                    max_fee_per_gas: max_fee_per_gas.to_le_bytes().to_vec(),
                    max_priority_fee_per_gas: max_priority_fee_per_gas.to_le_bytes().to_vec(),
                    to: Some(to.into()),
                    value: value.to_le_bytes_vec(),
                    access_list: access_list.iter().map(Into::into).collect(),
                    input: input.to_vec(),
                })
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
                proto::transaction_envelope::Transaction::Eip7702(proto::TransactionEip7702 {
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
                })
            }
            TempoTxEnvelope::AA(signed) => {
                let tx = signed.tx();

                proto::transaction_envelope::Transaction::Tempo(proto::TransactionTempo {
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
