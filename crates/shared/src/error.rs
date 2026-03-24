use alloy_consensus::crypto::RecoveryError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CodecError {
    #[error(transparent)]
    SignerRecovery(#[from] RecoveryError),
}
