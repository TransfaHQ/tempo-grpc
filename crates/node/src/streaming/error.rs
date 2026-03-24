use reth::providers::ProviderError;
use shared::{error::CodecError, proto};
use thiserror::Error;
use tokio::{sync::mpsc::error::SendError, task::JoinError};
use tonic::Status;

#[derive(Error, Debug)]
pub enum StreamingError {
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error(transparent)]
    ChannelClosed(#[from] SendError<proto::BlockChunk>),
    #[error("receiver lagged by {0} messages")]
    BroadcastReceiverLagged(u64),
    #[error("receiver closed")]
    BroadcastReceiverClosed,
    #[error(transparent)]
    BackfillError(#[from] BackfillError),
}

impl From<StreamingError> for Status {
    fn from(value: StreamingError) -> Self {
        Self::internal(value.to_string())
    }
}

#[derive(Error, Debug)]
pub enum BackfillError {
    #[error("invalid range from: {from} must <= to: {to}")]
    InvalidRange { from: u64, to: u64 },
    #[error(transparent)]
    RethProviderError(#[from] ProviderError),
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error(transparent)]
    JoinError(#[from] JoinError),
    #[error(transparent)]
    ChannelClosed(#[from] SendError<proto::BlockChunk>),
}

impl From<BackfillError> for Status {
    fn from(value: BackfillError) -> Self {
        if let BackfillError::InvalidRange { .. } = value {
            Self::invalid_argument(value.to_string())
        } else {
            Self::internal(value.to_string())
        }
    }
}
