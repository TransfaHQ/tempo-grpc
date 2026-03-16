use std::array::TryFromSliceError;

use async_channel::{RecvError, SendError};
use shared::proto;
use thiserror::Error;
use tonic::Status;

#[derive(Debug, Error)]
pub enum IndexerError {
    #[error("failed to decode block")]
    Decode(#[from] TryFromSliceError),
    #[error("failed to write row to clickhouse")]
    Clickhouse(#[from] clickhouse::error::Error),
    #[error("channel empty or closed")]
    Receiver(#[from] RecvError),
    #[error(transparent)]
    GRPC(#[from] Status),
    #[error(transparent)]
    Sender(#[from] SendError<Vec<proto::RpcBlock>>),
}
