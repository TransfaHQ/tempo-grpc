use async_channel::{RecvError, SendError};
use shared::proto;
use thiserror::Error;
use tonic::Status;

use crate::models::error::ParseError;

#[derive(Debug, Error)]
pub enum IndexerError {
    #[error(transparent)]
    Decode(#[from] ParseError),
    #[error("failed to write row to clickhouse")]
    Clickhouse(#[from] clickhouse::error::Error),
    #[error("channel empty or closed")]
    Receiver(#[from] RecvError),
    #[error(transparent)]
    GRPC(#[from] Status),
    #[error(transparent)]
    Sender(#[from] SendError<Vec<proto::Block>>),
}
