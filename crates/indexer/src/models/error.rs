use std::array::TryFromSliceError;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error(transparent)]
    Slice(#[from] TryFromSliceError),
    #[error("missing field: {0}")]
    MissingField(&'static str),
}
