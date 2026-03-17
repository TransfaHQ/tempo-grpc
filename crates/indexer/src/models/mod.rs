pub mod block;
pub mod error;
pub mod log;
pub mod receipt;
pub mod transaction;
pub use block::BlockRow;
pub use transaction::TransactionRow;

pub type Hash = [u8; 32];
pub type Address = [u8; 20];
