use reth_exex::ExExNotification;
use shared::{codec::block::chain_to_rpc_blocks, proto};
use tempo_primitives::TempoPrimitives;
use tokio::sync::{
    broadcast::{self, error::RecvError},
    mpsc,
};

use crate::streaming::error::StreamingError;

pub async fn live(
    sender: &mpsc::Sender<Result<proto::BlockChunk, tonic::Status>>,
    mut exex_notification_rx: broadcast::Receiver<ExExNotification<TempoPrimitives>>,
) -> Result<(), StreamingError> {
    loop {
        match exex_notification_rx.recv().await {
            Ok(notification) => {
                let blocks = process_exex_notification(&notification)?;
                sender.send(Ok(proto::BlockChunk { items: blocks })).await?;
            }
            Err(RecvError::Lagged(n)) => {
                return Err(StreamingError::BroadcastReceiverLagged(n));
            }
            Err(RecvError::Closed) => {
                return Ok(());
            }
        }
    }
}

fn process_exex_notification(
    notification: &ExExNotification<TempoPrimitives>,
) -> Result<Vec<proto::Block>, StreamingError> {
    let blocks = match notification {
        ExExNotification::ChainCommitted { new } => {
            chain_to_rpc_blocks(&new, proto::BlockStatus::Committed)
        }
        ExExNotification::ChainReorged { old, new } => {
            let reorged = chain_to_rpc_blocks(&old, proto::BlockStatus::Reorged);
            let committed = chain_to_rpc_blocks(&new, proto::BlockStatus::Committed);
            reorged.and_then(|reorged| Ok(reorged.into_iter().chain(committed?).collect()))
        }
        ExExNotification::ChainReverted { old } => {
            chain_to_rpc_blocks(&old, proto::BlockStatus::Reverted)
        }
    };

    blocks.map_err(Into::into)
}
