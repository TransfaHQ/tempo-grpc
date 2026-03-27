use reth_exex::ExExNotification;
use shared::{codec::block::chain_to_rpc_blocks, proto};
use tempo_primitives::TempoPrimitives;
use tokio::sync::{
    broadcast::{self, error::RecvError},
    mpsc,
};

use crate::streaming::error::LiveError;

pub async fn live(
    sender: &mpsc::Sender<Result<proto::BlockChunk, tonic::Status>>,
    mut exex_notification_rx: broadcast::Receiver<ExExNotification<TempoPrimitives>>,
) -> Result<(), LiveError> {
    loop {
        match exex_notification_rx.recv().await {
            Ok(notification) => {
                let blocks = process_exex_notification(&notification)?;
                sender.send(Ok(proto::BlockChunk { items: blocks })).await?;
            }
            Err(RecvError::Lagged(n)) => {
                return Err(LiveError::BroadcastReceiverLagged(n));
            }
            Err(RecvError::Closed) => {
                return Ok(());
            }
        }
    }
}

pub(super) fn process_exex_notification(
    notification: &ExExNotification<TempoPrimitives>,
) -> Result<Vec<proto::Block>, LiveError> {
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
            chain_to_rpc_blocks(&old, proto::BlockStatus::Reorged)
        }
    };

    blocks.map_err(Into::into)
}
