use std::sync::Arc;

use futures_util::TryStreamExt;
use reth::api::FullNodeComponents;
use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use reth_tracing::tracing::info;
use tempo_evm::TempoEvmConfig;
use tempo_node::node::TempoNode;
use tempo_primitives::TempoPrimitives;
use tokio::sync::broadcast;

pub struct ExEx<Node: FullNodeComponents> {
    pub ctx: ExExContext<Node>,
    pub notifications_tx: Arc<broadcast::Sender<ExExNotification<TempoPrimitives>>>,
}

impl<Node> ExEx<Node>
where
    Node: FullNodeComponents<Types = TempoNode, Evm = TempoEvmConfig>,
{
    pub async fn start(mut self) -> eyre::Result<()> {
        while let Some(notification) = self.ctx.notifications.try_next().await? {
            match &notification {
                ExExNotification::ChainCommitted { new } => {
                    info!(committed_chain = ?new.range(), "Committed chain");
                }
                ExExNotification::ChainReorged { old, new } => {
                    info!(old_chain = ?old.range(), new_chain = ?new.range(), "Reorged chain received");
                }
                ExExNotification::ChainReverted { old } => {
                    info!(reverted_chain = ?old.range(), "Reverted chain");
                }
            };

            if let Some(committed_chain) = &notification.committed_chain() {
                self.ctx
                    .events
                    .send(ExExEvent::FinishedHeight(committed_chain.tip().num_hash()))?;
            };
            self.notifications_tx.send(notification)?;
        }
        Ok(())
    }
}
