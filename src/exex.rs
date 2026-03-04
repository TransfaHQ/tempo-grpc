use futures_util::TryStreamExt;
use reth::api::FullNodeComponents;
use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use tempo_evm::TempoEvmConfig;
use tempo_node::node::TempoNode;
use reth_tracing::tracing::info;


pub struct ExEx<Node: FullNodeComponents> {
    ctx: ExExContext<Node>
}

impl<Node> ExEx<Node> 
    where 
        Node: FullNodeComponents<Types = TempoNode, Evm = TempoEvmConfig> 
{
    pub fn new(ctx: ExExContext<Node>) -> Self {
        Self {
            ctx
        }
    }

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

            if let Some(committed_chain) = notification.committed_chain() {
                self.ctx.events.send(ExExEvent::FinishedHeight(committed_chain.tip().num_hash()))?;
            }
        }
        Ok(())
    }
}
