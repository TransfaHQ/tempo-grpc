use reth::providers::Chain;
use reth_exex::ExExNotification;
use std::sync::Arc;
use tempo_primitives::TempoPrimitives;
use tokio::sync::broadcast::Receiver as BroadcastReceiver;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;

pub enum NewChainNotification {
    Committed {
        new: Arc<Chain<TempoPrimitives>>,
    },
    Reorged {
        old: Arc<Chain<TempoPrimitives>>,
        new: Arc<Chain<TempoPrimitives>>,
    },
}

impl NewChainNotification {
    pub fn new_chain(&self) -> &Arc<Chain<TempoPrimitives>> {
        match self {
            NewChainNotification::Committed { new } => new,
            NewChainNotification::Reorged { old: _, new } => new,
        }
    }
    /// Spawns a task that converts [`ExExNotification`]s into [`ChainNotification`]s.
    ///
    /// Returns a receiver that yields committed and reorged chain notifications.
    /// Reverted notifications are silently dropped.
    ///
    /// The internal channel has a buffer of 1, which means this naturally
    /// applies backpressure: if the consumer hasn't processed the previous
    /// notification, incoming ExEx notifications are skipped until the
    /// channel is ready. This guarantees the consumer always sees the
    /// latest state rather than queuing up stale intermediate notifications.
    ///
    /// If the broadcast sender lags (i.e., notifications arrive faster than
    /// they can be forwarded), lagged notifications are skipped and the task
    /// resumes from the next available one. The task exits when either the
    /// broadcast sender or the returned receiver is dropped.
    pub fn emit_from(
        mut exex_notification_rx: BroadcastReceiver<ExExNotification<TempoPrimitives>>,
    ) -> mpsc::Receiver<Self> {
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(async move {
            loop {
                match exex_notification_rx.recv().await {
                    Ok(notification) => {
                        let chain_notification = match notification {
                            ExExNotification::ChainCommitted { new } => {
                                Some(NewChainNotification::Committed { new })
                            }
                            ExExNotification::ChainReorged { old, new } => {
                                Some(NewChainNotification::Reorged { old, new })
                            }
                            ExExNotification::ChainReverted { .. } => None,
                        };
                        if let Some(chain_notification) = chain_notification {
                            if let Err(_) = tx.send(chain_notification).await {
                                return;
                            }
                        }
                    }
                    Err(RecvError::Lagged(_)) => continue,
                    Err(RecvError::Closed) => return,
                }
            }
        });
        rx
    }
}
