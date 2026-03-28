use std::{ops::RangeInclusive, sync::Arc};

use futures_util::{StreamExt as _, stream};
use reth::providers::BlockReader;
use reth::providers::ReceiptProvider;
use reth::{api::NodeTypesWithDBAdapter, providers::providers::BlockchainProvider};
use reth_ethereum::provider::db::DatabaseEnv;
use reth_exex::ExExNotification;
use shared::codec::block::chain_to_rpc_blocks;
use shared::proto;
use tempo_node::node::TempoNode;
use tempo_primitives::TempoPrimitives;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;

use crate::streaming::error::BackfillError;
use crate::streaming::error::BackfillToLiveError;
use crate::streaming::live;

pub type TempoRethProvider = BlockchainProvider<NodeTypesWithDBAdapter<TempoNode, DatabaseEnv>>;

pub async fn backfill(
    sender: &mpsc::Sender<Result<proto::BlockChunk, tonic::Status>>,
    request: proto::BackfillRequest,
    provider: &Arc<TempoRethProvider>,
) -> Result<(), BackfillError> {
    if request.from > request.to {
        return Err(BackfillError::InvalidRange {
            from: request.from,
            to: request.to,
        });
    }
    if request.size == 0 {
        return Err(BackfillError::InvalidBatchSize(request.size));
    }

    let chunks: Vec<_> = (request.from..=request.to)
        .step_by(request.size as usize)
        .map(|start| (start, (start + request.size - 1).min(request.to)))
        .collect();

    let mut stream = stream::iter(chunks)
        .map(|(start, end)| {
            let provider = Arc::clone(provider);
            tokio::task::spawn_blocking(move || fetch_block_range(&provider, start..=end))
        })
        .buffered(16);

    while let Some(result) = stream.next().await {
        let blocks = result??;
        sender.send(Ok(proto::BlockChunk { items: blocks })).await?;
    }
    Ok(())
}

pub async fn backfill_to_live(
    sender: &mpsc::Sender<Result<proto::BlockChunk, tonic::Status>>,
    request: proto::BackfillToLiveRequest,
    mut exex_notification_rx: broadcast::Receiver<ExExNotification<TempoPrimitives>>,
    provider: &Arc<TempoRethProvider>,
) -> Result<(), BackfillToLiveError> {
    let mut from = request.from;
    loop {
        match exex_notification_rx.recv().await {
            Ok(notification) => {
                if let Some(old) = notification.reverted_chain()
                    && *old.range().start() < from
                {
                    let blocks = chain_to_rpc_blocks(&old, proto::BlockStatus::Reorged)
                        .map_err(BackfillError::from)?;
                    let blocks: Vec<_> = blocks.into_iter().filter(|b| b.number < from).collect();

                    if !blocks.is_empty() {
                        sender
                            .send(Ok(proto::BlockChunk { items: blocks }))
                            .await
                            .map_err(BackfillError::from)?;
                    }

                    from = *old.range().start();
                }

                let Some(chain) = notification.committed_chain() else {
                    continue;
                };
                let start = *chain.range().start();
                if start == from {
                    let blocks = chain_to_rpc_blocks(&chain, proto::BlockStatus::Committed)
                        .map_err(BackfillError::from)?;

                    sender
                        .send(Ok(proto::BlockChunk { items: blocks }))
                        .await
                        .map_err(BackfillError::from)?;

                    live(sender, exex_notification_rx).await?;
                    return Ok(());
                }

                let tip = *chain.range().end();

                // Chain is behind the requested `from` block so we wait
                if tip < from {
                    continue;
                }

                let request = proto::BackfillRequest {
                    from,
                    to: tip,
                    size: request.size,
                };
                backfill(sender, request, provider).await?;
                from = tip + 1;
            }
            Err(RecvError::Lagged(_)) => continue,
            Err(RecvError::Closed) => return Ok(()),
        }
    }
}

fn fetch_block_range(
    provider: &Arc<TempoRethProvider>,
    range: RangeInclusive<u64>,
) -> Result<Vec<proto::Block>, BackfillError> {
    let blocks = provider.block_with_senders_range(range.clone())?;

    let block_receipts = provider.receipts_by_block_range(range)?;

    let rpc_blocks = blocks
        .iter()
        .zip(block_receipts)
        .map(|(block, receipts)| {
            proto::Block::try_from_blocks_and_receipts(
                block,
                &receipts,
                proto::BlockStatus::Committed,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(rpc_blocks)
}
