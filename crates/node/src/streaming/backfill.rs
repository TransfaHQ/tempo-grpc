use std::{ops::RangeInclusive, sync::Arc};

use futures_util::{StreamExt as _, stream};
use reth::providers::BlockReader;
use reth::providers::ReceiptProvider;
use reth::{api::NodeTypesWithDBAdapter, providers::providers::BlockchainProvider};
use reth_ethereum::provider::db::DatabaseEnv;
use reth_exex::ExExNotification;
use shared::proto;
use tempo_node::node::TempoNode;
use tempo_primitives::TempoPrimitives;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;

use crate::streaming::error::BackfillError;

pub type TempoRethProvider =
    BlockchainProvider<NodeTypesWithDBAdapter<TempoNode, Arc<DatabaseEnv>>>;

pub async fn backfill(
    sender: &mpsc::Sender<proto::BlockChunk>,
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
            let provider = Arc::clone(&provider);
            tokio::task::spawn_blocking(move || fetch_block_range(&provider, start..=end))
        })
        .buffered(16);

    while let Some(result) = stream.next().await {
        let blocks = result??;
        sender.send(proto::BlockChunk { items: blocks }).await?;
    }
    Ok(())
}

pub async fn backfill_to_live(
    sender: &mpsc::Sender<proto::BlockChunk>,
    request: proto::BackfillToLiveRequest,
    mut exex_notification_rx: broadcast::Receiver<ExExNotification<TempoPrimitives>>,
    provider: &Arc<TempoRethProvider>,
) -> Result<(), BackfillError> {
    let mut from = request.from;
    loop {
        match exex_notification_rx.recv().await {
            Ok(notification) => {
                let Some(chain) = notification.committed_chain() else {
                    continue;
                };
                let tip = chain.range().end().clone();
                if tip < from {
                    continue;
                }
                let request = proto::BackfillRequest {
                    from: from,
                    to: tip,
                    size: request.size,
                };
                backfill(&sender, request, provider).await?;
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
    let blocks = provider.block_with_senders_range(range.clone().into())?;

    let block_receipts = provider.receipts_by_block_range(range.into())?;

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
