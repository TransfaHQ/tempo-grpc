use std::{ops::RangeInclusive, sync::Arc, time::Instant};

use futures_util::{StreamExt as _, stream};
use reth::{
    api::FullNodeComponents,
    builder::{NodeAdapter, RethFullAdapter},
    providers::{BlockReader, ReceiptProvider},
};
use reth_ethereum::provider::db::DatabaseEnv;
use reth_exex::ExExNotification;
use reth_tracing::tracing::info;
use shared::{
    codec::block::chain_to_rpc_blocks,
    proto::{self, SubscribeRequest, block_stream_server::BlockStream},
};
use tempo_node::node::TempoNode;
use tempo_primitives::TempoPrimitives;
use tokio::sync::{
    broadcast::{self, error::RecvError},
    mpsc,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

type TempoNodeAdapter = NodeAdapter<RethFullAdapter<Arc<DatabaseEnv>, TempoNode>>;

#[derive(Debug)]
pub struct BlockStreamService<N: FullNodeComponents> {
    inner: Arc<Inner<N>>,
}

impl<N: FullNodeComponents> BlockStreamService<N> {
    pub fn new(
        exex_notifications: Arc<broadcast::Sender<ExExNotification<TempoPrimitives>>>,
        provider: N::Provider,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                exex_notifications,
                provider,
            }),
        }
    }
}

#[derive(Debug)]
struct Inner<Node: FullNodeComponents> {
    pub exex_notifications: Arc<broadcast::Sender<ExExNotification<TempoPrimitives>>>,
    pub provider: Node::Provider,
}

impl Inner<TempoNodeAdapter> {
    fn fetch_block_range(&self, range: RangeInclusive<u64>) -> eyre::Result<Vec<proto::Block>> {
        let t = Instant::now();
        let blocks = self
            .provider
            .block_with_senders_range(range.clone().into())?;
        info!(target: "tempo_grpc", elapsed = ?t.elapsed(), "fetch_blocks");

        let t = Instant::now();
        let block_receipts = self.provider.receipts_by_block_range(range.into())?;
        info!(target: "tempo_grpc", elapsed = ?t.elapsed(), "fetch_receipts");

        let t = Instant::now();
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
            .collect::<eyre::Result<Vec<_>>>()?;
        info!(target: "tempo_grpc", elapsed = ?t.elapsed(), count = rpc_blocks.len(), "encode_blocks");

        Ok(rpc_blocks)
    }
}

#[tonic::async_trait]
impl BlockStream for BlockStreamService<TempoNodeAdapter> {
    type SubscribeStream = ReceiverStream<Result<proto::Block, Status>>;
    type BackfillStream = ReceiverStream<Result<proto::BlockChunk, Status>>;

    async fn subscribe(
        &self,
        _request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let (tx, rx) = mpsc::channel(32);
        let mut receiver = self.inner.exex_notifications.subscribe();

        tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Ok(notification) => {
                        let blocks = match notification {
                            ExExNotification::ChainCommitted { new } => {
                                chain_to_rpc_blocks(&new, proto::BlockStatus::Committed)
                            }
                            ExExNotification::ChainReorged { old, new } => {
                                let reorged =
                                    chain_to_rpc_blocks(&old, proto::BlockStatus::Reorged);
                                let committed =
                                    chain_to_rpc_blocks(&new, proto::BlockStatus::Committed);
                                reorged.and_then(|reorged| {
                                    Ok(reorged.into_iter().chain(committed?).collect())
                                })
                            }
                            ExExNotification::ChainReverted { old } => {
                                chain_to_rpc_blocks(&old, proto::BlockStatus::Reverted)
                            }
                        };
                        let blocks = match blocks {
                            Ok(b) => b,
                            Err(e) => {
                                let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                                return;
                            }
                        };

                        for block in blocks {
                            if tx.send(Ok(block)).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(RecvError::Lagged(n)) => {
                        let status = Status::data_loss(format!(
                            "consumer lagged by {n} messages, reconnect to subscribe"
                        ));
                        let _ = tx.send(Err(status)).await;
                        return;
                    }
                    Err(RecvError::Closed) => return,
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn backfill(
        &self,
        request: Request<proto::BackfillRequest>,
    ) -> Result<Response<Self::BackfillStream>, Status> {
        let message = request.into_inner();
        if message.from > message.to {
            return Err(Status::invalid_argument(
                "invalid range: from must be <= to",
            ));
        }
        info!(target: "tempo_grpc", from = message.from, to = message.to, "Backfill requested");
        let (tx, rx) = mpsc::channel(128);
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            let e = Instant::now();
            let chunks: Vec<_> = (message.from..=message.to)
                .step_by(message.size as usize)
                .map(|start| (start, (start + message.size - 1).min(message.to)))
                .collect();

            let mut stream = stream::iter(chunks)
                .map(|(start, end)| {
                    let inner = Arc::clone(&inner);
                    tokio::task::spawn_blocking(move || inner.fetch_block_range(start..=end))
                })
                .buffered(16);

            while let Some(result) = stream.next().await {
                let result = result
                    .map_err(|e| Status::internal(e.to_string()))
                    .and_then(|r| r.map_err(|e| Status::internal(e.to_string())));

                let blocks = match result {
                    Ok(b) => b,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                let count = blocks.len();
                let t = Instant::now();
                if tx
                    .send(Ok(proto::BlockChunk { items: blocks }))
                    .await
                    .is_err()
                {
                    info!(target: "tempo_grpc", "Client disconnected during streaming");
                    return;
                }
                info!(target: "tempo_grpc", elapsed = ?t.elapsed(), count, "streamed chunk");
            }
            info!(target: "tempo_grpc", elapsed = ?e.elapsed(), "Backfill completed");
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
