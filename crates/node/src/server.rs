use std::{ops::RangeInclusive, sync::Arc, time::Instant};

use eyre::{Context, eyre};
use reth::{
    api::FullNodeComponents,
    builder::{NodeAdapter, RethFullAdapter},
    providers::{BlockReader, ReceiptProvider, TransactionVariant},
};
use reth_ethereum::provider::db::DatabaseEnv;
use reth_exex::{BackfillJobFactory, ExExNotification};
use reth_tracing::tracing::info;
use shared::{
    codec::block::chain_to_rpc_blocks,
    proto::{
        self, SubscribeRequest, block_stream_server::BlockStream, remote_ex_ex_server::RemoteExEx,
    },
};
use tempo_evm::TempoEvmConfig;
use tempo_node::node::TempoNode;
use tempo_primitives::TempoPrimitives;
use tokio::sync::{
    broadcast::{self, error::RecvError},
    mpsc,
};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};

type TempoNodeAdapter = NodeAdapter<RethFullAdapter<Arc<DatabaseEnv>, TempoNode>>;

#[derive(Debug)]
pub struct RemoteExExService<Node: FullNodeComponents> {
    pub exex_notifications: Arc<broadcast::Sender<ExExNotification<TempoPrimitives>>>,
    pub backfill_job_factory: BackfillJobFactory<TempoEvmConfig, Node::Provider>,
}

#[tonic::async_trait]
impl RemoteExEx for RemoteExExService<TempoNodeAdapter> {
    type SubscribeStream = ReceiverStream<Result<proto::ExExNotification, Status>>;
    type BackfillStream = ReceiverStream<Result<proto::Chain, Status>>;

    async fn subscribe(
        &self,
        _request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let (tx, rx) = mpsc::channel(32);
        let mut receiver = self.exex_notifications.subscribe();

        tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Ok(notification) => {
                        let msg = (&notification)
                            .try_into()
                            .map_err(|e: eyre::Error| Status::internal(e.to_string()));
                        if tx.send(msg).await.is_err() {
                            break;
                        }
                    }
                    Err(RecvError::Lagged(n)) => {
                        let status = Status::data_loss(format!(
                            "consumer lagged by {n} messages, reconnect to subscribe"
                        ));
                        let _ = tx.send(Err(status)).await;
                        break;
                    }
                    Err(RecvError::Closed) => break,
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
        let (tx, rx) = mpsc::channel(32);
        let job = self
            .backfill_job_factory
            .backfill(message.from..=message.to);
        let mut stream = job.into_stream();
        tokio::spawn(async move {
            while let Some(chain) = stream.next().await {
                let chain = match chain {
                    Ok(chain) => (&chain)
                        .try_into()
                        .map_err(|e: eyre::Error| Status::internal(e.to_string())),
                    Err(e) => Err(Status::internal(e.to_string())),
                };
                if tx.send(chain).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[derive(Debug)]
pub struct BlockStreamService<N: FullNodeComponents> {
    inner: Arc<Inner<N>>,
}

impl<N: FullNodeComponents> BlockStreamService<N> {
    pub fn new(
        exex_notifications: Arc<broadcast::Sender<ExExNotification<TempoPrimitives>>>,
        backfill_job_factory: BackfillJobFactory<TempoEvmConfig, N::Provider>,
        provider: N::Provider,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                exex_notifications,
                backfill_job_factory,
                provider,
            }),
        }
    }
}

#[derive(Debug)]
struct Inner<Node: FullNodeComponents> {
    pub exex_notifications: Arc<broadcast::Sender<ExExNotification<TempoPrimitives>>>,
    pub backfill_job_factory: BackfillJobFactory<TempoEvmConfig, Node::Provider>,
    pub provider: Node::Provider,
}

impl Inner<TempoNodeAdapter> {
    fn fetch_block(&self, block_number: u64) -> eyre::Result<proto::RpcBlock> {
        let block = self
            .provider
            .recovered_block(block_number.into(), TransactionVariant::WithHash)?;
        let receipts = self.provider.receipts_by_block(block_number.into())?;
        if let Some(block) = block
            && let Some(receipts) = receipts
        {
            proto::RpcBlock::try_from_blocks_and_receipts(
                &block,
                &receipts,
                proto::BlockStatus::Committed,
            )
        } else {
            Err(eyre!("Block not found: {}", block_number))
        }
    }

    fn fetch_block_range(&self, range: RangeInclusive<u64>) -> eyre::Result<Vec<proto::RpcBlock>> {
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
                proto::RpcBlock::try_from_blocks_and_receipts(
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
    type SubscribeStream = ReceiverStream<Result<proto::RpcBlock, Status>>;
    type BackfillStream = ReceiverStream<Result<proto::RpcBlock, Status>>;

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
        let (tx, rx) = mpsc::channel(32);
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            let inner = Arc::clone(&inner);
            let blocks = tokio::task::spawn_blocking(move || {
                inner.fetch_block_range(message.from..=message.to)
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))
            .and_then(|r| r.map_err(|e| Status::internal(e.to_string())));
            let blocks = match blocks {
                Ok(b) => b,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };
            let count = blocks.len();
            let t = Instant::now();
            for block in blocks {
                if tx.send(Ok(block)).await.is_err() {
                    info!(target: "tempo_grpc", "Client disconnected during streaming");
                    return;
                }
            }
            info!(target: "tempo_grpc", elapsed = ?t.elapsed(), count, "Backfill complete");
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
