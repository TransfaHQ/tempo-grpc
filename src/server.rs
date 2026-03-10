use std::sync::Arc;

use crate::{
    codec::block::chain_to_rpc_blocks,
    server::proto::{
        SubscribeRequest, block_stream_server::BlockStream, remote_ex_ex_server::RemoteExEx,
    },
};
use reth::{
    api::FullNodeComponents,
    builder::{NodeAdapter, RethFullAdapter},
};
use reth_ethereum::provider::db::DatabaseEnv;
use reth_exex::{BackfillJobFactory, ExExNotification};
use tempo_evm::TempoEvmConfig;
use tempo_node::node::TempoNode;
use tempo_primitives::TempoPrimitives;
use tokio::sync::{
    broadcast::{self, error::RecvError},
    mpsc,
};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};

pub mod proto {
    tonic::include_proto!("exex");
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("exex_descriptor");
}

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
pub struct BlockStreamService<Node: FullNodeComponents> {
    pub exex_notifications: Arc<broadcast::Sender<ExExNotification<TempoPrimitives>>>,
    pub backfill_job_factory: BackfillJobFactory<TempoEvmConfig, Node::Provider>,
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
        let mut receiver = self.exex_notifications.subscribe();

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
        let (tx, rx) = mpsc::channel(32);
        let job = self
            .backfill_job_factory
            .backfill(message.from..=message.to);
        let mut stream = job.into_stream();
        tokio::spawn(async move {
            while let Some(chain) = stream.next().await {
                let chain = match chain {
                    Ok(chain) => chain_to_rpc_blocks(&chain, proto::BlockStatus::Committed)
                        .map_err(|e: eyre::Error| Status::internal(e.to_string())),
                    Err(e) => Err(Status::internal(e.to_string())),
                };
                let chain = match chain {
                    Ok(c) => c,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                for block in chain {
                    if tx.send(Ok(block)).await.is_err() {
                        return;
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
