use std::sync::Arc;

use crate::{
    codec::block::chain_to_rpc_blocks,
    server::proto::{
        SubscribeRequest, block_stream_server::BlockStream, remote_ex_ex_server::RemoteExEx,
    },
};
use reth_exex::ExExNotification;
use tempo_primitives::TempoPrimitives;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

pub mod proto {
    tonic::include_proto!("exex");
}

#[derive(Debug)]
pub struct RemoteExExService {
    pub exex_notifications: Arc<broadcast::Sender<ExExNotification<TempoPrimitives>>>,
}

#[tonic::async_trait]
impl RemoteExEx for RemoteExExService {
    type SubscribeStream = ReceiverStream<Result<proto::ExExNotification, Status>>;

    async fn subscribe(
        &self,
        _request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let (tx, rx) = mpsc::channel(1);
        let mut receiver = self.exex_notifications.subscribe();

        tokio::spawn(async move {
            while let Ok(notification) = receiver.recv().await {
                tx.send(Ok((&notification).try_into().expect("failed to encode")))
                    .await
                    .expect("failed to send notification to client");
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[derive(Debug)]
pub struct BlockStreamService {
    pub exex_notifications: Arc<broadcast::Sender<ExExNotification<TempoPrimitives>>>,
}

#[tonic::async_trait]
impl BlockStream for BlockStreamService {
    type SubscribeStream = ReceiverStream<Result<proto::RpcBlock, Status>>;
    async fn subscribe(
        &self,
        _request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let (tx, rx) = mpsc::channel(1);
        let mut receiver = self.exex_notifications.subscribe();

        tokio::spawn(async move {
            while let Ok(notification) = receiver.recv().await {
                let blocks = match notification {
                    ExExNotification::ChainCommitted { new } => {
                        chain_to_rpc_blocks(&new, proto::BlockStatus::Committed)
                            .expect("failed to encode")
                    }
                    ExExNotification::ChainReorged { old, new } => {
                        let reorged = chain_to_rpc_blocks(&old, proto::BlockStatus::Reorged)
                            .expect("failed to encode");
                        let committed = chain_to_rpc_blocks(&new, proto::BlockStatus::Committed)
                            .expect("failed to encode");
                        reorged.into_iter().chain(committed).collect()
                    }
                    ExExNotification::ChainReverted { old } => {
                        chain_to_rpc_blocks(&old, proto::BlockStatus::Reverted)
                            .expect("failed to encode")
                    }
                };
                for block in blocks {
                    tx.send(Ok(block))
                        .await
                        .expect("failed to send notification to client");
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
