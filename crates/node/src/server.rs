use std::sync::Arc;

use reth::{
    api::FullNodeComponents,
    builder::{NodeAdapter, RethFullAdapter},
};
use reth_ethereum::provider::db::DatabaseEnv;
use reth_exex::ExExNotification;
use shared::proto::{self, SubscribeRequest, block_stream_server::BlockStream};
use tempo_node::node::TempoNode;
use tempo_primitives::TempoPrimitives;
use tokio::sync::{
    broadcast::{self},
    mpsc,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::streaming;

type TempoNodeAdapter = NodeAdapter<RethFullAdapter<DatabaseEnv, TempoNode>>;

#[derive(Debug)]
pub struct BlockStreamService<N: FullNodeComponents> {
    pub exex_notifications: Arc<broadcast::Sender<ExExNotification<TempoPrimitives>>>,
    pub provider: Arc<N::Provider>,
}

impl<N: FullNodeComponents> BlockStreamService<N> {
    pub fn new(
        exex_notifications: Arc<broadcast::Sender<ExExNotification<TempoPrimitives>>>,
        provider: Arc<N::Provider>,
    ) -> Self {
        Self {
            exex_notifications,
            provider,
        }
    }
}

#[tonic::async_trait]
impl BlockStream for BlockStreamService<TempoNodeAdapter> {
    type SubscribeStream = ReceiverStream<Result<proto::BlockChunk, Status>>;
    type BackfillStream = ReceiverStream<Result<proto::BlockChunk, Status>>;
    type BackfillToLiveStream = ReceiverStream<Result<proto::BlockChunk, Status>>;

    async fn subscribe(
        &self,
        _request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let (tx, rx) = mpsc::channel(128);
        let exex_receiver = self.exex_notifications.subscribe();

        tokio::spawn(async move {
            if let Err(e) = streaming::live(&tx, exex_receiver).await {
                let _ = tx.send(Err(e.into())).await;
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn backfill(
        &self,
        request: Request<proto::BackfillRequest>,
    ) -> Result<Response<Self::BackfillStream>, Status> {
        let message = request.into_inner();
        let (tx, rx) = mpsc::channel(128);
        let provider = Arc::clone(&self.provider);
        tokio::spawn(async move {
            if let Err(e) = streaming::backfill(&tx, message, &provider).await {
                let _ = tx.send(Err(e.into())).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn backfill_to_live(
        &self,
        request: Request<proto::BackfillToLiveRequest>,
    ) -> Result<Response<Self::BackfillToLiveStream>, Status> {
        let message = request.into_inner();
        let (tx, rx) = mpsc::channel(128);
        let exex_receiver = self.exex_notifications.subscribe();

        let provider = Arc::clone(&self.provider);
        tokio::spawn(async move {
            if let Err(e) =
                streaming::backfill_to_live(&tx, message, exex_receiver, &provider).await
            {
                let _ = tx.send(Err(e.into())).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
