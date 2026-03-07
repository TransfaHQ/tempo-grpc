use std::sync::Arc;

use crate::server::proto::{SubscribeRequest, remote_ex_ex_server::RemoteExEx};
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
