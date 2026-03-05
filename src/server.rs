use reth_exex::ExExNotification;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use crate::server::proto::{ SubscribeRequest, remote_ex_ex_server::RemoteExEx};



pub mod proto {
    tonic::include_proto!("exex");
}

#[derive(Debug)]
pub struct RemoteExExService {
    pub exex_notifications: broadcast::Sender<ExExNotification>
}

#[tonic::async_trait]
impl RemoteExEx for RemoteExExService {
    type SubscribeStream = ReceiverStream<Result<proto::ExExNotification, Status>>;
    
    async fn subscribe(&self, request: Request<SubscribeRequest>) -> Result<Response<Self::SubscribeStream>, Status> {
        let (tx, rx) = mpsc::channel(1);
        let mut receiver = self.exex_notifications.subscribe();

        tokio::spawn(async move {
            while let Ok(notification) = receiver.recv().await {

                tx.send(Ok(proto::ExExNotification { notification: Ok(notification) }));
            }
        });
            
        Ok(Response::new(ReceiverStream::new(rx)))  
    }
}
