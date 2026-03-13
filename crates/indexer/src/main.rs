use std::time::Instant;

use shared::proto::{BackfillRequest, block_stream_client::BlockStreamClient};
use tokio_stream::StreamExt;
use tonic::Request;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = BlockStreamClient::connect("https://grpc.moderato.trma.xyz")
        .await?
        .max_decoding_message_size(usize::MAX);
    let request = Request::new(BackfillRequest {
        from: 6000000,
        to: 6_001_000,
        size: 128,
    });
    let start = Instant::now();
    let response = client.backfill(request).await?;
    let mut stream = response.into_inner();
    while let Some(item) = stream.next().await {
        println!("Received block: {}", item?.items.len());
    }

    println!("Done in {}", start.elapsed().as_millis());
    Ok(())
}
