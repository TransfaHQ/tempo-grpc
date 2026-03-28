use std::time::Instant;

use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::pipeline::{Indexer, IndexerArgs};

mod error;
mod models;
mod pipeline;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let args = IndexerArgs::parse();
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber)?;
    let start = Instant::now();
    let cancel_token = CancellationToken::new();
    let mut indexer = Indexer::new(args, cancel_token.child_token());

    tokio::select! {
        res = indexer.start() => {
            if let Err(err) = res {
                error!("Indexing pipeline failed: {:?}", err);
            } else {
                info!("Done in {}", start.elapsed().as_millis());
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Shutting down...");
            cancel_token.cancel();
        }
    }

    Ok(())
}
