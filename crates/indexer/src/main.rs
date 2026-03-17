use std::{sync::Arc, time::Instant};

use clap::Parser;
use clickhouse::Client;
use eyre::eyre;
use shared::proto::{BackfillRequest, block_stream_client::BlockStreamClient};
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tracing::{debug, error, info};

use crate::{
    error::IndexerError,
    models::{
        BlockRow, TransactionRow,
        error::ParseError,
        log::{LogRow, log_to_row},
        transaction::txn_to_row,
    },
};

mod error;
mod models;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    #[arg(long)]
    grpc_url: String,

    #[arg(long)]
    from: u64,

    #[arg(long)]
    to: u64,

    #[arg(long)]
    ch_url: String,

    #[arg(long)]
    ch_password: String,

    #[arg(long)]
    ch_database: String,

    #[arg(long)]
    ch_user: String,

    #[arg(long)]
    concurrency: u64,

    #[arg(long)]
    batch_size: u64,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let args = Args::parse();
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber)?;
    let mut client = BlockStreamClient::connect(args.grpc_url)
        .await?
        .max_decoding_message_size(usize::MAX);
    let request = Request::new(BackfillRequest {
        from: args.from,
        to: args.to,
        size: args.batch_size,
    });
    let start = Instant::now();
    let response = client.backfill(request).await?;
    let mut stream = response.into_inner();
    let cancel_token = CancellationToken::new();
    let (tx, rx) = async_channel::unbounded();
    let producer_cancel_token = cancel_token.child_token();
    let mut handles = JoinSet::<Result<(), IndexerError>>::new();
    handles.spawn(async move {
        while let Some(item) = stream.next().await {
            if producer_cancel_token.is_cancelled() {
                break;
            }
            let blocks = item?.items;
            info!("Produced batch");
            tx.send(blocks).await?;
        }
        Ok(())
    });
    let rx = Arc::new(rx);
    let client = Client::default()
        .with_url(args.ch_url)
        .with_password(args.ch_password)
        .with_user(args.ch_user)
        .with_database(args.ch_database);
    let client = Arc::new(client);

    for _ in 0..args.concurrency {
        let rx = Arc::clone(&rx);
        let client = Arc::clone(&client);
        let cancel_token = cancel_token.child_token();
        handles.spawn(async move {
            loop {
                if cancel_token.is_cancelled() {
                    return Ok(());
                }
                match rx.recv().await {
                    Ok(blocks) => {
                        info!("Processing batch");
                        let mut block_inserter = client.inserter::<BlockRow>("blocks");
                        let mut tx_inserter = client.inserter::<TransactionRow>("txs");
                        let mut log_inserter = client.inserter::<LogRow>("logs");
                        for block in blocks {
                            let row = (&block).try_into()?;
                            block_inserter.write(&row).await?;
                            let tx_rows = block
                                .transactions
                                .iter()
                                .map(|tx| txn_to_row(&block, tx))
                                .collect::<Result<Vec<_>, _>>()?;
                            for row in tx_rows {
                                tx_inserter.write(&row).await?;
                            }
                            let mut log_index = 0;
                            let mut logs = Vec::new();
                            for tx in &block.transactions {
                                let receipt = tx
                                    .receipt
                                    .as_ref()
                                    .ok_or(ParseError::MissingField("transaction"))?;
                                let inner = tx
                                    .transaction
                                    .as_ref()
                                    .ok_or(ParseError::MissingField("transaction.transaction"))?;
                                for log in &receipt.logs {
                                    logs.push((tx.index, &inner.hash, log));
                                }
                            }
                            for (tx_index, tx_hash, log) in logs {
                                let row = log_to_row(&log, log_index, &block, tx_index, tx_hash)?;
                                log_inserter.write(&row).await?;
                                log_index += 1;
                            }
                        }
                        tokio::try_join!(
                            block_inserter.end(),
                            tx_inserter.end(),
                            log_inserter.end()
                        )?;
                        info!("Batch processed");
                    }
                    Err(_) => {
                        debug!("channel closed");
                        return Ok(());
                    }
                }
            }
        });
    }

    let pipelines = async {
        while let Some(res) = handles.join_next().await {
            match res {
                Err(join_err) => {
                    return Err(eyre!("task join error: {}", join_err));
                }
                Ok(Err(indexer_err)) => {
                    return Err(eyre!("indexer error: {:?}", indexer_err));
                }
                Ok(Ok(())) => {}
            }
        }
        Ok(())
    };

    tokio::select! {
        res = pipelines => {
            if let Err(err) = res {
                error!("Indexing pipeline failed: {:#}", err);
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
