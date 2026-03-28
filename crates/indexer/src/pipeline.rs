use std::sync::Arc;

use async_channel::{Receiver, Sender};
use clap::Parser;
use clickhouse::{Client, inserter::Inserter};
use shared::proto::{
    BackfillRequest, BackfillToLiveRequest, Block, block_stream_client::BlockStreamClient,
};
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tracing::{debug, info};

use crate::{
    error::IndexerError,
    models::{
        BlockRow, TransactionRow,
        error::ParseError,
        log::{LogRow, log_to_row},
        transaction::txn_to_row,
    },
};

#[derive(Parser, Debug)]
#[command(version, about)]
pub struct IndexerArgs {
    #[arg(long)]
    grpc_url: String,

    #[arg(long)]
    from: u64,

    #[arg(long)]
    to: Option<u64>,

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
pub struct Indexer {
    args: IndexerArgs,
    shutdown_token: Arc<CancellationToken>,
    handles: JoinSet<Result<(), IndexerError>>,
    clickhouse: Arc<Client>,
}

impl Indexer {
    pub fn new(args: IndexerArgs, shutdown_token: CancellationToken) -> Self {
        let client = Client::default()
            .with_url(&args.ch_url)
            .with_password(&args.ch_password)
            .with_user(&args.ch_user)
            .with_database(&args.ch_database);
        Self {
            args,
            shutdown_token: Arc::new(shutdown_token),
            handles: JoinSet::new(),
            clickhouse: Arc::new(client),
        }
    }

    pub async fn start(&mut self) -> Result<(), IndexerError> {
        let (tx, rx) = async_channel::bounded(64);
        self.start_producer(tx).await?;
        self.start_consumers(Arc::new(rx));
        let handles = &mut self.handles;
        while let Some(res) = handles.join_next().await {
            match res {
                Err(join_err) => {
                    return Err(join_err.into());
                }
                Ok(Err(indexer_err)) => {
                    return Err(indexer_err);
                }
                Ok(Ok(())) => {}
            }
        }
        Ok(())
    }

    pub async fn start_producer(&mut self, tx: Sender<Vec<Block>>) -> Result<(), IndexerError> {
        let grpc_url = self.args.grpc_url.clone();
        let mut client = BlockStreamClient::connect(grpc_url)
            .await?
            .max_decoding_message_size(usize::MAX);
        let response = {
            if let Some(to) = self.args.to {
                let request = Request::new(BackfillRequest {
                    from: self.args.from,
                    to: to,
                    size: self.args.batch_size,
                });
                client.backfill(request).await?
            } else {
                let request = Request::new(BackfillToLiveRequest {
                    from: self.args.from,
                    size: self.args.batch_size,
                });
                client.backfill_to_live(request).await?
            }
        };
        let mut stream = response.into_inner();
        let cancel_token = self.shutdown_token.child_token();
        self.handles.spawn(async move {
            while let Some(item) = stream.next().await {
                if cancel_token.is_cancelled() {
                    break;
                }
                let blocks = item?.items;
                info!("Produced batch");
                tx.send(blocks).await?;
            }
            Ok(())
        });
        Ok(())
    }

    pub fn start_consumers(&mut self, rx: Arc<Receiver<Vec<Block>>>) {
        for _ in 0..self.args.concurrency {
            let rx = Arc::clone(&rx);
            let client = Arc::clone(&self.clickhouse);
            let cancel_token = self.shutdown_token.child_token();
            self.handles.spawn(async move {
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
                                tokio::try_join!(
                                    insert_block(&mut block_inserter, &block),
                                    insert_block_txs(&mut tx_inserter, &block),
                                    insert_block_logs(&mut log_inserter, &block),
                                )?;
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
    }
}

async fn insert_block(
    inserter: &mut Inserter<BlockRow>,
    block: &Block,
) -> Result<(), IndexerError> {
    let row = block.try_into()?;
    inserter.write(&row).await?;
    Ok(())
}

async fn insert_block_txs(
    inserter: &mut Inserter<TransactionRow>,
    block: &Block,
) -> Result<(), IndexerError> {
    let tx_rows = block
        .transactions
        .iter()
        .map(|tx| txn_to_row(&block, tx))
        .collect::<Result<Vec<_>, _>>()?;
    for row in tx_rows {
        inserter.write(&row).await?;
    }
    Ok(())
}

async fn insert_block_logs(
    inserter: &mut Inserter<LogRow>,
    block: &Block,
) -> Result<(), IndexerError> {
    let mut log_index = 0;
    let mut logs = Vec::new();
    for tx in &block.transactions {
        let receipt = tx
            .receipt
            .as_ref()
            .ok_or(ParseError::MissingField("transaction"))?;
        for log in &receipt.logs {
            logs.push((tx.index, &tx.hash, log));
        }
    }
    for (tx_index, tx_hash, log) in logs {
        let row = log_to_row(&log, log_index, &block, tx_index, tx_hash)?;
        inserter.write(&row).await?;
        log_index += 1;
    }
    Ok(())
}
