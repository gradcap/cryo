use std::collections::{HashMap, HashSet};

use ethers::prelude::*;
use polars::prelude::*;
use tokio::{sync::mpsc, task};

use super::{blocks::BlockColumns, logs::LogColumns, transactions::TransactionColumns};
use crate::{
    sources::GradTransaction,
    types::{
        BlockChunk, BlocksWithTxsAndReceipts, CollectError, Datatype, MultiDataset, RowFilter,
        Source, Table,
    },
};

#[async_trait::async_trait]
impl MultiDataset for BlocksWithTxsAndReceipts {
    fn name(&self) -> &'static str {
        "blocks_transactions_and_logs"
    }

    fn datatypes(&self) -> HashSet<Datatype> {
        [Datatype::Blocks, Datatype::Transactions, Datatype::Logs].into_iter().collect()
    }

    async fn collect_block_chunk(
        &self,
        chunk: &BlockChunk,
        source: &Source,
        schemas: HashMap<Datatype, Table>,
        _filter: HashMap<Datatype, RowFilter>,
    ) -> Result<HashMap<Datatype, DataFrame>, CollectError> {
        let rx = fetch_blocks_with_logs(chunk, source).await;
        let output = blocks_with_logs_to_dfs(
            rx,
            &schemas.get(&Datatype::Blocks),
            &schemas.get(&Datatype::Transactions),
            &schemas.get(&Datatype::Logs),
            source.chain_id,
        )
        .await;
        match output {
            Ok((Some(blocks_df), Some(txs_df), Some(logs_df))) => {
                let mut output: HashMap<Datatype, DataFrame> = HashMap::new();
                output.insert(Datatype::Blocks, blocks_df);
                output.insert(Datatype::Transactions, txs_df);
                output.insert(Datatype::Logs, logs_df);
                Ok(output)
            }
            Ok((_, _, _)) => Err(CollectError::BadSchemaError),
            Err(e) => Err(e),
        }
    }
}

async fn fetch_blocks_with_logs(
    block_chunk: &BlockChunk,
    source: &Source,
) -> mpsc::Receiver<BlockTxsLogs> {
    let (tx, rx) = mpsc::channel(block_chunk.numbers().len());
    let source = Arc::new(source.clone());

    for number in block_chunk.numbers() {
        let tx = tx.clone();
        let provider = source.fetcher.clone();
        task::spawn(async move {
            let block_result = provider.grad_get_block_with_txs(number).await;
            let receipts_result = provider.grad_get_block_receipts(number).await;

            // get gas usage
            let result = match (block_result, receipts_result) {
                (Ok(Some(block)), Ok(receipts)) => {
                    let logs: Vec<_> = receipts
                        .into_iter()
                        .map(|tr| (tr.local_parse_time_ns, tr.inner.logs))
                        .collect();
                    Ok((block, logs))
                }
                (Ok(None), _) => Err(CollectError::CollectError("no block found".into())),
                (Err(e), _) | (_, Err(e)) => Err(e),
            };

            // send to channel
            if let Err(mpsc::error::SendError(_e)) = tx.send(result).await {
                eprintln!("send error, try using a rate limit with --requests-per-second or limiting max concurrency with --max-concurrent-requests");
                std::process::exit(1)
            }
        });
    }
    rx
}

type BlockTxsLogs = Result<(Block<GradTransaction>, Vec<(U64, Vec<Log>)>), CollectError>;

async fn blocks_with_logs_to_dfs(
    mut rx: mpsc::Receiver<BlockTxsLogs>,
    blocks_schema: &Option<&Table>,
    txs_schema: &Option<&Table>,
    logs_schema: &Option<&Table>,
    chain_id: u64,
) -> Result<(Option<DataFrame>, Option<DataFrame>, Option<DataFrame>), CollectError> {
    let mut blocks = BlockColumns::default();
    let mut txs = TransactionColumns::default();
    let mut logs = LogColumns::default();

    while let Some(result) = rx.recv().await {
        match result {
            Ok((block, block_logs)) => {
                let block_time = block.time().unwrap_or_default();
                if let Some(schema) = blocks_schema {
                    blocks.process_block(&block, schema);
                }
                if let Some(schema) = txs_schema {
                    for tx in block.transactions {
                        txs.process_grad_transaction(&tx, &block_time, schema, None);
                    }
                }
                if let Some(logs_schema) = logs_schema {
                    for (lp_time_ns, tx_logs) in block_logs {
                        logs.process_logs(&block_time, lp_time_ns.as_u64(), tx_logs, logs_schema)?;
                    }
                }
            }
            Err(e) => return Err(e),
        }
    }

    let blocks_df = match blocks_schema {
        Some(schema) => Some(blocks.create_df(schema, chain_id)?),
        None => None,
    };
    let txs_df = match txs_schema {
        Some(schema) => Some(txs.create_df(schema, chain_id)?),
        None => None,
    };
    let logs_df = match logs_schema {
        Some(schema) => Some(logs.create_df(schema, chain_id)?),
        None => None,
    };

    Ok((blocks_df, txs_df, logs_df))
}
