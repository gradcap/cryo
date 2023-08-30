use std::sync::Arc;

use ethers::prelude::*;
use governor::{
    clock::DefaultClock,
    middleware::NoOpMiddleware,
    state::{direct::NotKeyed, InMemoryState},
};
use serde::{Deserialize, Serialize};
use tokio::sync::{AcquireError, Semaphore, SemaphorePermit};

use crate::CollectError;

/// RateLimiter based on governor crate
pub type RateLimiter = governor::RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>;

/// Options for fetching data from node
#[derive(Clone)]
pub struct Source {
    /// Shared provider for rpc data
    pub fetcher: Arc<Fetcher<Http>>,
    /// chain_id of network
    pub chain_id: u64,
    /// number of blocks per log request
    pub inner_request_size: u64,
    /// Maximum chunks collected concurrently
    pub max_concurrent_chunks: u64,
}

/// Wrapper over `Provider<P>` that adds concurrency and rate limiting controls
pub struct Fetcher<P> {
    /// provider data source
    pub provider: Provider<P>,
    /// semaphore for controlling concurrency
    pub semaphore: Option<Semaphore>,
    /// rate limiter for controlling request rate
    pub rate_limiter: Option<RateLimiter>,
}

/// Transaction receipt with local parse time
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GradTransaction {
    /// Wrapped transaction
    #[serde(flatten)]
    pub inner: Transaction,
    /// Time when transaction appeared on the node for the first time
    pub local_parse_time_ns: U64,
}

/// Receipt of an executed transaction with custom gradcap fields: local parse time when it appeared
/// on the node
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GradTransactionReceipt {
    /// Wrapped receipt
    #[serde(flatten)]
    pub inner: TransactionReceipt,
    /// Time when transaction appeared on the node for the first time
    pub local_parse_time_ns: U64,
}

type Result<T> = ::core::result::Result<T, CollectError>;

impl<P: JsonRpcClient> Fetcher<P> {
    /// Returns an array (possibly empty) of logs that match the filter
    pub async fn get_logs(&self, filter: &Filter) -> Result<Vec<Log>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_logs(filter).await)
    }

    /// Replays all transactions in a block returning the requested traces for each transaction
    pub async fn trace_replay_block_transactions(
        &self,
        block: BlockNumber,
        trace_types: Vec<TraceType>,
    ) -> Result<Vec<BlockTrace>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.trace_replay_block_transactions(block, trace_types).await)
    }

    /// Replays a transaction, returning the traces
    pub async fn trace_replay_transaction(
        &self,
        tx_hash: TxHash,
        trace_types: Vec<TraceType>,
    ) -> Result<BlockTrace> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.trace_replay_transaction(tx_hash, trace_types).await)
    }

    /// Gets the transaction with transaction_hash
    pub async fn get_transaction(&self, tx_hash: TxHash) -> Result<Option<Transaction>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_transaction(tx_hash).await)
    }

    /// Gets the transaction receipt with transaction_hash
    pub async fn get_transaction_receipt(
        &self,
        tx_hash: TxHash,
    ) -> Result<Option<TransactionReceipt>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_transaction_receipt(tx_hash).await)
    }

    /// Gets the block at `block_num` (transaction hashes only)
    pub async fn get_block(&self, block_num: impl Into<BlockId>) -> Result<Option<Block<TxHash>>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_block(block_num.into()).await)
    }

    /// Gets the block at `block_num` (full transactions included) using gradcap API endpoint with
    /// extra payload
    pub async fn grad_get_block_with_txs(
        &self,
        block_num: u64,
    ) -> Result<Option<Block<GradTransaction>>> {
        let _permit = self.permit_request().await;

        let block_num = serde_json::to_value(BlockNumber::from(block_num)).unwrap();
        println!("fetching block {}", block_num);
        Self::map_err(
            self.provider
                .request("gradcap_getBlockByNumber", [block_num.clone(), true.into()])
                .await,
        )
    }

    /// Returns all receipts for a block using gradcap API endpoint with extra payload.
    pub async fn grad_get_block_receipts(
        &self,
        block_num: u64,
    ) -> Result<Vec<GradTransactionReceipt>> {
        let _permit = self.permit_request().await;
        let block_num = serde_json::to_value(BlockNumber::from(block_num)).unwrap();
        println!("fetching receipts for block {}", block_num);
        Self::map_err(self.provider.request("gradcap_getBlockReceipts", [block_num]).await)
    }

    /// Gets the block at `block_num` (full transactions included)
    pub async fn get_block_with_txs(&self, block_num: u64) -> Result<Option<Block<Transaction>>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_block_with_txs(block_num).await)
    }

    /// Returns all receipts for a block.
    pub async fn get_block_receipts(&self, block_num: u64) -> Result<Vec<TransactionReceipt>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.get_block_receipts(block_num).await)
    }

    /// Returns traces created at given block
    pub async fn trace_block(&self, block_num: BlockNumber) -> Result<Vec<Trace>> {
        let _permit = self.permit_request().await;
        Self::map_err(self.provider.trace_block(block_num).await)
    }

    /// Returns all traces of a given transaction
    pub async fn trace_transaction(&self, tx_hash: TxHash) -> Result<Vec<Trace>> {
        let _permit = self.permit_request().await;
        self.provider.trace_transaction(tx_hash).await.map_err(CollectError::ProviderError)
    }

    /// Get the block number
    pub async fn get_block_number(&self) -> Result<U64> {
        Self::map_err(self.provider.get_block_number().await)
    }

    async fn permit_request(
        &self,
    ) -> Option<::core::result::Result<SemaphorePermit<'_>, AcquireError>> {
        let permit = match &self.semaphore {
            Some(semaphore) => Some(semaphore.acquire().await),
            _ => None,
        };
        if let Some(limiter) = &self.rate_limiter {
            limiter.until_ready().await;
        }
        permit
    }

    fn map_err<T>(res: ::core::result::Result<T, ProviderError>) -> Result<T> {
        res.map_err(CollectError::ProviderError)
    }
}

// impl Source {
//     /// create Source for an individual chunk
//     pub fn build_source(&self) -> Source {
//         let sem = Arc::new(tokio::sync::Semaphore::new(
//             self.max_concurrent_blocks as usize,
//         ));
//         Source {
//             provider: Arc::clone(&self.provider),
//             rate_limiter: self.rate_limiter.as_ref().map(Arc::clone),
//             semaphore: sem,
//             chain_id: self.chain_id,
//             inner_request_size: self.inner_request_size,
//             max
//         }
//     }
// }

// pub struct SourceBuilder {
//     provider: Option<Arc<Provider<Http>>>,
//     semaphore: Option<Arc<Semaphore>>,
//     rate_limiter: Option<Arc<RateLimiter>>,
//     chain_id: Option<u64>,
//     inner_request_size: Option<u64>,
//     max_concurrent_chunks: Option<u64>,
// }

// impl SourceBuilder {
//     pub fn new() -> SourceBuilder {
//         SourceBuilder {
//             provider: None,
//             semaphore: None,
//             rate_limiter: None,
//             chain_id: None,
//             inner_request_size: None,
//             max_concurrent_chunks: None,
//         }
//     }

//     pub fn provider(mut self, provider: Arc<Provider<Http>>) -> Self {
//         self.provider = Some(provider);
//         self
//     }

//     pub fn semaphore(mut self, semaphore: Arc<Semaphore>) -> Self {
//         self.semaphore = Some(semaphore);
//         self
//     }

//     pub fn rate_limiter(mut self, rate_limiter: Arc<RateLimiter>) -> Self {
//         self.rate_limiter = Some(rate_limiter);
//         self
//     }

//     pub fn chain_id(mut self, chain_id: u64) -> Self {
//         self.chain_id = Some(chain_id);
//         self
//     }

//     pub fn inner_request_size(mut self, inner_request_size: u64) -> Self {
//         self.inner_request_size = Some(inner_request_size);
//         self
//     }

//     pub fn max_concurrent_chunks(mut self, max_concurrent_chunks: u64) -> Self {
//         self.max_concurrent_chunks = Some(max_concurrent_chunks);
//         self
//     }

//     pub fn build(self) -> Result<Source, &'static str> {
//         if let (
//             Some(provider),
//             Some(semaphore),
//             Some(chain_id),
//             Some(inner_request_size),
//             Some(max_concurrent_chunks),
//         ) = ( self.provider, self.semaphore, self.chain_id, self.inner_request_size,
//           self.max_concurrent_chunks,
//         ) { Ok(Source { provider, semaphore, rate_limiter: self.rate_limiter, chain_id,
//           inner_request_size, max_concurrent_chunks, })
//         } else {
//             Err("Cannot build Source. Missing fields.")
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_grad_transaction_receipt() {
        let res: GradTransactionReceipt = serde_json::from_str(r#"{
            "transactionHash": "0xa3ece39ae137617669c6933b7578b94e705e765683f260fcfe30eaa41932610f",
            "blockHash": "0xf6084155ff2022773b22df3217d16e9df53cbc42689b27ca4789e06b6339beb2",
            "blockNumber": "0x52a975",
            "contractAddress": null,
            "cumulativeGasUsed": "0x797db0",
            "from": "0xd907941c8b3b966546fc408b8c942eb10a4f98df",
            "gasUsed": "0x1308c",
            "logs": [
                {
                    "blockHash": "0xf6084155ff2022773b22df3217d16e9df53cbc42689b27ca4789e06b6339beb2",
                    "address": "0xd6df5935cd03a768b7b9e92637a01b25e24cb709",
                    "logIndex": "0x119",
                    "data": "0x0000000000000000000000000000000000000000000000000000008bb2c97000",
                    "removed": false,
                    "topics": [
                        "0x8940c4b8e215f8822c5c8f0056c12652c746cbc57eedbd2a440b175971d47a77",
                        "0x000000000000000000000000d907941c8b3b966546fc408b8c942eb10a4f98df"
                    ],
                    "blockNumber": "0x52a975",
                    "transactionIndex": "0x29",
                    "transactionHash": "0xa3ece39ae137617669c6933b7578b94e705e765683f260fcfe30eaa41932610f"
                },
                {
                    "blockHash": "0xf6084155ff2022773b22df3217d16e9df53cbc42689b27ca4789e06b6339beb2",
                    "address": "0xd6df5935cd03a768b7b9e92637a01b25e24cb709",
                    "logIndex": "0x11a",
                    "data": "0x0000000000000000000000000000000000000000000000000000008bb2c97000",
                    "removed": false,
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x0000000000000000000000000000000000000000000000000000000000000000",
                        "0x000000000000000000000000d907941c8b3b966546fc408b8c942eb10a4f98df"
                    ],
                    "blockNumber": "0x52a975",
                    "transactionIndex": "0x29",
                    "transactionHash": "0xa3ece39ae137617669c6933b7578b94e705e765683f260fcfe30eaa41932610f"
                }
            ],
            "logsBloom": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000008000000000000000000000000000000000000000000000000000000000000008000000000000000000000000000000000000000000000000020000000000000000000800000000000000004010000010100000000000000000000000000000000000000000000000000040000080000000000000080000000000000000000000000000000000000000000020000000000000000000000002000000000000000000000000000000000000000000000000000020000000010000000000000000000000000000000000000000000000000000000000",
            "root": null,
            "status": "0x1",
            "to": "0xd6df5935cd03a768b7b9e92637a01b25e24cb709",
            "transactionIndex": "0x29",
            "localParseTimeNs": "0x177a077a4bd55637"
        }"#).unwrap();
        assert_eq!(U64::from(1691672831870719543u64), res.local_parse_time_ns);
    }

    #[test]
    fn decode_grad_block_with_txs() {
        let res: Option<Block<GradTransaction>> = serde_json::from_str(r#"{
            "baseFeePerGas":"0x342770c0",
            "difficulty":"0x0",
            "extraData":"0xd883010c01846765746888676f312e32302e37856c696e7578",
            "gasLimit":"0xafa5bd",
            "gasUsed":"0x5208",
            "hash":"0x376f233432cc999f2ad42a60c082f08ca3c88450e325c01485df8fef5085b663",
            "logsBloom":"0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
            "miner":"0x0000000000000000000000000000000000000000",
            "mixHash":"0x0000000000000000000000000000000000000000000000000000000000000000",
            "nonce":"0x0000000000000000",
            "number":"0x1",
            "parentHash":"0xde992fcf9cfbcb9afa748e57e9c72b155d2c6b7754565601d562220cf90af2fd",
            "receiptsRoot":"0xf78dfb743fbd92ade140711c8bbc542b5e307f0ab7984eff35d751969fe57efa",
            "sha3Uncles":"0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
            "size":"0x2b5",
            "stateRoot":"0x2327c9aeed645c25f26bae1522d657be4f4b9d07e0f808287e9c1f7c4eda857e",
            "timestamp":"0x64d61d2c",
            "totalDifficulty":"0x0",
            "transactions":[{
                "blockHash":"0x376f233432cc999f2ad42a60c082f08ca3c88450e325c01485df8fef5085b663",
                "blockNumber":"0x1",
                "from":"0x6e7dd432a84311b6769a844f13cdb7387c65ef28",
                "gas":"0x5208",
                "gasPrice":"0x342770c1",
                "maxFeePerGas":"0x77359401",
                "maxPriorityFeePerGas":"0x1",
                "hash":"0x8bf364a44791541c481244e4c182b53c82b852eb2db0f21e3fe0b94690af144b",
                "input":"0x",
                "nonce":"0x0",
                "to":"0x6e7dd432a84311b6769a844f13cdb7387c65ef28",
                "transactionIndex":"0x0",
                "value":"0x2b5e3af16b1880000",
                "type":"0x2",
                "accessList":[],
                "chainId":"0x539",
                "v":"0x0",
                "r":"0xdbc82af0d508aad60b308c7b3d64a21bcac5bf0ab32551772f8ac799e90759b5",
                "s":"0x2af062a468a3a251ee125e459f6a2c6f4f007e959a47e681110c9be7cfe3c651",
                "yParity":"0x0",
                "localParseTimeNs":"0x177a5117d1b1d6ae"
            }],
            "transactionsRoot":"0x37c6f1297e10818fe0399993b69cb9af652d863dd4abfff39b0f481e58c8a4b4",
            "uncles":[],
            "withdrawals":[],
            "withdrawalsRoot":"0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"
        }"#).unwrap();
        assert_eq!(
            U64::from(1691753772775233198u64),
            res.unwrap().transactions[0].local_parse_time_ns
        );
    }
}
