use std::{
    ops::Bound,
    path::{Path, PathBuf},
    sync::Arc,
};

use chrono::NaiveDate;
use ethers::prelude::*;

use cryo_freeze::{BlockChunk, Chunk, ChunkData, ParseError, Subchunk};
use serde::{Deserialize, Serialize};

use crate::args::Args;

pub(crate) async fn parse_blocks(
    args: &Args,
    provider: Arc<Provider<Http>>,
) -> Result<Vec<(Chunk, Option<String>)>, ParseError> {
    // parse inputs into BlockChunks
    let block_chunks = if let Some(date) = args.date {
        let dates_map_path = PathBuf::from("./aux/dates_map.toml");
        let mut dates_map = DateBlocksMap::default();
        dates_map.load_from(&dates_map_path)?;
        let bounds = dates_map.calculate_bounds(date, &provider).await.map_err(|e| {
            ParseError::ParseError(format!("Failed to calculate bounds for date due to {e}"))
        })?;
        dates_map.save_to(&dates_map_path)?;
        bounds.map(|range| vec![range]).unwrap_or_default()
    } else {
        match &args.blocks {
            Some(inputs) => parse_block_inputs(inputs, &provider).await?,
            None => return Err(ParseError::ParseError("could not parse block inputs".to_string())),
        }
    };
    postprocess_block_chunks(block_chunks, args, provider).await
}

async fn postprocess_block_chunks(
    block_chunks: Vec<BlockChunk>,
    args: &Args,
    provider: Arc<Provider<Http>>,
) -> Result<Vec<(Chunk, Option<String>)>, ParseError> {
    // align
    let block_chunks = if args.align {
        block_chunks.into_iter().filter_map(|x| x.align(args.chunk_size)).collect()
    } else {
        block_chunks
    };

    // split block range into chunks
    let block_chunks = match args.n_chunks {
        Some(n_chunks) => block_chunks.subchunk_by_count(&n_chunks),
        None => block_chunks.subchunk_by_size(&args.chunk_size),
    };

    // apply reorg buffer
    let block_chunks = apply_reorg_buffer(block_chunks, args.reorg_buffer, &provider).await?;

    // put into Chunk enums
    let chunks: Vec<(Chunk, Option<String>)> =
        block_chunks.iter().map(|x| (Chunk::Block(x.clone()), None)).collect();

    Ok(chunks)
}

pub(crate) async fn get_default_block_chunks(
    args: &Args,
    provider: Arc<Provider<Http>>,
) -> Result<Vec<(Chunk, Option<String>)>, ParseError> {
    let block_chunks = parse_block_inputs(&String::from(r"0:latest"), &provider).await?;
    postprocess_block_chunks(block_chunks, args, provider).await
}

/// parse block numbers to freeze
async fn parse_block_inputs<P>(
    inputs: &str,
    provider: &Provider<P>,
) -> Result<Vec<BlockChunk>, ParseError>
where
    P: JsonRpcClient,
{
    let parts: Vec<&str> = inputs.split(' ').collect();
    match parts.len() {
        1 => {
            let first_input = parts.first().ok_or_else(|| {
                ParseError::ParseError("Failed to get the first input".to_string())
            })?;
            parse_block_token(first_input, true, provider).await.map(|x| vec![x])
        }
        _ => {
            let mut chunks = Vec::new();
            for part in parts {
                chunks.push(parse_block_token(part, false, provider).await?);
            }
            Ok(chunks)
        }
    }
}

enum RangePosition {
    First,
    Last,
    None,
}

async fn parse_block_token<P>(
    s: &str,
    as_range: bool,
    provider: &Provider<P>,
) -> Result<BlockChunk, ParseError>
where
    P: JsonRpcClient,
{
    let s = s.replace('_', "");

    let parts: Vec<&str> = s.split(':').collect();
    match parts.as_slice() {
        [block_ref] => {
            let block = parse_block_number(block_ref, RangePosition::None, provider).await?;
            Ok(BlockChunk::Numbers(vec![block]))
        }
        [first_ref, second_ref] => {
            let (start_block, end_block) = match (first_ref, second_ref) {
                _ if first_ref.starts_with('-') => {
                    let end_block =
                        parse_block_number(second_ref, RangePosition::Last, provider).await?;
                    let start_block = end_block
                        .checked_sub(first_ref[1..].parse::<u64>().map_err(|_e| {
                            ParseError::ParseError("start_block parse error".to_string())
                        })?)
                        .ok_or_else(|| {
                            ParseError::ParseError("start_block underflow".to_string())
                        })?;
                    (start_block, end_block)
                }
                _ if second_ref.starts_with('+') => {
                    let start_block =
                        parse_block_number(first_ref, RangePosition::First, provider).await?;
                    let end_block = start_block
                        .checked_add(second_ref[1..].parse::<u64>().map_err(|_e| {
                            ParseError::ParseError("start_block parse error".to_string())
                        })?)
                        .ok_or_else(|| ParseError::ParseError("end_block underflow".to_string()))?;
                    (start_block, end_block)
                }
                _ => {
                    let start_block =
                        parse_block_number(first_ref, RangePosition::First, provider).await?;
                    let end_block =
                        parse_block_number(second_ref, RangePosition::Last, provider).await?;
                    (start_block, end_block)
                }
            };

            if end_block <= start_block {
                Err(ParseError::ParseError(
                    "end_block should not be less than start_block".to_string(),
                ))
            } else if as_range {
                Ok(BlockChunk::Range(start_block, end_block))
            } else {
                Ok(BlockChunk::Numbers((start_block..=end_block).collect()))
            }
        }
        _ => Err(ParseError::ParseError(
            "blocks must be in format block_number or start_block:end_block".to_string(),
        )),
    }
}

async fn parse_block_number<P>(
    block_ref: &str,
    range_position: RangePosition,
    provider: &Provider<P>,
) -> Result<u64, ParseError>
where
    P: JsonRpcClient,
{
    match (block_ref, range_position) {
        ("latest", _) => provider.get_block_number().await.map(|n| n.as_u64()).map_err(|_e| {
            ParseError::ParseError("Error retrieving latest block number".to_string())
        }),
        ("", RangePosition::First) => Ok(0),
        ("", RangePosition::Last) => {
            provider.get_block_number().await.map(|n| n.as_u64()).map_err(|_e| {
                ParseError::ParseError("Error retrieving last block number".to_string())
            })
        }
        ("", RangePosition::None) => Err(ParseError::ParseError("invalid input".to_string())),
        _ if block_ref.ends_with('B') | block_ref.ends_with('b') => {
            let s = &block_ref[..block_ref.len() - 1];
            s.parse::<f64>()
                .map(|n| (1e9 * n) as u64)
                .map_err(|_e| ParseError::ParseError("Error parsing block ref".to_string()))
        }
        _ if block_ref.ends_with('M') | block_ref.ends_with('m') => {
            let s = &block_ref[..block_ref.len() - 1];
            s.parse::<f64>()
                .map(|n| (1e6 * n) as u64)
                .map_err(|_e| ParseError::ParseError("Error parsing block ref".to_string()))
        }
        _ if block_ref.ends_with('K') | block_ref.ends_with('k') => {
            let s = &block_ref[..block_ref.len() - 1];
            s.parse::<f64>()
                .map(|n| (1e3 * n) as u64)
                .map_err(|_e| ParseError::ParseError("Error parsing block ref".to_string()))
        }
        _ => block_ref
            .parse::<f64>()
            .map_err(|_e| ParseError::ParseError("Error parsing block ref".to_string()))
            .map(|x| x as u64),
    }
}

async fn apply_reorg_buffer(
    block_chunks: Vec<BlockChunk>,
    reorg_filter: u64,
    provider: &Provider<Http>,
) -> Result<Vec<BlockChunk>, ParseError> {
    match reorg_filter {
        0 => Ok(block_chunks),
        reorg_filter => {
            let latest_block = match provider.get_block_number().await {
                Ok(result) => result.as_u64(),
                Err(_e) => {
                    return Err(ParseError::ParseError("reorg buffer parse error".to_string()))
                }
            };
            let max_allowed = latest_block - reorg_filter;
            Ok(block_chunks
                .into_iter()
                .filter_map(|x| match x.max_value() {
                    Some(max_block) if max_block <= max_allowed => Some(x),
                    _ => None,
                })
                .collect())
        }
    }
}

type BlockRange = (u64, u64);

#[derive(Debug, Default, Serialize, Deserialize)]
struct DateBlocksMap {
    /// Tracks block number ranges (inclusive) by date
    blocks: std::collections::BTreeMap<NaiveDate, BlockRange>,
}

impl DateBlocksMap {
    fn mark_block_date(&mut self, block_num: u64, date: NaiveDate) {
        let (min, max) = self.blocks.entry(date).or_insert((block_num, block_num));
        if block_num < *min {
            *min = block_num;
        } else if block_num > *max {
            *max = block_num;
        }
    }

    async fn get_and_account_block(
        &mut self,
        num: impl Into<BlockNumber>,
        provider: &Provider<Http>,
    ) -> Result<Option<Block<TxHash>>, ProviderError> {
        let block_id = BlockId::Number(num.into());
        println!("Fetching block {block_id:?}");
        let block = provider.get_block(block_id).await?;
        if let Some(block) = &block {
            if let (Ok(t), Some(block_num)) = (block.time(), block.number) {
                self.mark_block_date(block_num.as_u64(), t.date_naive())
            }
        }
        Ok(block)
    }

    async fn calculate_bounds(
        &mut self,
        date: NaiveDate,
        provider: &Provider<Http>,
    ) -> Result<Option<BlockChunk>, ProviderError> {
        if self.blocks.is_empty() && self.get_and_account_block(1, provider).await?.is_none() {
            println!("First block not available");
            return Ok(None)
        }
        if date > *self.blocks.last_key_value().unwrap().0 {
            let latest_block = provider.get_block_number().await?;
            self.get_and_account_block(latest_block, provider).await?;
            if date >= *self.blocks.last_key_value().unwrap().0 {
                println!("{} may still get new blocks", date);
                return Ok(None)
            }
        }
        let is_tight = |range: BlockRange| range.0 + 1 >= range.1;
        let mid_point = |range: BlockRange| (range.0 + range.1) / 2;
        let first = loop {
            let start_range = self.current_date_start_block_range(&date);
            if is_tight(start_range) {
                break start_range.1
            }
            self.get_and_account_block(mid_point(start_range), provider).await?;
        };
        let end = loop {
            let end_range = self.current_date_end_block_range(&date);
            if is_tight(end_range) {
                break end_range.1
            }
            self.get_and_account_block(mid_point(end_range), provider).await?;
        };
        Ok(Some(BlockChunk::RangeForDate(first, end, date)))
    }

    fn current_date_end_block_range(&self, date: &NaiveDate) -> BlockRange {
        let nearest = Bound::Excluded(date);
        let (next_first, _) =
            self.blocks.lower_bound(nearest).value().unwrap_or(&(u64::MAX, u64::MAX));
        if let Some((_, cur_last)) = self.blocks.get(date) {
            (*cur_last, *next_first)
        } else {
            let (_, prev_last) = self.blocks.upper_bound(nearest).value().unwrap_or(&(0, 0));
            (*prev_last, *next_first)
        }
    }

    fn current_date_start_block_range(&self, date: &NaiveDate) -> BlockRange {
        let nearest = Bound::Excluded(date);
        let (_, prev_last) = self.blocks.upper_bound(nearest).value().unwrap_or(&(0, 0));
        if let Some((cur_first, _)) = self.blocks.get(date) {
            (*prev_last, *cur_first)
        } else {
            let (next_first, _) =
                self.blocks.lower_bound(nearest).value().unwrap_or(&(u64::MAX, u64::MAX));
            (*prev_last, *next_first)
        }
    }

    fn serialize(&self) -> String {
        toml::to_string(&self).unwrap()
    }
    fn deserialize(&mut self, str: &str) -> Result<(), toml::de::Error> {
        *self = toml::from_str(str)?;
        Ok(())
    }

    fn load_from(&mut self, path: &Path) -> Result<(), ParseError> {
        if std::path::Path::exists(path) {
            let str = std::fs::read_to_string(path).map_err(|e| {
                ParseError::ParseError(format!("Problem reading dates map due to {e}"))
            })?;
            self.deserialize(&str).map_err(|e| {
                ParseError::ParseError(format!("Unable to deserialize dates map due to {e}"))
            })
        } else {
            Ok(())
        }
    }
    fn save_to(&self, path: &Path) -> Result<(), ParseError> {
        std::fs::write(path, self.serialize())
            .map_err(|e| ParseError::ParseError(format!("Problem saving dates map due to {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    enum BlockTokenTest<'a> {
        WithoutMock((&'a str, BlockChunk)),   // Token | Expected
        WithMock((&'a str, BlockChunk, u64)), // Token | Expected | Mock Block Response
    }

    async fn block_token_test_helper(tests: Vec<(BlockTokenTest<'_>, bool)>) {
        let (provider, mock) = Provider::mocked();
        for (test, res) in tests {
            match test {
                BlockTokenTest::WithMock((token, expected, latest)) => {
                    mock.push(U64::from(latest)).unwrap();
                    assert_eq!(block_token_test_executor(&token, expected, &provider).await, res);
                }
                BlockTokenTest::WithoutMock((token, expected)) => {
                    assert_eq!(block_token_test_executor(&token, expected, &provider).await, res);
                }
            }
        }
    }

    async fn block_token_test_executor<P>(
        token: &str,
        expected: BlockChunk,
        provider: &Provider<P>,
    ) -> bool
    where
        P: JsonRpcClient,
    {
        match expected {
            BlockChunk::Numbers(expected_block_numbers) => {
                let block_chunks = parse_block_token(token, false, &provider).await.unwrap();
                assert!(matches!(block_chunks, BlockChunk::Numbers { .. }));
                let BlockChunk::Numbers(block_numbers) = block_chunks else {
                    panic!("Unexpected shape")
                };
                return block_numbers == expected_block_numbers
            }
            BlockChunk::Range(expected_range_start, expected_range_end) |
            BlockChunk::RangeForDate(expected_range_start, expected_range_end, _) => {
                let block_chunks = parse_block_token(token, true, &provider).await.unwrap();
                assert!(matches!(block_chunks, BlockChunk::Range { .. }));
                let BlockChunk::Range(range_start, range_end) = block_chunks else {
                    panic!("Unexpected shape")
                };
                return expected_range_start == range_start && expected_range_end == range_end
            }
        }
    }

    enum BlockInputTest<'a> {
        WithoutMock((&'a String, Vec<BlockChunk>)), // Token | Expected
        WithMock((&'a String, Vec<BlockChunk>, u64)), // Token | Expected | Mock Block Response
    }

    async fn block_input_test_helper(tests: Vec<(BlockInputTest<'_>, bool)>) {
        let (provider, mock) = Provider::mocked();
        for (test, res) in tests {
            match test {
                BlockInputTest::WithMock((inputs, expected, latest)) => {
                    mock.push(U64::from(latest)).unwrap();
                    assert_eq!(block_input_test_executor(&inputs, expected, &provider).await, res);
                }
                BlockInputTest::WithoutMock((inputs, expected)) => {
                    assert_eq!(block_input_test_executor(&inputs, expected, &provider).await, res);
                }
            }
        }
    }

    async fn block_input_test_executor<P>(
        inputs: &String,
        expected: Vec<BlockChunk>,
        provider: &Provider<P>,
    ) -> bool
    where
        P: JsonRpcClient,
    {
        let block_chunks = parse_block_inputs(inputs, &provider).await.unwrap();
        assert_eq!(block_chunks.len(), expected.len());
        for (i, block_chunk) in block_chunks.iter().enumerate() {
            let expected_chunk = &expected[i];
            match expected_chunk {
                BlockChunk::Numbers(expected_block_numbers) => {
                    assert!(matches!(block_chunk, BlockChunk::Numbers { .. }));
                    let BlockChunk::Numbers(block_numbers) = block_chunk else {
                        panic!("Unexpected shape")
                    };
                    if expected_block_numbers != block_numbers {
                        return false
                    }
                }
                BlockChunk::Range(expected_range_start, expected_range_end) |
                BlockChunk::RangeForDate(expected_range_start, expected_range_end, _) => {
                    assert!(matches!(block_chunk, BlockChunk::Range { .. }));
                    let BlockChunk::Range(range_start, range_end) = block_chunk else {
                        panic!("Unexpected shape")
                    };
                    if expected_range_start != range_start || expected_range_end != range_end {
                        return false
                    }
                }
            }
        }
        return true
    }

    enum BlockNumberTest<'a> {
        WithoutMock((&'a str, RangePosition, u64)),
        WithMock((&'a str, RangePosition, u64, u64)),
    }

    async fn block_number_test_helper(tests: Vec<(BlockNumberTest<'_>, bool)>) {
        let (provider, mock) = Provider::mocked();
        for (test, res) in tests {
            match test {
                BlockNumberTest::WithMock((block_ref, range_position, expected, latest)) => {
                    mock.push(U64::from(latest)).unwrap();
                    assert_eq!(
                        block_number_test_executor(&block_ref, range_position, expected, &provider)
                            .await,
                        res
                    );
                }
                BlockNumberTest::WithoutMock((block_ref, range_position, expected)) => {
                    assert_eq!(
                        block_number_test_executor(&block_ref, range_position, expected, &provider)
                            .await,
                        res
                    );
                }
            }
        }
    }

    async fn block_number_test_executor<P>(
        block_ref: &str,
        range_position: RangePosition,
        expected: u64,
        provider: &Provider<P>,
    ) -> bool
    where
        P: JsonRpcClient,
    {
        let block_number = parse_block_number(block_ref, range_position, &provider).await.unwrap();
        return block_number == expected
    }

    #[tokio::test]
    async fn block_token_parsing() {
        // Ranges
        let tests: Vec<(BlockTokenTest<'_>, bool)> = vec![
            // Range Type
            (BlockTokenTest::WithoutMock((r"1:2", BlockChunk::Range(1, 2))), true), /* Single block range */
            (BlockTokenTest::WithoutMock((r"0:2", BlockChunk::Range(0, 2))), true), /* Implicit start */
            (BlockTokenTest::WithoutMock((r"-10:100", BlockChunk::Range(90, 100))), true), /* Relative negative */
            (BlockTokenTest::WithoutMock((r"10:+100", BlockChunk::Range(10, 110))), true), /* Relative positive */
            (BlockTokenTest::WithMock((r"1:latest", BlockChunk::Range(1, 12), 12)), true), /* Explicit latest */
            (BlockTokenTest::WithMock((r"1:", BlockChunk::Range(1, 12), 12)), true), /* Implicit latest */
            // Number type
            (BlockTokenTest::WithoutMock((r"1", BlockChunk::Numbers(vec![1]))), true), /* Single block */
        ];
        block_token_test_helper(tests).await;
    }

    #[tokio::test]
    async fn block_inputs_parsing() {
        // Ranges
        let block_inputs_single = String::from(r"1:2");
        let block_inputs_multiple = String::from(r"1 2");
        let block_inputs_latest = String::from(r"1:latest");
        let block_inputs_multiple_complex = String::from(r"15M:+1 1000:1002 -3:1b 2000");
        let tests: Vec<(BlockInputTest<'_>, bool)> = vec![
            // Range Type
            (
                BlockInputTest::WithoutMock((&block_inputs_single, vec![BlockChunk::Range(1, 2)])),
                true,
            ), // Single input
            (
                BlockInputTest::WithoutMock((
                    &block_inputs_multiple,
                    vec![BlockChunk::Numbers(vec![1]), BlockChunk::Numbers(vec![2])],
                )),
                true,
            ), // Multi input
            (
                BlockInputTest::WithMock((
                    &block_inputs_latest,
                    vec![BlockChunk::Range(1, 12)],
                    12,
                )),
                true,
            ), // Single input latest
            (
                BlockInputTest::WithoutMock((
                    &block_inputs_multiple_complex,
                    vec![
                        BlockChunk::Numbers(vec![15000000, 15000001]),
                        BlockChunk::Numbers(vec![1000, 1001, 1002]),
                        BlockChunk::Numbers(vec![999999997, 999999998, 999999999, 1000000000]),
                        BlockChunk::Numbers(vec![2000]),
                    ],
                )),
                true,
            ), // Multi input complex
        ];
        block_input_test_helper(tests).await;
    }

    #[tokio::test]
    async fn block_number_parsing() {
        // Ranges
        let tests: Vec<(BlockNumberTest<'_>, bool)> = vec![
            (BlockNumberTest::WithoutMock((r"1", RangePosition::None, 1)), true), // Integer
            (BlockNumberTest::WithMock((r"latest", RangePosition::None, 12, 12)), true), /* Lastest block */
            (BlockNumberTest::WithoutMock((r"", RangePosition::First, 0)), true), // First block
            (BlockNumberTest::WithMock((r"", RangePosition::Last, 12, 12)), true), // Last block
            (BlockNumberTest::WithoutMock((r"1B", RangePosition::None, 1000000000)), true), // B
            (BlockNumberTest::WithoutMock((r"1M", RangePosition::None, 1000000)), true), // M
            (BlockNumberTest::WithoutMock((r"1K", RangePosition::None, 1000)), true), // K
            (BlockNumberTest::WithoutMock((r"1b", RangePosition::None, 1000000000)), true), // b
            (BlockNumberTest::WithoutMock((r"1m", RangePosition::None, 1000000)), true), // m
            (BlockNumberTest::WithoutMock((r"1k", RangePosition::None, 1000)), true), // k
        ];
        block_number_test_helper(tests).await;
    }

    #[test]
    fn test_serde() {
        let mut map = DateBlocksMap::default();
        let example_str = r#"
            [blocks]
            2020-05-10 = [10, 20]
            2021-01-01 = [100, 200]
            2022-12-31 = [9876543210, 9876543212]"#;
        map.deserialize(example_str).expect("should parse");

        assert_eq!(
            example_str.split_whitespace().collect::<Vec<_>>(),
            map.serialize().split_whitespace().collect::<Vec<_>>()
        );
    }
}
