use std::{
    ops::Bound,
    path::{Path, PathBuf},
    sync::Arc,
};

use chrono::NaiveDate;
use ethers::prelude::*;

use cryo_freeze::{BlockChunk, Chunk, CollectError, Fetcher, ParseError};
use serde::{Deserialize, Serialize};

type BlockRange = (u64, u64);

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct DateBlocksMap {
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

    async fn get_and_account_block<P: JsonRpcClient>(
        &mut self,
        num: impl Into<BlockNumber>,
        provider: &Fetcher<P>,
    ) -> Result<Option<Block<TxHash>>, CollectError> {
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

    pub(crate) async fn calculate_bounds<P: JsonRpcClient>(
        &mut self,
        date: NaiveDate,
        provider: &Fetcher<P>,
    ) -> Result<Option<BlockChunk>, CollectError> {
        if self.blocks.is_empty() && self.get_and_account_block(1, provider).await?.is_none() {
            println!("First block not available");
            return Ok(None)
        }
        if date >= *self.blocks.last_key_value().unwrap().0 {
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
        let last = loop {
            let end_range = self.current_date_end_block_range(&date);
            if is_tight(end_range) {
                break end_range.0
            }
            self.get_and_account_block(mid_point(end_range), provider).await?;
        };
        Ok(Some(BlockChunk::RangeForDate(first, last, date)))
    }

    fn current_date_end_block_range(&self, date: &NaiveDate) -> BlockRange {
        let nearest = Bound::Excluded(date);
        let next_first = self.first_block_after(nearest);
        if let Some((_, cur_last)) = self.blocks.get(date) {
            (*cur_last, next_first)
        } else {
            let prev_last = self.last_block_before(nearest);
            (prev_last, next_first)
        }
    }

    fn current_date_start_block_range(&self, date: &NaiveDate) -> BlockRange {
        let nearest = Bound::Excluded(date);
        let prev_last = self.last_block_before(nearest);
        if let Some((cur_first, _)) = self.blocks.get(date) {
            (prev_last, *cur_first)
        } else {
            let next_first = self.first_block_after(nearest);
            (prev_last, next_first)
        }
    }

    fn first_block_after(&self, date: Bound<&NaiveDate>) -> u64 {
        let next = self.blocks.range((date, Bound::Unbounded)).next();
        let (_, (next_first, _)) = next.unwrap_or((&NaiveDate::MAX, &(u64::MAX, u64::MAX)));
        *next_first
    }

    fn last_block_before(&self, date: Bound<&NaiveDate>) -> u64 {
        let prev = self.blocks.range((Bound::Unbounded, date)).next_back();
        let (_, (_, prev_last)) = prev.unwrap_or((&NaiveDate::MIN, &(0, 0)));
        *prev_last
    }

    fn serialize(&self) -> String {
        toml::to_string(&self).unwrap()
    }
    fn deserialize(&mut self, str: &str) -> Result<(), toml::de::Error> {
        *self = toml::from_str(str)?;
        Ok(())
    }

    pub(crate) fn load_from(&mut self, path: &Path) -> Result<(), ParseError> {
        if path.exists() {
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
    pub(crate) fn save_to(&self, path: &Path) -> Result<(), ParseError> {
        std::fs::write(path, self.serialize())
            .map_err(|e| ParseError::ParseError(format!("Problem saving dates map due to {e}")))
    }
}

pub(crate) async fn parse_date<P: JsonRpcClient>(
    date: &NaiveDate,
    fetcher: Arc<Fetcher<P>>,
) -> Result<Vec<(Chunk, Option<String>)>, ParseError> {
    let dates_map_path = PathBuf::from("./aux/dates_map.toml");
    let mut dates_map = DateBlocksMap::default();
    dates_map.load_from(&dates_map_path)?;
    let bounds = dates_map.calculate_bounds(*date, &fetcher).await.map_err(|e| {
        ParseError::ParseError(format!("Failed to calculate bounds for date due to {e}"))
    })?;
    dates_map.save_to(&dates_map_path)?;
    Ok(bounds.map(|range| vec![(Chunk::Block(range), None)]).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dates_map_serde() {
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

    #[test]
    fn test_dates_map_range_bounds() -> Result<(), chrono::ParseError> {
        let mut dates = DateBlocksMap::default();

        assert_eq!((0, u64::MAX), dates.current_date_start_block_range(&("2023-08-01".parse()?)));
        assert_eq!((0, u64::MAX), dates.current_date_end_block_range(&("2023-08-01".parse()?)));

        dates.mark_block_date(10000, "2023-08-01".parse()?);
        dates.mark_block_date(10005, "2023-08-01".parse()?);
        dates.mark_block_date(10007, "2023-08-05".parse()?);
        dates.mark_block_date(10010, "2023-08-05".parse()?);
        dates.mark_block_date(10011, "2023-08-10".parse()?);
        dates.mark_block_date(10016, "2023-08-10".parse()?);
        dates.mark_block_date(10200, "2023-08-15".parse()?);
        dates.mark_block_date(10210, "2023-08-15".parse()?);

        assert_eq!((0, 10000), dates.current_date_start_block_range(&("2023-08-01".parse()?)));
        assert_eq!((10005, 10007), dates.current_date_end_block_range(&("2023-08-01".parse()?)));
        assert_eq!((10005, 10007), dates.current_date_start_block_range(&("2023-08-03".parse()?)));
        assert_eq!((10005, 10007), dates.current_date_end_block_range(&("2023-08-03".parse()?)));
        assert_eq!((10005, 10007), dates.current_date_start_block_range(&("2023-08-05".parse()?)));
        assert_eq!((10010, 10011), dates.current_date_end_block_range(&("2023-08-05".parse()?)));
        assert_eq!((10010, 10011), dates.current_date_start_block_range(&("2023-08-07".parse()?)));
        assert_eq!((10010, 10011), dates.current_date_end_block_range(&("2023-08-07".parse()?)));
        assert_eq!((10010, 10011), dates.current_date_start_block_range(&("2023-08-10".parse()?)));
        assert_eq!((10016, 10200), dates.current_date_end_block_range(&("2023-08-10".parse()?)));
        assert_eq!((10016, 10200), dates.current_date_start_block_range(&("2023-08-15".parse()?)));
        assert_eq!((10210, u64::MAX), dates.current_date_end_block_range(&("2023-08-15".parse()?)));

        Ok(())
    }
}
