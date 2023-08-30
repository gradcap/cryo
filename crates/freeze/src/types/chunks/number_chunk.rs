use ethers::types::FilterBlockOption;

use super::chunk_ops::ChunkData;

/// Chunk of blocks
#[derive(Debug, Clone)]
pub enum NumberChunk {
    /// Vec of block numbers
    Numbers(Vec<u64>),

    /// Range of block numbers
    Range(u64, u64),

    /// Range of block numbers known to be within UTC date
    RangeForDate(u64, u64, chrono::NaiveDate),
}

impl ChunkData for NumberChunk {
    type Inner = u64;

    fn format_item(value: Self::Inner) -> String {
        format!("{:0>8}", value)
    }

    fn size(&self) -> u64 {
        match self {
            Self::Numbers(numbers) => numbers.len() as u64,
            Self::Range(start, end) | Self::RangeForDate(start, end, _) => end - start + 1,
        }
    }

    fn min_value(&self) -> Option<Self::Inner> {
        match self {
            Self::Numbers(numbers) => numbers.iter().min().cloned(),
            Self::Range(start, _) | Self::RangeForDate(start, _, _) => Some(*start),
        }
    }

    fn max_value(&self) -> Option<Self::Inner> {
        match self {
            Self::Numbers(numbers) => numbers.iter().max().cloned(),
            Self::Range(_, end) | Self::RangeForDate(_, end, _) => Some(*end),
        }
    }

    fn date(&self) -> Option<&chrono::NaiveDate> {
        match self {
            Self::RangeForDate(_, _, date) => Some(date),
            Self::Numbers(_) | Self::Range(_, _) => None,
        }
    }
}

impl NumberChunk {
    /// get list of block numbers in chunk
    pub fn numbers(&self) -> Vec<u64> {
        match self {
            NumberChunk::Numbers(numbers) => numbers.to_vec(),
            NumberChunk::Range(start, end) | NumberChunk::RangeForDate(start, end, _) => {
                (*start..=*end).collect()
            }
        }
    }

    /// convert block range to a list of Filters for get_logs()
    pub fn to_log_filter_options(&self, log_request_size: &u64) -> Vec<FilterBlockOption> {
        match self {
            Self::Numbers(block_numbers) => block_numbers
                .iter()
                .map(|block| FilterBlockOption::Range {
                    from_block: Some((*block).into()),
                    to_block: Some((*block).into()),
                })
                .collect(),
            Self::Range(start_block, end_block) | Self::RangeForDate(start_block, end_block, _) => {
                let chunks = range_to_chunks(start_block, &(end_block + 1), log_request_size);
                chunks
                    .iter()
                    .map(|(start, end)| FilterBlockOption::Range {
                        from_block: Some((*start).into()),
                        to_block: Some((*end).into()),
                    })
                    .collect()
            }
        }
    }

    /// align boundaries of chunk to clean boundaries
    pub fn align(self, chunk_size: u64) -> Option<Self> {
        match self {
            Self::Numbers(numbers) => Some(Self::Numbers(numbers)),
            Self::Range(start, end) => {
                let start = ((start + chunk_size - 1) / chunk_size) * chunk_size;
                let end = (end / chunk_size) * chunk_size;
                if end > start {
                    Some(Self::Range(start, end))
                } else {
                    None
                }
            }
            Self::RangeForDate(start, end, date) => {
                let start = ((start + chunk_size - 1) / chunk_size) * chunk_size;
                let end = (end / chunk_size) * chunk_size;
                if end > start {
                    Some(Self::RangeForDate(start, end, date))
                } else {
                    None
                }
            }
        }
    }
}

pub(crate) fn range_to_chunks(start: &u64, end: &u64, chunk_size: &u64) -> Vec<(u64, u64)> {
    let mut chunks = Vec::new();
    let mut chunk_start = *start;
    while chunk_start < *end {
        let chunk_end = (chunk_start + chunk_size).min(*end) - 1;
        chunks.push((chunk_start, chunk_end));
        chunk_start += chunk_size;
    }
    chunks
}
