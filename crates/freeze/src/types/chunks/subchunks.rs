use super::{chunk::BlockChunk, chunk_ops::ChunkData, number_chunk::range_to_chunks};

/// Aggregation operations related to chunks
pub trait Subchunk {
    /// divide into subchunks by size
    fn subchunk_by_size(&self, chunk_size: &u64) -> Vec<BlockChunk>;

    /// divide into number of subchunks
    fn subchunk_by_count(&self, n_chunks: &u64) -> Vec<BlockChunk>;
}

impl Subchunk for BlockChunk {
    fn subchunk_by_size(&self, chunk_size: &u64) -> Vec<BlockChunk> {
        if self.size() <= *chunk_size {
            return vec![self.clone()]
        }
        match &self {
            Self::Numbers(numbers) => numbers
                .chunks(*chunk_size as usize)
                .map(|chunk| Self::Numbers(chunk.to_vec()))
                .collect(),
            Self::Range(start_block, end_block) => {
                range_to_chunks(start_block, end_block, chunk_size)
                    .iter()
                    .map(|(start, end)| Self::Range(*start, *end))
                    .collect()
            }
            Self::RangeForDate(_, _, _) => panic!("Date range doesn't support subchunking"),
        }
    }

    fn subchunk_by_count(&self, n_chunks: &u64) -> Vec<BlockChunk> {
        let total_blocks = self.size();
        let chunk_size = (total_blocks + n_chunks - 1) / n_chunks;
        self.subchunk_by_size(&chunk_size)
    }
}

impl Subchunk for Vec<BlockChunk> {
    fn subchunk_by_size(&self, chunk_size: &u64) -> Vec<BlockChunk> {
        to_single_chunk(self).subchunk_by_size(chunk_size)
    }

    fn subchunk_by_count(&self, n_chunks: &u64) -> Vec<BlockChunk> {
        to_single_chunk(self).subchunk_by_count(n_chunks)
    }
}

fn to_single_chunk(chunks: &Vec<BlockChunk>) -> BlockChunk {
    match (chunks.len(), chunks.get(0)) {
        (1, Some(chunk)) => chunk.clone(),
        _ => {
            let numbers = chunks.iter().flat_map(|x| x.numbers()).collect();
            BlockChunk::Numbers(numbers)
        }
    }
}
