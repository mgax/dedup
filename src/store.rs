use std::collections::HashMap;
use std::collections::HashSet;
use crypto::digest::Digest;
use crypto::sha2::Sha256;
use adler32::RollingAdler32;

pub struct Store {
    block_size: usize,
    blocks: HashMap<Vec<u8>, Vec<u8>>,
    matches: HashSet<u32>,
    files: HashMap<String, Vec<Vec<u8>>>,
}

impl Store {
    pub fn new(block_size: usize) -> Store {
        Store{
            block_size: block_size,
            blocks: HashMap::new(),
            matches: HashSet::new(),
            files: HashMap::new(),
        }
    }

    pub fn save(&mut self, key: &str, data: &[u8]) -> Stats {
        let (block_keys, stats) = Deduplicator::store(
            self.block_size,
            data,
            &mut self.blocks,
            &mut self.matches,
        );
        self.files.insert(key.to_string(), block_keys);
        return stats;
    }

    pub fn load(&self, key: &str) -> Vec<u8> {
        let mut out: Vec<u8> = Vec::new();
        for block_key in self.files.get(key).unwrap().iter() {
            let block = self.blocks.get(block_key).unwrap();
            out.extend(block.as_slice());
        }
        return out;
    }
}

struct Deduplicator<'a, 'b> {
    block_size: usize,
    blocks: &'b mut HashMap<Vec<u8>, Vec<u8>>,
    matches: &'b mut HashSet<u32>,
    data: &'a [u8],
    cursor: usize,
    block_keys: Vec<Vec<u8>>,
    stats: Stats,
}

impl<'a, 'b> Deduplicator<'a, 'b> {
    fn hash(&self, block: &[u8]) -> Vec<u8> {
        let mut hasher = Sha256::new();
        hasher.input(block);
        let mut hash: Vec<u8> = vec![0; 32];
        hasher.result(&mut hash);
        return hash;
    }

    fn save(&mut self, block: Vec<u8>) {
        let block_size = block.len();
        let block_key = self.hash(block.as_slice());
        if ! self.blocks.contains_key(&block_key) {
            if block.len() == self.block_size {
                let rollhash = RollingAdler32::from_buffer(block.as_slice()).hash();
                self.matches.insert(rollhash);
            }
            self.blocks.insert(block_key.clone(), block);
            self.stats.new_blocks += 1;
            self.stats.new_bytes += block_size;
        }
        else {
            self.stats.old_blocks += 1;
            self.stats.old_bytes += block_size;
        }

        self.block_keys.push(block_key);
    }

    fn flushn(&mut self, buffer: &mut Vec<u8>, size: usize) {
        if buffer.len() > size {
            let mut remainder = buffer.split_off(size);
            self.save(buffer.clone());
            buffer.clear();
            buffer.append(&mut remainder);
        }
        else {
            self.save(buffer.clone());
            buffer.clear();
        }
    }

    fn next_byte(&mut self) -> Option<u8> {
        if self.cursor < self.data.len() {
            let rv = self.data[self.cursor];
            self.cursor += 1;
            Some(rv)
        }
        else {
            None
        }
    }

    fn consume(&mut self) {
        let block_size = self.block_size;
        let mut buffer: Vec<u8> = Vec::new();
        loop {
            while buffer.len() < block_size {
                match self.next_byte() {
                    Some(byte) => {
                        buffer.push(byte);
                    },
                    None => {
                        let block = buffer.clone();
                        self.save(block);
                        return;
                    },
                }
            }

            let mut roll = RollingAdler32::from_buffer(buffer.as_slice());

            while buffer.len() < 2 * block_size {
                if self.matches.contains(&roll.hash()) {
                    let offset = buffer.len() - block_size;
                    let hash = self.hash(&buffer[offset ..]);
                    if self.blocks.contains_key(&hash) {
                        self.flushn(&mut buffer, offset);
                        break;
                    }
                }

                match self.next_byte() {
                    Some(byte) => {
                        roll.remove(self.block_size,
                            buffer[buffer.len() - self.block_size]);
                        buffer.push(byte);
                        roll.update(byte);
                    },
                    None => {
                        self.flushn(&mut buffer, block_size);
                        if buffer.len() > 0 {
                            self.flushn(&mut buffer, block_size);
                        }
                        return;
                    },
                }
            }

            self.flushn(&mut buffer, block_size);
        }
    }

    pub fn store(
        block_size: usize,
        data: &'a [u8],
        blocks: &'b mut HashMap<Vec<u8>, Vec<u8>>,
        matches: &'b mut HashSet<u32>,
    ) -> (Vec<Vec<u8>>, Stats) {
        let mut deduplicator = Deduplicator{
            block_size: block_size,
            blocks: blocks,
            matches: matches,
            data: data,
            cursor: 0,
            block_keys: Vec::new(),
            stats: Stats{
                old_blocks: 0,
                old_bytes: 0,
                new_blocks: 0,
                new_bytes: 0,
            },
        };
        deduplicator.consume();
        return (deduplicator.block_keys, deduplicator.stats);
    }
}

pub struct Stats {
    pub old_blocks: u32,
    pub old_bytes: usize,
    pub new_blocks: u32,
    pub new_bytes: usize,
}

#[cfg(test)]
mod tests {
    #[test]
    fn single_small_file() {
        let mut store = super::Store::new(4);
        let fox = "the quick brown fox jumps over the lazy dog".as_bytes();
        store.save("fox", fox);
        assert_eq!(store.load("fox"), fox);
    }

    #[test]
    fn two_small_files() {
        let mut store = super::Store::new(4);
        let fox_one = "the quick brown fox jumps over the lazy dog".as_bytes();
        let fox_two = "the qqq brown rabbit jumpd over the lazy dog".as_bytes();
        store.save("fox_one", fox_one);
        store.save("fox_two", fox_two);
        assert_eq!(store.load("fox_one"), fox_one);
        assert_eq!(store.load("fox_two"), fox_two);
    }
}
