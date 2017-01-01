extern crate crypto;
use std::collections::HashMap;
use crypto::digest::Digest;
use crypto::sha2::Sha256;

fn main() {
    println!("Hello, world!");
}

struct Store {
    block_size: usize,
    blocks: HashMap<Vec<u8>, Vec<u8>>,
    files: HashMap<String, Vec<Vec<u8>>>,
}

impl Store {
    pub fn new(block_size: usize) -> Store {
        Store{
            block_size: block_size,
            blocks: HashMap::new(),
            files: HashMap::new(),
        }
    }

    pub fn save(&mut self, key: &str, data: &[u8]) {
        let block_keys = Deduplicator::store(self.block_size, data, &mut self.blocks);
        self.files.insert(key.to_string(), block_keys);
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
    data: &'a [u8],
    start: usize,
    end: usize,
    block_keys: Vec<Vec<u8>>,
}

impl<'a, 'b> Deduplicator<'a, 'b> {
    fn hash(&self, block: &[u8]) -> Vec<u8> {
        let mut hasher = Sha256::new();
        hasher.input(block);
        let mut hash: Vec<u8> = vec![0; 32];
        hasher.result(&mut hash);
        return hash;
    }

    fn block(&self) -> &[u8] {
        &self.data[self.start .. self.end]
    }

    fn save_block(&mut self) {
        let block_key = self.hash(self.block());
        let block = self.block().to_vec();
        self.blocks.insert(block_key.clone(), block);
        self.block_keys.push(block_key);
        self.start = self.end;
    }

    pub fn _store(&mut self) {
        loop {
            if self.end - self.start == self.block_size {
                self.save_block();
            }
            if self.end == self.data.len() {
                if self.end > self.start {
                    self.save_block();
                }
                break;
            }

            self.end += 1;
            assert!(self.end - self.start <= self.block_size);
            assert!(self.end <= self.data.len());
        }
        assert!(self.start == self.end);
        assert!(self.start == self.data.len());
    }

    pub fn store(block_size: usize, data: &'a [u8], blocks: &'b mut HashMap<Vec<u8>, Vec<u8>>) -> Vec<Vec<u8>> {
        let mut deduplicator = Deduplicator{
            block_size: block_size,
            blocks: blocks,
            data: data,
            start: 0,
            end: 0,
            block_keys: Vec::new(),
        };
        deduplicator._store();
        return deduplicator.block_keys;
    }
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
        let fox_two = "the quack brewn fox jumped over lazy dog".as_bytes();
        store.save("fox_one", fox_one);
        store.save("fox_two", fox_two);
        assert_eq!(store.load("fox_one"), fox_one);
        assert_eq!(store.load("fox_two"), fox_two);
    }
}
