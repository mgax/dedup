use std::collections::HashMap;

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
        let mut deduplicator = Deduplicator::new(self.block_size, data);
        let block_keys = deduplicator.store(&mut self.blocks);
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

struct Deduplicator<'a> {
    block_size: usize,
    data: &'a [u8],
}

impl<'a> Deduplicator<'a> {
    pub fn new(block_size: usize, data: &'a [u8]) -> Deduplicator {
        Deduplicator{
            block_size: block_size,
            data: data,
        }
    }

    pub fn store(&mut self, blocks: &mut HashMap<Vec<u8>, Vec<u8>>) -> Vec<Vec<u8>> {
        let mut block_keys: Vec<Vec<u8>> = Vec::new();
        let mut start = 0;
        let mut size = 1;

        loop {
            if size == self.block_size {
                let block = self.data[start .. start + size].to_vec();
                let block_key = block.clone();
                blocks.insert(block_key.clone(), block);
                block_keys.push(block_key);
                start += size;
                size = 0;
            }
            if start + size == self.data.len() {
                if size > 0 {
                    let block = self.data[start .. start + size].to_vec();
                    let block_key = block.clone();
                    blocks.insert(block_key.clone(), block);
                    block_keys.push(block_key);
                    start += size;
                    size = 0;
                }
                break;
            }

            size += 1;
            assert!(size <= self.block_size);
            assert!(start + size <= self.data.len());
        }
        assert!(size == 0);
        assert!(start == self.data.len());

        return block_keys;
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
