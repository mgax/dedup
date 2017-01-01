use std::collections::HashMap;

fn main() {
    println!("Hello, world!");
}

struct Store {
    blocks: HashMap<String, Vec<u8>>,
}

impl Store {
    pub fn new() -> Store {
        Store{blocks: HashMap::new()}
    }

    pub fn save(&mut self, key: &str, data: &[u8]) {
        self.blocks.insert(key.to_string(), data.to_vec());
    }

    pub fn load(&self, key: &str) -> Vec<u8> {
        self.blocks.get(key).unwrap().to_vec()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn single_small_file() {
        let mut store = super::Store::new();
        let fox = "the quick brown fox jumps over the lazy dog".as_bytes();
        store.save("fox", fox);
        assert_eq!(store.load("fox"), fox);
    }
}
