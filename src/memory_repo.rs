use std::collections::HashMap;
use std::collections::HashSet;
use errors::{LoadError, NotFoundError, CorruptDatabaseError};
use super::Repo;

pub struct MemoryRepo {
    block_size: usize,
    blocks: HashMap<Vec<u8>, Vec<u8>>,
    matches: HashSet<u32>,
    files: HashMap<String, Vec<Vec<u8>>>,
}

impl Repo for MemoryRepo {
    fn block_size(&self) -> usize {
        self.block_size
    }

    fn write_hash(&mut self, hash: u32) {
        self.matches.insert(hash);
    }

    fn contains_hash(&self, hash: u32) -> bool {
        self.matches.contains(&hash)
    }

    fn write_block(&mut self, block_id: &Vec<u8>, block: Vec<u8>) {
        self.blocks.insert(block_id.clone(), block);
    }

    fn contains_block(&self, block_id: &Vec<u8>) -> bool {
        self.blocks.contains_key(block_id)
    }

    fn read_block(&self, block_id: &Vec<u8>) -> Result<&Vec<u8>, LoadError> {
        self.blocks.get(block_id).ok_or(LoadError::CorruptDatabase(CorruptDatabaseError{}))
    }

    fn write_file(&mut self, name: &str, block_ids: Vec<Vec<u8>>) {
        self.files.insert(name.to_string(), block_ids);
    }

    fn read_file(&self, name: &str) -> Result<&Vec<Vec<u8>>, LoadError> {
        self.files.get(name).ok_or(LoadError::NotFound(NotFoundError{}))
    }
}

impl MemoryRepo {
    pub fn new(block_size: usize) -> MemoryRepo {
        MemoryRepo{
            block_size: block_size,
            blocks: HashMap::new(),
            matches: HashSet::new(),
            files: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::testsuite as ts;
    use super::MemoryRepo;

    #[test]
    fn single_small_file() {
        ts::single_small_file(&mut MemoryRepo::new(4));
    }

    #[test]
    fn two_small_files() {
        ts::two_small_files(&mut MemoryRepo::new(4));
    }

    #[test]
    fn not_found_error() {
        ts::not_found_error(&mut MemoryRepo::new(4));
    }

    #[test]
    fn corrupt_db_error() {
        fn break_repo(repo: &mut MemoryRepo) { repo.blocks.clear() }
        ts::corrupt_db_error(&mut MemoryRepo::new(4), break_repo);
    }
}
