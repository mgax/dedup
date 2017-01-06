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
    use std::io::Cursor;
    use errors::{LoadError, NotFoundError, CorruptDatabaseError};
    use super::super::{save, load};

    fn _load(repo: &super::MemoryRepo, name: &str) -> Vec<u8> {
        let mut cursor = Cursor::new(vec!());
        load(repo, name, &mut cursor).unwrap();
        return cursor.into_inner();
    }

    #[test]
    fn single_small_file() {
        let mut repo = super::MemoryRepo::new(4);
        let fox = "the quick brown fox jumps over the lazy dog".as_bytes();
        save(&mut repo, "fox", &mut Cursor::new(fox)).unwrap();
        assert_eq!(_load(&repo, "fox"), fox);
    }

    #[test]
    fn two_small_files() {
        let mut repo = super::MemoryRepo::new(4);
        let fox_one = "the quick brown fox jumps over the lazy dog".as_bytes();
        let fox_two = "the qqq brown rabbit jumpd over the lazy dog".as_bytes();
        save(&mut repo, "fox_one", &mut Cursor::new(fox_one)).unwrap();
        save(&mut repo, "fox_two", &mut Cursor::new(fox_two)).unwrap();
        assert_eq!(_load(&repo, "fox_one"), fox_one);
        assert_eq!(_load(&repo, "fox_two"), fox_two);
    }

    #[test]
    fn not_found_error() {
        let repo = super::MemoryRepo::new(4);
        let rv = load(&repo, "no such file", &mut Cursor::new(vec!()));
        match rv {
            Err(LoadError::NotFound(NotFoundError{})) => (),
            _ => panic!("should fail with NotFoundError"),
        };
    }

    #[test]
    fn corrupt_db_error() {
        let mut repo = super::MemoryRepo::new(4);
        let fox = "the quick brown fox jumps over the lazy dog".as_bytes();
        save(&mut repo, "fox", &mut Cursor::new(fox)).unwrap();
        repo.blocks.clear();
        let rv = load(&repo, "fox", &mut Cursor::new(vec!()));
        match rv {
            Err(LoadError::CorruptDatabase(CorruptDatabaseError{})) => (),
            _ => panic!("should fail with CorruptDatabase error"),
        };
    }
}
