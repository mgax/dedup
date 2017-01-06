use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Read;
use crypto::digest::Digest;
use crypto::sha2::Sha256;
use adler32::RollingAdler32;
use errors::{SaveError, LoadError, NotFoundError, CorruptDatabaseError};
use super::{Repo, Stats};

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

pub struct Deduplicator<'a, 'b> {
    reader: &'a mut Read,
    repo: &'b mut Repo,
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
        let block_len = block.len();
        if block_len == 0 { return }
        let block_key = self.hash(&block);
        if ! self.repo.contains_block(&block_key) {
            if block_len == self.repo.block_size() {
                let rollhash = RollingAdler32::from_buffer(&block).hash();
                self.repo.write_hash(rollhash);
            }
            self.repo.write_block(&block_key, block);
            self.stats.new_blocks += 1;
            self.stats.new_bytes += block_len;
        }
        else {
            self.stats.dup_blocks += 1;
            self.stats.dup_bytes += block_len;
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

    fn next_byte(&mut self) -> Result<Option<u8>, SaveError> {
        let mut buffer = [0; 1];
        let n = try!(self.reader.read(&mut buffer).map_err(SaveError::Io));
        if n > 0 {
            Ok(Some(buffer[0]))
        }
        else {
            Ok(None)
        }
    }

    fn consume(&mut self) -> Result<(), SaveError> {
        let block_size = self.repo.block_size();
        let mut buffer: Vec<u8> = Vec::new();
        loop {
            while buffer.len() < block_size {
                match try!(self.next_byte()) {
                    Some(byte) => {
                        buffer.push(byte);
                    },
                    None => {
                        let block = buffer.clone();
                        self.save(block);
                        return Ok(());
                    },
                }
            }

            let mut roll = RollingAdler32::from_buffer(&buffer);

            while buffer.len() < 2 * block_size {
                if self.repo.contains_hash(roll.hash()) {
                    let offset = buffer.len() - block_size;
                    let hash = self.hash(&buffer[offset ..]);
                    if self.repo.contains_block(&hash) {
                        self.flushn(&mut buffer, offset);
                        break;
                    }
                    else {
                        self.stats.roll_false += 1;
                    }
                }

                match try!(self.next_byte()) {
                    Some(byte) => {
                        roll.remove(block_size,
                            buffer[buffer.len() - block_size]);
                        buffer.push(byte);
                        roll.update(byte);
                    },
                    None => {
                        self.flushn(&mut buffer, block_size);
                        if buffer.len() > 0 {
                            self.flushn(&mut buffer, block_size);
                        }
                        return Ok(());
                    },
                }
            }

            self.flushn(&mut buffer, block_size);
        }
    }

    pub fn store(repo: &'b mut Repo, name: &str, reader: &'a mut Read) -> Result<Stats, SaveError> {
        let mut deduplicator = Deduplicator{
            repo: repo,
            reader: reader,
            block_keys: Vec::new(),
            stats: Stats{
                dup_blocks: 0,
                dup_bytes: 0,
                new_blocks: 0,
                new_bytes: 0,
                roll_false: 0,
            },
        };
        try!(deduplicator.consume());
        let Deduplicator{repo, stats, block_keys, ..} = deduplicator;
        repo.write_file(name, block_keys);
        return Ok(stats);
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
