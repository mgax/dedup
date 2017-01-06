use std::collections::HashMap;
use std::collections::HashSet;
use std::io;
use std::io::{Read, Write};
use crypto::digest::Digest;
use crypto::sha2::Sha256;
use adler32::RollingAdler32;

pub struct Repo {
    block_size: usize,
    blocks: HashMap<Vec<u8>, Vec<u8>>,
    matches: HashSet<u32>,
    files: HashMap<String, Vec<Vec<u8>>>,
}

#[derive(Debug)]
pub enum SaveError {
    Io(io::Error),
}

#[derive(Debug)]
pub struct NotFoundError {}

#[derive(Debug)]
pub struct CorruptDatabaseError {}

#[derive(Debug)]
pub enum LoadError {
    Io(io::Error),
    NotFound(NotFoundError),
    CorruptDatabase(CorruptDatabaseError),
}

impl Repo {
    pub fn new(block_size: usize) -> Repo {
        Repo{
            block_size: block_size,
            blocks: HashMap::new(),
            matches: HashSet::new(),
            files: HashMap::new(),
        }
    }

    pub fn save(&mut self, key: &str, reader: &mut Read) -> Result<Stats, SaveError> {
        let (block_keys, stats) = try!(Deduplicator::store(reader, self));
        self.files.insert(key.to_string(), block_keys);
        return Ok(stats);
    }

    pub fn load(&self, key: &str, writer: &mut Write) -> Result<(), LoadError> {
        let not_found = LoadError::NotFound(NotFoundError{});
        for block_key in try!(self.files.get(key).ok_or(not_found)).iter() {
            let corrupt_db = LoadError::CorruptDatabase(CorruptDatabaseError{});
            let block = try!(self.blocks.get(block_key).ok_or(corrupt_db));
            try!(writer.write(&block).map_err(LoadError::Io));
        }
        Ok(())
    }
}

struct Deduplicator<'a, 'b> {
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
        if ! self.repo.blocks.contains_key(&block_key) {
            if block_len == self.repo.block_size {
                let rollhash = RollingAdler32::from_buffer(&block).hash();
                self.repo.matches.insert(rollhash);
            }
            self.repo.blocks.insert(block_key.clone(), block);
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
        let block_size = self.repo.block_size;
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
                if self.repo.matches.contains(&roll.hash()) {
                    let offset = buffer.len() - block_size;
                    let hash = self.hash(&buffer[offset ..]);
                    if self.repo.blocks.contains_key(&hash) {
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

    pub fn store(
        reader: &'a mut Read,
        repo: &'b mut Repo,
    ) -> Result<(Vec<Vec<u8>>, Stats), SaveError> {
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
        return Ok((deduplicator.block_keys, deduplicator.stats));
    }
}

pub struct Stats {
    pub dup_blocks: u32,
    pub dup_bytes: usize,
    pub new_blocks: u32,
    pub new_bytes: usize,
    pub roll_false: u32,
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    fn load(repo: &super::Repo, name: &str) -> Vec<u8> {
        let mut cursor = Cursor::new(vec!());
        repo.load(name, &mut cursor).unwrap();
        return cursor.into_inner();
    }

    #[test]
    fn single_small_file() {
        let mut repo = super::Repo::new(4);
        let fox = "the quick brown fox jumps over the lazy dog".as_bytes();
        repo.save("fox", &mut Cursor::new(fox)).unwrap();
        assert_eq!(load(&repo, "fox"), fox);
    }

    #[test]
    fn two_small_files() {
        let mut repo = super::Repo::new(4);
        let fox_one = "the quick brown fox jumps over the lazy dog".as_bytes();
        let fox_two = "the qqq brown rabbit jumpd over the lazy dog".as_bytes();
        repo.save("fox_one", &mut Cursor::new(fox_one)).unwrap();
        repo.save("fox_two", &mut Cursor::new(fox_two)).unwrap();
        assert_eq!(load(&repo, "fox_one"), fox_one);
        assert_eq!(load(&repo, "fox_two"), fox_two);
    }

    #[test]
    fn not_found_error() {
        let repo = super::Repo::new(4);
        let rv = repo.load("no such file", &mut Cursor::new(vec!()));
        match rv {
            Err(super::LoadError::NotFound(super::NotFoundError{})) => (),
            _ => panic!("should fail with NotFoundError"),
        };
    }

    #[test]
    fn corrupt_db_error() {
        let mut repo = super::Repo::new(4);
        let fox = "the quick brown fox jumps over the lazy dog".as_bytes();
        repo.save("fox", &mut Cursor::new(fox)).unwrap();
        repo.blocks.clear();
        let rv = repo.load("fox", &mut Cursor::new(vec!()));
        match rv {
            Err(super::LoadError::CorruptDatabase(super::CorruptDatabaseError{})) => (),
            _ => panic!("should fail with CorruptDatabase error"),
        };
    }
}
