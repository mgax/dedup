use std::io::Read;
use crypto::digest::Digest;
use crypto::sha2::Sha256;
use adler32::RollingAdler32;
use errors::SaveError;
use super::{Repo, Stats};

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
