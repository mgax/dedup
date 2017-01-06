extern crate crypto;
extern crate adler32;

pub mod errors;
mod memory_repo;
mod deduplicator;

pub use memory_repo::MemoryRepo;
pub use errors::{SaveError, LoadError};

use std::io::{Read, Write};

pub trait Repo {
    fn block_size(&self) -> usize;
    fn write_hash(&mut self, hash: u32);
    fn contains_hash(&self, hash: u32) -> bool;
    fn write_block(&mut self, block_id: &Vec<u8>, block: Vec<u8>);
    fn contains_block(&self, block_id: &Vec<u8>) -> bool;
    fn read_block(&self, block_id: &Vec<u8>) -> Result<&Vec<u8>, LoadError>;
    fn write_file(&mut self, name: &str, block_ids: Vec<Vec<u8>>);
    fn read_file(&self, name: &str) -> Result<&Vec<Vec<u8>>, LoadError>;
}

pub struct Stats {
    pub dup_blocks: u32,
    pub dup_bytes: usize,
    pub new_blocks: u32,
    pub new_bytes: usize,
    pub roll_false: u32,
}

pub fn save(repo: &mut Repo, name: &str, reader: &mut Read) -> Result<Stats, SaveError> {
    deduplicator::Deduplicator::store(repo, name, reader)
}

pub fn load(repo: &Repo, name: &str, writer: &mut Write) -> Result<(), LoadError> {
    for block_key in try!(repo.read_file(name)).iter() {
        let block = try!(repo.read_block(block_key));
        try!(writer.write(&block).map_err(LoadError::Io));
    }
    Ok(())
}
