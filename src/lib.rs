extern crate crypto;
extern crate adler32;

pub mod errors;
pub mod repo;
pub use repo::{MemoryRepo, save, load};
pub use errors::{SaveError, LoadError};
