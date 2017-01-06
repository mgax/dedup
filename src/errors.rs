use std::io;

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
