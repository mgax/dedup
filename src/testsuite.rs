use std::io::Cursor;
use errors::{LoadError, NotFoundError, CorruptDatabaseError};
use super::{Repo, save, load};

fn _load(repo: &Repo, name: &str) -> Vec<u8> {
    let mut cursor = Cursor::new(vec!());
    load(repo, name, &mut cursor).unwrap();
    return cursor.into_inner();
}

pub fn single_small_file(repo: &mut Repo) {
    let fox = "the quick brown fox jumps over the lazy dog".as_bytes();
    save(repo, "fox", &mut Cursor::new(fox)).unwrap();
    assert_eq!(_load(repo, "fox"), fox);
}

pub fn two_small_files(repo: &mut Repo) {
    let fox_one = "the quick brown fox jumps over the lazy dog".as_bytes();
    let fox_two = "the qqq brown rabbit jumpd over the lazy dog".as_bytes();
    save(repo, "fox_one", &mut Cursor::new(fox_one)).unwrap();
    save(repo, "fox_two", &mut Cursor::new(fox_two)).unwrap();
    assert_eq!(_load(repo, "fox_one"), fox_one);
    assert_eq!(_load(repo, "fox_two"), fox_two);
}

pub fn not_found_error(repo: &mut Repo) {
    let rv = load(repo, "no such file", &mut Cursor::new(vec!()));
    match rv {
        Err(LoadError::NotFound(NotFoundError{})) => (),
        _ => panic!("should fail with NotFoundError"),
    };
}

pub fn corrupt_db_error<R: Repo>(repo: &mut R, break_repo: fn(&mut R)) {
    let fox = "the quick brown fox jumps over the lazy dog".as_bytes();
    save(repo, "fox", &mut Cursor::new(fox)).unwrap();
    break_repo(repo);
    let rv = load(repo, "fox", &mut Cursor::new(vec!()));
    match rv {
        Err(LoadError::CorruptDatabase(CorruptDatabaseError{})) => (),
        _ => panic!("should fail with CorruptDatabase error"),
    };
}
