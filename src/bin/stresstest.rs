extern crate regex;
extern crate crypto;
extern crate dedup;

use std::io;
use std::io::prelude::*;
use std::process::{Command, Stdio};
use std::collections::HashMap;
use regex::Regex;
use crypto::digest::Digest;
use crypto::sha2::Sha256;
use dedup::store::Store;

fn sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.input(data);
    hasher.result_str()
}

fn main() {
    let mut store = Store::new(1024);
    let mut hashes: HashMap<String, String> = HashMap::new();
    let re = Regex::new(r"^([^:]*):\s*(.*)$").unwrap();
    let stdin = io::stdin();
    println!("name                       new (bytes / chunks)  dup (bytes / chunks)");
    for line in stdin.lock().lines() {
        let uline = line.unwrap();
        let cap = re.captures(&uline).unwrap();
        let (name, cmd) = (&cap[1], &cap[2]);
        let mut child =
            Command::new("sh").arg("-c").arg(cmd)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut data = vec!();
        child.stdout.take().unwrap().read_to_end(&mut data).unwrap();
        assert!(child.wait().unwrap().success());
        let stats = store.save(name, &data);
        println!("{:24} {:12} / {:<6} {:12} / {:<6} fp={}",
            name,
            stats.new_bytes, stats.new_blocks,
            stats.dup_bytes, stats.dup_blocks,
            stats.roll_false,
        );
        hashes.insert(name.to_string(), sha256(&data));
    }
    for (name, hash) in &hashes {
        let stored_hash = sha256(&store.load(name));
        assert_eq!(*hash, *stored_hash);
    }
}
