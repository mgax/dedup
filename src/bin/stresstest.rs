extern crate regex;
extern crate dedup;

use std::io;
use std::io::prelude::*;
use std::process::Command;
use regex::Regex;
use dedup::store::Store;

fn main() {
    let mut store = Store::new(1024);
    let re = Regex::new(r"^([^:]*):\s*(.*)$").unwrap();
    let stdin = io::stdin();
    println!("name                       new (bytes / chunks)  dup (bytes / chunks)");
    for line in stdin.lock().lines() {
        let uline = line.unwrap();
        let cap = re.captures(&uline).unwrap();
        let (name, cmd) = (&cap[1], &cap[2]);
        let output = Command::new("sh").arg("-c").arg(cmd).output().unwrap();
        let stats = store.save(name, output.stdout.as_slice());
        println!("{:24} {:12} / {:<6} {:12} / {:<6} fp={}",
            name,
            stats.new_bytes, stats.new_blocks,
            stats.dup_bytes, stats.dup_blocks,
            stats.roll_false,
        );
    }
}
