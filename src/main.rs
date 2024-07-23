use std::{fs::File, io::Read};

use innodb::page::{Page};

pub mod innodb;
fn main() {
    let mut f = File::open("./data/page1.bin").expect("FFFFF");
    let mut v: Vec<u8> = Vec::new();
    f.read_to_end(&mut v).expect("gg no re");
    let p = Page::from_bytes(&v).expect("Failed to make page");
    println!(
        "Header: {:#X?}, \ninnodb:{:#X} crc32:{:#X}",
        p,
        p.innodb_checksum(),
        p.crc32_checksum()
    );
}
