use std::fs::File;
use std::path::PathBuf;

use clap::Parser;
use memmap2::MmapMut;

#[derive(Parser, Debug)]
struct Arguments {
    file: PathBuf,
}

const PAGE_SIZE: usize = 16 * 1024; // 16K块大小

struct Page {
    data: [u8; PAGE_SIZE],
}

impl Page {
    fn offset(&self) -> u32 {
        let num: [u8; 4] = self.data[4..8].try_into().expect("slice with incorrect length");;
        return u32::from_be_bytes(num);
    }
}

fn main() -> std::io::Result<()> {
    let args = Arguments::parse();


    let file = File::options().read(true).write(true).open(args.file)?;

    let mmap = unsafe { MmapMut::map_mut(&file)? };

    // 不知道怎么更科学的转类型，GPT 写的，
    let pages: &mut [Page] = unsafe {
        std::slice::from_raw_parts_mut(mmap.as_ptr() as *mut Page, mmap.len() / PAGE_SIZE)
    };

    pages.sort_by_key(|f| f.offset());

    let mut missing = 0;
    let mut expected = pages[0].offset();
    for page in pages {
        missing += page.offset() - expected;
        expected = page.offset() + 1;
    }

    println!("max: {}, missing: {}", expected - 1, missing);

    // 确保所有更改被刷到磁盘
    mmap.flush()?;

    Ok(())
}
