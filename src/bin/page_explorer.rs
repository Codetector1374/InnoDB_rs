use std::{
    fs::File,
    io::{BufReader, Read},
    path::PathBuf,
};

use clap::Parser;
use innodb::innodb::page::{index::IndexHeader, Page, PageType, FIL_PAGE_SIZE};
use tracing::{debug, info, trace, warn, Level};

#[derive(Parser, Debug)]
struct Arguments {
    #[arg(short='v', action = clap::ArgAction::Count)]
    verbose: u8,

    #[arg(long="no-color", action = clap::ArgAction::SetFalse)]
    color: bool,

    #[arg(long)]
    limit: Option<usize>,

    #[arg(
        help = "Page(s) file, should contain one or multiple raw 16K page",
        value_name = "PAGE FILE"
    )]
    file: PathBuf,
}

fn process_page(file_offset: usize, page: Page) {
    if page.crc32_checksum() == page.header.new_checksum {
        debug!("Page @ {:#x} byte has valid CRC32c checksum", file_offset);
    } else if page.innodb_checksum() == page.header.new_checksum {
        debug!("Page @ {:#x} byte has valid InnoDB checksum", file_offset);
    } else {
        warn!(
            "Page @ {:#x} has invalid checksum: {:#08x} vs crc32: {:#08x} InnoDB: {:#08x}",
            file_offset,
            page.header.new_checksum,
            page.crc32_checksum(),
            page.innodb_checksum()
        );
        return;
    }

    trace!("{:x?}", page);

    match page.header.page_type {
        PageType::Index => {
            let index_header = IndexHeader::from_bytes(page.body()).unwrap();
            debug!("Index Header:\n{:#?}", &index_header);
        },
        _ => {}
    }
}

fn main() {
    let args = Arguments::parse();

    /* Setup Logging */
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(match args.verbose {
            0 => Level::INFO,
            1 => Level::DEBUG,
            _ => Level::TRACE,
        })
        .with_ansi(args.color)
        .without_time()
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to setup Logger");

    let mut reader = BufReader::new(File::open(&args.file).expect("Can't open page file"));
    let mut buffer = Box::<[u8]>::from([0u8; FIL_PAGE_SIZE]);
    let mut counter = 0usize;
    loop {
        let cur_offset = counter * FIL_PAGE_SIZE;
        counter += 1;
        match reader.read(&mut buffer) {
            Ok(num_bytes) => {
                if num_bytes < buffer.len() {
                    break;
                }
                let page = Page::from_bytes(&buffer).unwrap();
                process_page(cur_offset, page);
            }
            Err(e) => panic!("Read error: {:?}", e),
        }

        if let Some(limit) = args.limit {
            if counter >= limit {
                info!("Exiting early due to --limit argument");
                break;
            }
        }
    }

    info!("Processed {} pages in {:?}", counter, args.file);
}
