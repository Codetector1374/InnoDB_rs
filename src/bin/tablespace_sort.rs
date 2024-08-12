use clap::Parser;
use innodb::innodb::page::{Page, PageType, FIL_PAGE_SIZE};
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use tracing::{info, warn, Level};

#[derive(Parser, Debug)]
struct Arguments {
    #[arg(short='n', long="dry-run", action = clap::ArgAction::SetTrue)]
    dry_run: bool,

    #[arg(long="no-color", action = clap::ArgAction::SetFalse)]
    color: bool,

    #[arg(short='v', action = clap::ArgAction::Count, help="verbose level")]
    verbose: u8,

    file: PathBuf,
    output: PathBuf,
}

const ZEROS_BUFFER: [u8; FIL_PAGE_SIZE] = [0u8; FIL_PAGE_SIZE];

fn main() {
    let args = Arguments::parse();

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(match args.verbose {
            0 => Level::INFO,
            1 => Level::DEBUG,
            _ => Level::TRACE,
        })
        .with_ansi(args.color)
        .finish();
    _ = tracing::subscriber::set_global_default(subscriber);

    let file = File::open(args.file).expect("Failed to open input file");

    let mut output_len: usize = 0;
    let mut output_opt = if args.dry_run {
        None
    } else {
        Some(File::create(args.output).expect("Failed to open output file for write"))
    };

    let mut reader = BufReader::new(file);
    let mut page_buffer: Vec<u8> = Vec::new();
    page_buffer.resize(FIL_PAGE_SIZE, 0);

    let mut pages_processed = 0u32;
    let mut largest_page_number = 0u32;
    let mut sorted = true;

    loop {
        match reader.read_exact(&mut page_buffer) {
            Ok(_) => {
                pages_processed += 1;

                let page = Page::from_bytes(&page_buffer).expect("Failed to construct page");
                // only allocated page is empty
                if page.header.page_type == PageType::Allocated {
                    continue;
                }

                if page.crc32_checksum() != page.header.new_checksum {
                    warn!("Invalid page detected: {:?}", page)
                } else {
                    largest_page_number = std::cmp::max(largest_page_number, page.header.offset);
                }

                if page.header.offset != (pages_processed - 1) {
                    sorted = false;
                }

                let page_offset_in_file = page.header.offset as usize * FIL_PAGE_SIZE;

                if let Some(output) = output_opt.as_mut() {
                    // If the target file is "shorter" than where we need to write, fill it with zeros
                    while output_len < page_offset_in_file {
                        output
                            .seek(SeekFrom::Start(output_len as u64))
                            .expect("Seek success");
                        output
                            .write_all(&ZEROS_BUFFER)
                            .expect("Failed to write spacer");
                        output_len += ZEROS_BUFFER.len();
                    }

                    debug_assert!((page_offset_in_file == output_len)
                              || (page_offset_in_file + FIL_PAGE_SIZE < output_len),
                              "either we should be tacking on at the end, or completely within the current file");
                    output
                        .seek(SeekFrom::Start(page_offset_in_file as u64))
                        .expect("Failed to seek to page location");
                    output
                        .write_all(&page_buffer)
                        .expect("Failed to write page data");
                    if page_offset_in_file == output_len {
                        output_len += page_buffer.len();
                    }

                    debug_assert!(
                        output_len % FIL_PAGE_SIZE == 0,
                        "output must be page aligned"
                    );
                }
            }
            Err(_) => break,
        }
    }

    info!("Processed {} pages, max page number is {}", pages_processed, largest_page_number);
    info!("Original file is sorted = {:?}", sorted);
}
