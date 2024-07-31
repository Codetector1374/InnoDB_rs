use std::{
    fs::File,
    io::{BufReader, Read, Write},
    path::PathBuf,
};

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use innodb::innodb::page::{index::IndexHeader, Page, PageType};
use tracing::{debug, info, trace, Level};

#[derive(Parser, Debug)]
struct Arguments {
    #[arg(
        short,
        long,
        help = "Max possible size of the db, used to estimate page number"
    )]
    size: Option<usize>,

    #[arg(short='n', action = clap::ArgAction::SetTrue, help="dry run (no action is taken)")]
    dry_run: bool,

    #[arg(long="no-index-page", action = clap::ArgAction::SetFalse)]
    extract_index_pages: bool,

    #[arg(long="by-tablespace", action = clap::ArgAction::SetTrue, conflicts_with="extract_index_pages")]
    by_tablespace: bool,

    #[arg(long="no-color", action = clap::ArgAction::SetFalse)]
    color: bool,

    #[arg(short='v', action = clap::ArgAction::Count, help="verbose level")]
    verbose: u8,

    #[arg(
        short = 'o',
        long = "output",
        default_value = "output",
        help = "output directory name"
    )]
    output: PathBuf,

    file: PathBuf,
}

#[derive(Debug, Clone)]
enum PageValidationResult<'a> {
    Valid(Page<'a>),
    InvalidChecksum,
    NotAPage,
    EmptyPage,
}

fn validate_page(page: &[u8]) -> PageValidationResult {
    let page = Page::from_bytes(page).expect("Can't construct page?");
    match page.header.page_type {
        PageType::Unknown => {
            return PageValidationResult::NotAPage;
        }
        PageType::Allocated => {
            if page.header.new_checksum == 0 {
                return PageValidationResult::EmptyPage;
            }
        }
        _ => {
            if page.crc32_checksum() == page.header.new_checksum
                || page.innodb_checksum() == page.header.new_checksum
            {
                return PageValidationResult::Valid(page);
            } else if (page.header.lsn as u32) == page.trailer.lsn_low_32 {
                return PageValidationResult::InvalidChecksum;
            }
        }
    };

    trace!("Bad page: {:#?}", page);
    return PageValidationResult::NotAPage;
}

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

    let output_index = args.output.join("FIL_PAGE_INDEX");
    let output_blob = args.output.join("FIL_PAGE_TYPE_BLOB");
    let output_by_tablespace = args.output.join("BY_TABLESPACE");
    if !args.dry_run {
        if args.by_tablespace {
            std::fs::create_dir_all(&output_by_tablespace)
                .expect("Failed to create output directory");
            if output_by_tablespace.read_dir().unwrap().next().is_some() {
                panic!(
                    "Output directory is not empty: {}",
                    output_blob.to_str().unwrap()
                );
            }
        } else {
            if args.extract_index_pages {
                std::fs::create_dir_all(&output_index).expect("Failed to create output directory");
                if output_index.read_dir().unwrap().next().is_some() {
                    panic!("{} is not empty!", output_index.to_str().unwrap());
                }
            }

            std::fs::create_dir_all(&output_blob).expect("Failed to create output directory");
            if output_blob.read_dir().unwrap().next().is_some() {
                panic!(
                    "Output directory is not empty: {}",
                    output_blob.to_str().unwrap()
                );
            }
        }
    }

    let file = File::open(args.file).expect("Can't open provided file");
    let metadata = file.metadata().expect("No metadata?");

    let pb: Option<ProgressBar> = if args.verbose == 0 {
        Some(ProgressBar::new(metadata.len()))
    } else {
        None
    };

    if let Some(pb) = &pb {
        pb.set_style(
            ProgressStyle::with_template(
                "[{eta}] [{bar:40}] ({bytes_per_sec}) {bytes}/{total_bytes} {msg}",
            )
            .unwrap()
            .progress_chars("=> "),
        );
    }

    let mut reader = BufReader::new(file);

    let mut valid_counter = 0usize;
    let mut valid_index_counter = 0usize;
    let mut failed_checksum = 0usize;

    #[allow(clippy::identity_op)]
    const CACHE_BUFFER_MAX_SIZE: usize = 1 * 1024 * 1024;
    const STEP_SIZE: usize = 4096;
    const PAGE_SIZE: usize = 16384;

    let mut buffer = Vec::new();
    let mut head_pointer: usize = 0;
    loop {
        let mut step_size = STEP_SIZE;
        if (buffer.len() - head_pointer) < PAGE_SIZE {
            buffer.drain(0..head_pointer);
            head_pointer = 0;
            let current_len = buffer.len();
            buffer.resize(CACHE_BUFFER_MAX_SIZE, 0);
            match reader.read(&mut buffer[current_len..]) {
                Ok(bytes) => {
                    if bytes == 0 {
                        break;
                    }
                    buffer.resize(current_len + bytes, 0)
                }
                Err(_) => break,
            }
            continue;
        }

        match validate_page(&buffer[head_pointer..][..PAGE_SIZE]) {
            PageValidationResult::Valid(page) => {
                trace!("Page validated {page:x?}");
                valid_counter += 1;

                // Handling is differnt if we are only grouping by table space
                if args.by_tablespace {
                    if !args.dry_run {
                        let save_path =
                            output_by_tablespace.join(format!("{:08}.pages", page.header.space_id));
                        let mut f = File::options()
                            .append(true)
                            .create(true)
                            .open(save_path)
                            .expect("Can't open file to save pages");
                        assert_eq!(
                            f.write(page.raw_data).expect("Failed to write"),
                            page.raw_data.len()
                        );
                    }
                } else {
                    // Not by table space
                    match page.header.page_type {
                        PageType::Index => {
                            let index_header = IndexHeader::from_bytes(page.body()).unwrap();
                            trace!("Index: {index_header:?}");
                            if !args.dry_run && args.extract_index_pages {
                                let save_path = output_index
                                    .join(format!("{:016}.page", index_header.index_id));
                                let mut f = File::options()
                                    .append(true)
                                    .create(true)
                                    .open(save_path)
                                    .expect("Can't open file to save pages");
                                assert_eq!(
                                    f.write(page.raw_data).expect("Failed to write"),
                                    page.raw_data.len()
                                );
                            }
                            valid_index_counter += 1;
                        }
                        _ => {
                            debug!("Unprocessed page type: {:?}", page.header.page_type);
                        }
                    }
                }
                step_size = PAGE_SIZE;
            }
            PageValidationResult::InvalidChecksum => {
                failed_checksum += 1;
            }
            PageValidationResult::NotAPage |
            PageValidationResult::EmptyPage => {}
        }

        head_pointer += step_size;
        if let Some(b) = pb.as_ref() {
            b.inc(step_size as u64)
        }
    }

    info!("found {valid_counter} pages that have valid checksum ({valid_index_counter} index pages), {failed_checksum} pages only failed checksum");
}
