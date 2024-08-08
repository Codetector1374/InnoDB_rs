use std::{fs::{self, File}, io::{Error, Read}, os::windows::fs::FileExt, path::PathBuf};

use clap::Parser;
use indicatif::ProgressStyle;
use tracing::{info, warn};

#[derive(Parser, Debug, Clone)]
struct Arguments {
    #[arg(short='v', action = clap::ArgAction::Count)]
    verbose: u8,

    #[arg(long = "no-color", action = clap::ArgAction::SetFalse)]
    color: bool,

    #[arg(
        short = 'o',
        long = "output",
        default_value = "output",
        help = "output file name",
        required = true
    )]
    output: PathBuf,

    #[arg(
        help = "Page(s) file directory, should contain one or multiple raw 16K page",
        value_name = "PAGE_FILE_DIR"
    )]
    directory: PathBuf
}

fn arr2int(buf:&[u8; 4]) -> u32{
    ((buf[0] as u32) << 24) | ((buf[1] as u32) << 16) | ((buf[2] as u32) << 8) | (buf[3] as u32)
}

fn get_page_number(path: &PathBuf) -> Result<u64, Error>{
    let page = File::open(path)?;
    let mut buffer = [0; 4];
    page.seek_read(&mut buffer, 4)?;
    Ok(arr2int(&buffer) as u64)
}

fn get_file_count(path: PathBuf) -> Result<usize, Error>{
    fs::read_dir(path)
        .and_then(|entries| {
            Ok(entries.count())
        })
}

fn copy_page(source: &PathBuf, destination: &File, offset: u64) -> Result<(), Error>{
    let mut source_page = File::open(source)?;
    let mut buffer = [0; 4096];
    let mut offset = offset;
    loop {
        let bytes_read = source_page.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        destination.seek_write(&buffer, offset)?;
        offset += bytes_read as u64;
    }
    Ok(())
}

fn main(){
    const PAGE_SIZE: usize = 16384;
    let args = Arguments::parse();

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(match args.verbose {
            0 => tracing::Level::INFO,
            1 => tracing::Level::DEBUG,
            _ => tracing::Level::TRACE,
        })
        .with_ansi(args.color)
        .without_time()
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to setup Logger");

    let output_file = File::create_new(args.output).expect("Failed to open output file");

    let entries = fs::read_dir(args.directory.clone()).expect("Failed to read directory");
    let total = get_file_count(args.directory).expect("Failed to get file count");
    let mut success: usize = 0;

    let process_bar = if args.verbose > 0 {
        Some(indicatif::ProgressBar::new(total as u64))
    } else {
        None
    };

    if let Some(process_bar) = &process_bar{
        process_bar.set_style(
            ProgressStyle::with_template(
                "[{eta}] [{bar:40}] ({per_sec}) {human_pos}/{human_len} {msg}"
            ).unwrap()
            .progress_chars("=> ")
        );
    }

    for entry in entries {
        let path = match entry {
            Ok(entry) => entry.path(),
            Err(entry) => {
                warn!("Failed to read entry: {}", entry);
                continue;
            }
        };
        if path.is_dir(){
            continue;
        }
        match path.extension(){
            None => continue,
            Some(ext) if ext != "page" => continue,
            Some(_) => {}
        };

        let offset = match get_page_number(&path){
            Ok(page_number) => page_number * PAGE_SIZE as u64,
            Err(err) => {
                warn!("Failed to get page number: {}", err);
                continue;
            }
        };

        match copy_page(&path, &output_file, offset){
            Ok(_) => {},
            Err(err) => {
                warn!("Failed to copy page: {}", err);
                continue;
            }
        }
        
        success += 1;
        if let Some(process_bar) = &process_bar{
            process_bar.inc(1);
        }
    }
    info!("Successfully sorted {} pages of {} pages", success, total);
}