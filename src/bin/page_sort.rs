use std::{fs::File, io::{Error, Seek}, os::windows::fs::FileExt, path::PathBuf};

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
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
        help = "Pages file, should contain one or multiple raw 16K page",
        value_name = "PAGES_FILE"
    )]
    input: PathBuf
}

fn arr2int(buf:&[u8; 4]) -> u32{
    ((buf[0] as u32) << 24) | ((buf[1] as u32) << 16) | ((buf[2] as u32) << 8) | (buf[3] as u32)
}

fn get_page_number(pages: &File, offset: u64) -> Result<u64, Error>{
    let mut buffer = [0; 4];
    pages.seek_read(&mut buffer, offset + 4)?;
    Ok(arr2int(&buffer) as u64)
}

fn copy_page(source: &File, destination: &File, source_offset: u64, destination_offset: u64) -> Result<(), Error>{
    let mut buffer = [0; 4096];
    let mut destination_offset = destination_offset;
    let mut source_offset = source_offset;
    loop {
        let bytes_read = source.seek_read(&mut buffer, source_offset)?;
        if bytes_read == 0 {
            break;
        }
        destination.seek_write(&buffer, destination_offset)?;
        destination_offset += bytes_read as u64;
        source_offset += bytes_read as u64;
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
    let mut input_file = File::open(args.input).expect("Failed to open input file");

    let total_bytes = input_file.seek(std::io::SeekFrom::End(0)).expect("Failed to get input file size");
    let total_pages = total_bytes / PAGE_SIZE as u64;
    let mut success: usize = 0;

    let process_bar = if args.verbose > 0 {
        Some(ProgressBar::new(total_bytes as u64))
    } else {
        None
    };

    if let Some(process_bar) = &process_bar {
        process_bar.set_style(
            ProgressStyle::with_template(
                "[{eta}] [{bar:40}] ({bytes_per_sec}) {bytes}/{total_bytes} {msg}",
            )
            .unwrap()
            .progress_chars("=> "),
        );
    }

    for i in 0..total_pages{
        let offset = i * PAGE_SIZE as u64;
        let page_number = match get_page_number(&input_file, offset){
            Ok(page_number) => page_number,
            Err(err) => {
                warn!("Failed to get page number of page {}: {}", i + 1, err);
                continue;
            }
        };
        let destination_offset = page_number * PAGE_SIZE as u64;
        match copy_page(&input_file, &output_file, offset, destination_offset){
            Ok(_) => {},
            Err(err) => {
                warn!("Failed to copy page {}: {}", i + 1, err);
                continue;
            }
        }
        success += 1;
        if let Some(process_bar) = &process_bar{
            process_bar.inc(PAGE_SIZE as u64);
        }
    }
    info!("Successfully sorted {} pages of {} pages", success, total_pages);

    // for entry in entries {
    //     let path = match entry {
    //         Ok(entry) => entry.path(),
    //         Err(entry) => {
    //             warn!("Failed to read entry: {}", entry);
    //             continue;
    //         }
    //     };
    //     if path.is_dir(){
    //         continue;
    //     }
    //     match path.extension(){
    //         None => continue,
    //         Some(ext) if ext != "page" => continue,
    //         Some(_) => {}
    //     };

    //     let offset = match get_page_number(&path){
    //         Ok(page_number) => page_number * PAGE_SIZE as u64,
    //         Err(err) => {
    //             warn!("Failed to get page number: {}", err);
    //             continue;
    //         }
    //     };

    //     match copy_page(&path, &output_file, offset){
    //         Ok(_) => {},
    //         Err(err) => {
    //             warn!("Failed to copy page: {}", err);
    //             continue;
    //         }
    //     }
        
    //     success += 1;
    //     if let Some(process_bar) = &process_bar{
    //         process_bar.inc(1);
    //     }
    // }
}