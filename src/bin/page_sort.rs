use std::{fs::{self, rename, File}, io::{Error, Read, Seek}, path::PathBuf};

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
        help = "Page(s) file directory, should contain one or multiple raw 16K page",
        value_name = "PAGE_FILE_DIR"
    )]
    directory: PathBuf
}

fn arr2int(buf:&[u8; 4]) -> u32{
    ((buf[0] as u32) << 24) | ((buf[1] as u32) << 16) | ((buf[2] as u32) << 8) | (buf[3] as u32)
}

fn get_page_number(path: &PathBuf) -> Result<u32, Error>{
    File::open(path)
        .and_then(|mut file| {
            match file.seek(std::io::SeekFrom::Start(0x4)){
                Ok(_) => Ok(file),
                Err(err) => {
                    Err(err)
                }
            }
        })
        .and_then(|mut file| {
            let mut buffer = [0; 4];
            match file.read_exact(&mut buffer){
                Ok(_) => Ok(arr2int(&buffer)),
                Err(err) => {
                    Err(err)
                }
            }
        })
}

fn get_file_count(path: PathBuf) -> Result<usize, Error>{
    fs::read_dir(path)
        .and_then(|entries| {
            Ok(entries.count())
        })
}

fn main(){
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

        let page_number = match get_page_number(&path){
            Ok(page_number) => page_number,
            Err(err) => {
                warn!("Failed to get page number: {}", err);
                continue;
            }
        };
        
        let new_name = format!("{}/sorted_{:0>10}.page", path.parent().unwrap().to_str().unwrap(), page_number);

        match rename(path, new_name){
            Ok(_) => {},
            Err(err) => {
                warn!("Failed to rename: {}", err);
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