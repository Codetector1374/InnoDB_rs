use std::{
    fs::{read_to_string, File},
    io::{BufReader, Read},
    path::PathBuf,
    sync::Arc,
};

use bitvec::vec::BitVec;
use clap::Parser;
use innodb::innodb::{
    page::{
        index::{record::RecordType, IndexPage},
        Page, PageType, FIL_PAGE_SIZE,
    },
    table::{
        field::{Field, FieldType},
        row::Row,
        TableDefinition,
    },
};
use tracing::{debug, info, trace, warn, Level};

#[derive(Parser, Debug)]
struct Arguments {
    #[arg(short='v', action = clap::ArgAction::Count)]
    verbose: u8,

    #[arg(long="no-color", action = clap::ArgAction::SetFalse)]
    color: bool,

    #[arg(long)]
    limit: Option<usize>,

    #[arg(short = 't', long = "table")]
    table_def: Option<PathBuf>,

    #[arg(
        help = "Page(s) file, should contain one or multiple raw 16K page",
        value_name = "PAGE FILE"
    )]
    file: PathBuf,
}

struct PageExplorer {
    arguments: Arguments,
    table_def: Option<Arc<TableDefinition>>,
}

impl PageExplorer {
    pub fn explore_index(&self, index: &IndexPage) {
        let index_header = &index.index_header;
        debug!("Index Header:\n{:#?}", &index_header);
        let mut record = index.infimum().unwrap();
        let mut counter = 1;
        loop {
            if record.header.record_type == RecordType::Conventional {
                if let Some(table) = &self.table_def {
                    let row = Row::try_from_record_and_table(&record, table)
                        .expect("Failed to parse row");
                    trace!("{counter} Row: {:#?}", row);
                    debug!("{:?}", row.values());
                }
            } else {
                debug!("{} Record: {:?}", counter, record);
            }
            if record.header.record_type == RecordType::Supremum {
                break;
            }

            let new_rec = record.next().unwrap();
            record = new_rec;
            counter += 1;
        }
    }

    fn explore_page(&self, file_offset: usize, page: Page) {
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

        if page.header.page_type == PageType::Index {
            let index_page = IndexPage::try_from_page(page).expect("Failed to construct index");
            self.explore_index(&index_page);
        }
    }

    fn run(&self) {
        let mut reader =
            BufReader::new(File::open(&self.arguments.file).expect("Can't open page file"));
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
                    self.explore_page(cur_offset, page);
                }
                Err(e) => panic!("Read error: {:?}", e),
            }

            if let Some(limit) = self.arguments.limit {
                if counter >= limit {
                    info!("Exiting early due to --limit argument");
                    break;
                }
            }
        }
        info!("Processed {} pages", counter);
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

    let table_def: Option<Arc<TableDefinition>> = args.table_def.as_ref().map(|table_def_sql| {
        let sql = read_to_string(table_def_sql).expect("Can't load SQL file");
        let tbl = TableDefinition::try_from_sql_statement(&sql).expect("Failed parsing table");
        info!("Loaded Table:\n{:#?}", &tbl);
        Arc::new(tbl)
    });

    let explorer = PageExplorer {
        arguments: args,
        table_def: table_def,
    };

    explorer.run();
}
