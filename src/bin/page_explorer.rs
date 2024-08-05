use anyhow::Result;
use std::{
    fs::{read_to_string, File},
    io::{BufReader, Read, Write},
    path::PathBuf,
    sync::Arc,
};

use clap::Parser;
use innodb::innodb::{
    buffer_manager::{
        lru::LRUBufferManager, simple::SimpleBufferManager, BufferManager, DummyBufferMangaer,
    },
    page::{
        index::{record::RecordType, IndexPage},
        Page, PageType, FIL_PAGE_SIZE,
    },
    table::{field::FieldValue, row::Row, TableDefinition},
};
use struson::writer::{JsonStreamWriter, JsonWriter};
use tracing::{debug, info, trace, warn, Level};

#[derive(Parser, Debug, Clone)]
struct Arguments {
    #[arg(short='v', action = clap::ArgAction::Count)]
    verbose: u8,

    #[arg(long="no-color", action = clap::ArgAction::SetFalse)]
    color: bool,

    #[arg(long)]
    limit: Option<usize>,

    #[arg(long = "tablespace-dir")]
    tablespce_dir: Option<PathBuf>,

    #[arg(long = "index-id")]
    index_id: Option<u64>,

    #[arg(long = "page-id")]
    page_id: Option<u32>,

    #[arg(
        short = 't',
        long = "table",
        help = "Path to sql file containing create table statement to use as table definition for parsing"
    )]
    table_def: Option<PathBuf>,

    #[arg(short = 'o', long = "output", help = "JSON file to write output to")]
    output: Option<PathBuf>,

    #[arg(
        help = "Page(s) file, should contain one or multiple raw 16K page, ideally sorted",
        value_name = "PAGE FILE"
    )]
    file: PathBuf,
}

struct PageExplorer {
    arguments: Arguments,
    table_def: Option<Arc<TableDefinition>>,
    output_writer: Option<JsonStreamWriter<Box<dyn Write>>>,
    buffer_mgr: Box<dyn BufferManager>,
    total_records: usize,
    missing_records: usize,
    incomplete_records: usize,
}

impl PageExplorer {
    fn write_row(&mut self, values: &[FieldValue]) -> Result<()> {
        let mut has_missing = false;
        if let Some(writer) = &mut self.output_writer {
            writer.begin_object()?;

            let td = self.table_def.as_ref().unwrap();
            for (idx, col) in td
                .cluster_columns
                .iter()
                .chain(td.data_columns.iter())
                .enumerate()
            {
                writer.name(&col.name)?;
                match &values[idx] {
                    FieldValue::SignedInt(v) => writer.number_value(*v)?,
                    FieldValue::UnsignedInt(v) => writer.number_value(*v)?,
                    FieldValue::String(s) => writer.string_value(s)?,
                    FieldValue::Null => writer.null_value()?,
                    FieldValue::Skipped => {
                        has_missing = true;
                        writer.null_value()?;
                    }
                    _ => panic!("Unsupported Field Value for writing JSON"),
                };
            }
            writer.end_object()?;
        }

        if has_missing {
            self.incomplete_records += 1;
        }
        Ok(())
    }

    pub fn explore_index(&mut self, index: &IndexPage) {
        let index_header = &index.index_header;
        debug!("Inspecting Index Page {}", index.page.header.offset);
        trace!("Index Header:\n{:#?}", &index_header);
        let mut record = index.infimum().unwrap();
        let mut data_counter = 0;
        let mut other_record_counter = 0;
        loop {
            match record.header.record_type {
                RecordType::Infimum => {}
                RecordType::Supremum => {
                    break;
                }
                RecordType::Conventional => {
                    data_counter += 1;
                    if let Some(table) = &self.table_def {
                        let row = Row::try_from_record_and_table(&record, table)
                            .expect("Failed to parse row");
                        let values = row.parse_values(self.buffer_mgr.as_mut());
                        assert_eq!(values.len(), table.field_count());
                        debug!("{:?}", values);
                        self.write_row(&values).expect("Failed to write row");
                    }
                }
                RecordType::NodePointer => {
                    other_record_counter += 1;
                }
                #[allow(unreachable_patterns)]
                _ => {
                    info!("Unknown Record Type: {:?}", record);
                }
            }
            let new_rec = record.next().unwrap();
            record = new_rec;
        }
        self.total_records += data_counter;
        let missing =
            index.index_header.number_of_records as usize - data_counter - other_record_counter;
        if missing > 0 {
            self.missing_records += missing;
            warn!(
                "Missing {} records on page {}",
                missing, index.page.header.offset
            );
        }
        info!(
            "Found ({} data + {} node pointer)/{} records on index page {}",
            data_counter,
            other_record_counter,
            index.index_header.number_of_records,
            index.page.header.offset
        );
    }

    fn explore_page(&mut self, file_offset: usize, page: Page) {
        if page.header.page_type == PageType::Allocated {
            return;
        }
        if page.crc32_checksum() == page.header.new_checksum {
            trace!("Page @ {:#x} byte has valid CRC32c checksum", file_offset);
        } else if page.innodb_checksum() == page.header.new_checksum {
            trace!("Page @ {:#x} byte has valid InnoDB checksum", file_offset);
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
            PageType::Allocated => {}
            PageType::Index => {
                let index_page = IndexPage::try_from_page(page).expect("Failed to construct index");
                if let Some(filtered_index_id) = self.arguments.index_id {
                    if index_page.index_header.index_id != filtered_index_id {
                        return;
                    }
                }
                self.explore_index(&index_page);
            }
            PageType::Blob | PageType::LobFirst | PageType::LobData => {}
            _ => warn!("Unknown page type: {:?}", page.header.page_type),
        }
    }

    fn run(&mut self) {
        let mut reader =
            BufReader::new(File::open(&self.arguments.file).expect("Can't open page file"));
        let mut buffer = Box::<[u8]>::from([0u8; FIL_PAGE_SIZE]);
        let mut counter = 0usize;
        let mut index_counter = 0usize;

        if let Some(output) = &self.arguments.output {
            let file = File::create(output).expect("Can't open output file for write");
            let mut writer = JsonStreamWriter::new(Box::new(file) as Box<dyn Write>);
            writer.begin_array().expect("Can't begin array");
            self.output_writer.replace(writer);
        }

        loop {
            let cur_offset = counter * FIL_PAGE_SIZE;
            match reader.read(&mut buffer) {
                Ok(num_bytes) => {
                    if num_bytes < buffer.len() {
                        break;
                    }
                    let page = Page::from_bytes(&buffer).unwrap();
                    if page.header.page_type == PageType::Index {
                        index_counter += 1;
                    }
                    if let Some(page_id) = self.arguments.page_id {
                        if page.header.offset != page_id {
                            continue;
                        }
                    }
                    counter += 1;
                    self.explore_page(cur_offset, page);
                }
                Err(e) => panic!("Read error: {:?}", e),
            }

            if let Some(limit) = self.arguments.limit {
                if index_counter >= limit {
                    info!("Exiting early due to --limit argument");
                    break;
                }
            }
        }

        if let Some(mut writer) = self.output_writer.take() {
            writer.end_array().expect("Can't end array");
            writer.finish_document().expect("Can't finish document");
        }

        info!(
            "Processed {} pages, total records: {}, potentially missing: {}, Incomplete: {}",
            counter, self.total_records, self.missing_records, self.incomplete_records
        );
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

    let mut explorer = PageExplorer {
        arguments: args.clone(),
        table_def,
        buffer_mgr: Box::new(DummyBufferMangaer),
        output_writer: None,
        total_records: 0,
        missing_records: 0,
        incomplete_records: 0,
    };

    if let Some(tablespace) = &args.tablespce_dir {
        // explorer.buffer_mgr = Box::new(SimpleBufferManager::new(tablespace));
        explorer.buffer_mgr = Box::new(LRUBufferManager::new(tablespace));
    }

    explorer.run();
}
