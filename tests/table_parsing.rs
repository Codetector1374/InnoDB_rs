use std::{
    fs::{read_to_string, File},
    io::Read,
    path::PathBuf, sync::Arc,
};

use innodb::innodb::{
    buffer_manager::DummyBufferMangaer, charset::InnoDBCharset, page::{index::{record::RecordType, IndexPage}, Page, PageType}, table::{
        field::{Field, FieldType}, row::Row, TableDefinition
    }
};

#[test]
#[ignore]
fn test_parsing_table_with_floats() {
    let sql = read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test_data")
            .join("double_test_table.sql"),
    )
    .unwrap();

    let reference = TableDefinition {
        name: String::from("float_sample"),
        cluster_columns: vec![Field::new(
            "text",
            FieldType::Text(20, InnoDBCharset::Utf8mb4),
            false,
        )],
        data_columns: vec![
            Field::new("single_f", FieldType::Float, true),
            Field::new("double_f", FieldType::Double, true),
        ],
    };

    let parsed_table = Arc::new(TableDefinition::try_from_sql_statement(&sql).expect("Failed to parse SQL"));
    assert_eq!(parsed_table.as_ref(), &reference);

    let mut table_content_file = File::open(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test_data")
            .join("float_sample.ibd"),
    )
    .expect("Can't open test table");


    let buf_mgr = DummyBufferMangaer;
    let mut buffer = Vec::<u8>::new();
    buffer.resize(16384, 0);

    loop {
        match table_content_file.read_exact(&mut buffer) {
            Ok(_) => {
                let page = Page::from_bytes(&buffer).unwrap();
                if page.header.page_type == PageType::Index {
                    let index = IndexPage::try_from_page(page).unwrap();
                    assert_eq!(index.index_header.index_id, 960, "Wrong Index ID");
                    let mut record = index.infimum().unwrap();
                    while record.next().is_some() {

                        if record.header.record_type == RecordType::Conventional {
                            let row = Row::try_from_record_and_table(&record, &parsed_table).expect("Failed to parse row");
                            let values = row.parse_values(&buf_mgr);
                        }

                        record = record.next().unwrap();
                    }
                    assert_eq!(record.header.record_type, RecordType::Supremum);
                }
            }
            Err(_) => break,
        }
    }
}
