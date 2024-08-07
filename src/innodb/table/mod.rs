pub mod blob_header;
pub mod field;
pub mod row;

use anyhow::{anyhow, Result};
use field::{Field, FieldType};
use sqlparser::{
    ast::{CharacterLength, ColumnOption, DataType, Statement, TableConstraint},
    dialect::MySqlDialect,
    parser::Parser,
};
use tracing::{debug, info};

use crate::innodb::charset::InnoDBCharset;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct TableDefinition {
    pub name: String,
    pub cluster_columns: Vec<Field>,
    pub data_columns: Vec<Field>,
}

impl TableDefinition {
    pub fn try_from_sql_statement(sql: &str) -> Result<TableDefinition> {
        let mut parser = Parser::new(&MySqlDialect {}).try_with_sql(sql)?;
        let stmt = parser.parse_statement()?;
        if let Statement::CreateTable(parsed_table) = stmt {
            let mut table_def = TableDefinition::default();

            let table_charset = match parsed_table.default_charset {
                Some(charset_str) => InnoDBCharset::with_name(&charset_str).unwrap(),
                None => InnoDBCharset::Ascii,
            };

            assert_eq!(parsed_table.name.0.len(), 1, "Table name is only 1 part");
            table_def.name = parsed_table.name.0.first().unwrap().value.clone();

            // Actual Columns
            let mut parsed_fields: Vec<Field> = Vec::new();
            for column in parsed_table.columns.iter() {
                let charset = column
                    .options
                    .iter()
                    .map(|opt| &opt.option)
                    .filter_map(|opt| match opt {
                        ColumnOption::CharacterSet(name) => {
                            InnoDBCharset::with_name(&name.0.first().unwrap().value).ok()
                        }
                        _ => None,
                    })
                    .last()
                    .unwrap_or(table_charset);
                let f_type: FieldType = match &column.data_type {
                    DataType::Char(len_opt) => {
                        let final_len = match len_opt {
                            Some(l) => match l {
                                CharacterLength::IntegerLength { length, unit: _ } => *length,
                                CharacterLength::Max => u8::MAX as u64,
                            },
                            None => u8::MAX as u64,
                        };
                        assert!(final_len <= u8::MAX as u64);
                        if charset.max_len() == 1 {
                            FieldType::Char(final_len as usize, charset)
                        } else {
                            FieldType::Text(final_len as usize, charset)
                        }
                    }
                    DataType::Varchar(len_opt) => {
                        let final_len = match len_opt {
                            Some(l) => match l {
                                CharacterLength::IntegerLength { length, unit: _ } => *length,
                                CharacterLength::Max => u16::MAX as u64,
                            },
                            None => u16::MAX as u64,
                        };
                        assert!(final_len <= u16::MAX as u64);
                        FieldType::Text(final_len as usize, charset)
                    }
                    DataType::UnsignedTinyInt(_) => FieldType::TinyInt(false),
                    DataType::UnsignedSmallInt(_) => FieldType::SmallInt(false),
                    DataType::UnsignedMediumInt(_) => FieldType::MediumInt(false),
                    DataType::UnsignedInt(_) => FieldType::Int(false),
                    DataType::UnsignedBigInt(_) => FieldType::BigInt(false),
                    DataType::TinyInt(_) => FieldType::TinyInt(true),
                    DataType::SmallInt(_) => FieldType::SmallInt(true),
                    DataType::MediumInt(_) => FieldType::MediumInt(true),
                    DataType::Int(_) => FieldType::Int(true),
                    DataType::BigInt(_) => FieldType::BigInt(true),
                    DataType::Custom(name, _) => match name.0[0].value.as_str() {
                        "mediumtext" => FieldType::Text((1 << 24) - 1, charset),
                        "longtext" => FieldType::Text((1 << 32) - 1, charset),
                        _ => unimplemented!("Custom: {} unhandled", name.0[0].value),
                    },
                    DataType::Enum(values) => FieldType::Enum(values.clone()),
                    DataType::Date => FieldType::Date,
                    DataType::Datetime(_)=> FieldType::DateTime,
                    DataType::Timestamp(_,_) => FieldType::Timestamp,
                    _ => unimplemented!("mapping of {:?}", column.data_type),
                };

                let nullable = !column
                    .options
                    .iter()
                    .any(|opt| opt.option == ColumnOption::NotNull);

                let field = Field {
                    name: column.name.value.clone(),
                    field_type: f_type,
                    nullable,
                };

                parsed_fields.push(field);
            }

            // Parse Indexes
            let mut cluster_index_columns: Vec<String> = Vec::new();
            let mut unique_keys: Vec<Vec<String>> = Vec::new();
            for constraint in parsed_table.constraints.iter() {
                match constraint {
                    TableConstraint::PrimaryKey {
                        name: _,
                        index_name: _,
                        index_type: _,
                        columns,
                        index_options: _,
                        characteristics: _,
                    } => {
                        assert!(
                            cluster_index_columns.is_empty(),
                            "Multiple Primary Key is not allowed"
                        );
                        cluster_index_columns.extend(columns.iter().map(|c| c.value.clone()));
                    }
                    TableConstraint::Unique {
                        name: _,
                        index_name: _,
                        index_type_display: _,
                        index_type: _,
                        columns,
                        index_options: _,
                        characteristics: _,
                    } => {
                        unique_keys.push(columns.iter().map(|c| c.value.clone()).collect());
                    }
                    _ => {
                        debug!("Ignoring constraint {:?}", constraint);
                    }
                }
            }

            // If there is no use specified primary key, check for a unique
            // with all `NOT NULL` columns
            if cluster_index_columns.is_empty() {
                info!("No PRIMARY KEY specified, finding suitable column");
                for unique in unique_keys.iter() {
                    let is_all_not_null = unique.iter().all(|field_name| {
                        parsed_fields
                            .iter()
                            .find(|f| f.name == *field_name)
                            .map(|f| !f.nullable)
                            .unwrap_or(false)
                    });

                    if is_all_not_null {
                        info!("Using Unique({:?}) as Clustering Index", unique);
                        cluster_index_columns = unique.clone();
                        break;
                    }
                }
            }

            if cluster_index_columns.is_empty() {
                info!("No PRIMARY KEY or suitable UNIQUE, making a pseudo column for clustering index");
                table_def.cluster_columns.push(Field {
                    name: "ROWID".into(),
                    field_type: FieldType::Int6(false),
                    nullable: false,
                });
            }

            for field in cluster_index_columns.iter() {
                let field = parsed_fields
                    .iter()
                    .find(|f| f.name == *field)
                    .expect("Failed to find named column in clustering index");
                table_def.cluster_columns.push(field.clone());
            }

            for field in parsed_fields.into_iter() {
                if !cluster_index_columns.contains(&field.name) {
                    table_def.data_columns.push(field);
                }
            }

            assert!(
                !table_def.cluster_columns.is_empty(),
                "Table must have at least 1 cluster column"
            );

            Ok(table_def)
        } else {
            Err(anyhow!("Not Create Table Statement"))
        }
    }

    pub fn names(&self) -> Vec<&str> {
        self.cluster_columns
            .iter()
            .chain(self.data_columns.iter())
            .map(|f| f.name.as_str())
            .collect()
    }

    pub fn field_count(&self) -> usize {
        self.cluster_columns.len() + self.data_columns.len()
    }

    pub fn get_field(&self, name: &str) -> Option<&Field> {
        self.cluster_columns
            .iter()
            .chain(self.data_columns.iter())
            .find(|f| f.name == name)
    }

    pub fn get_field_mut(&mut self, name: &str) -> Option<&mut Field> {
        self.cluster_columns
            .iter_mut()
            .chain(self.data_columns.iter_mut())
            .find(|f| f.name == name)
    }
}

#[cfg(test)]
mod test {
    use std::{fs::read_to_string, path::PathBuf};

    use crate::innodb::{charset::InnoDBCharset, table::field::FieldType};

    use super::{field::Field, TableDefinition};

    #[test]
    fn parse_sql_to_table_def_1() {
        let sql = r#"CREATE TABLE `sample` (
            `field1` int unsigned NOT NULL,
            `field2` int,
            `field3` CHAR(5),
            PRIMARY KEY (`field1`)
        );"#;

        let def = TableDefinition::try_from_sql_statement(sql);
        assert!(def.is_ok());
        let def = def.unwrap();

        assert_eq!(def.name, "sample", "table name is wrong");

        assert_eq!(def.cluster_columns.len(), 1);
        assert_eq!(def.data_columns.len(), 2);

        let field1 = def.get_field("field1").unwrap();
        assert_eq!(field1.name, "field1");
        assert_eq!(field1.field_type, FieldType::Int(false));
        assert!(!field1.nullable);
    }

    #[test]
    fn prase_sql_complex_table() {
        let sql = read_to_string(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("test_data")
                .join("pre_ucenter_members.sql"),
        )
        .unwrap();
        let reference = TableDefinition {
            name: String::from("pre_ucenter_members"),
            cluster_columns: vec![
                // name, type, nullable, signed, pk
                Field::new("uid", FieldType::MediumInt(false), false),
            ],
            data_columns: vec![
                // name, type, nullable, signed, pk
                Field::new(
                    "username",
                    FieldType::Text(15, InnoDBCharset::Utf8mb4),
                    false,
                ),
                Field::new(
                    "password",
                    FieldType::Text(255, InnoDBCharset::Utf8mb4),
                    false,
                ),
                Field::new(
                    "secmobicc",
                    FieldType::Text(3, InnoDBCharset::Utf8mb4),
                    false,
                ),
                Field::new(
                    "secmobile",
                    FieldType::Text(12, InnoDBCharset::Utf8mb4),
                    false,
                ),
                Field::new("email", FieldType::Text(255, InnoDBCharset::Utf8mb4), false),
                Field::new("myid", FieldType::Text(30, InnoDBCharset::Utf8mb4), false),
                Field::new(
                    "myidkey",
                    FieldType::Text(16, InnoDBCharset::Utf8mb4),
                    false,
                ),
                Field::new("regip", FieldType::Text(45, InnoDBCharset::Utf8mb4), false),
                Field::new("regdate", FieldType::Int(false), false),
                Field::new("lastloginip", FieldType::Int(true), false),
                Field::new("lastlogintime", FieldType::Int(false), false),
                Field::new("salt", FieldType::Text(20, InnoDBCharset::Utf8mb4), false),
                Field::new("secques", FieldType::Text(8, InnoDBCharset::Utf8mb4), false),
            ],
        };

        let parsed = TableDefinition::try_from_sql_statement(&sql).expect("Failed to parse SQL");
        assert_eq!(parsed, reference);
    }
}
