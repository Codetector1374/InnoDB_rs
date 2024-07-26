pub mod field;
pub mod row;

use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, Result};
use bitvec::ptr::null;
use field::{Field, FieldType};
use sqlparser::{
    ast::{CharacterLength, ColumnOption, DataType, Statement, TableConstraint},
    dialect::MySqlDialect,
    parser::Parser,
};
use tracing::debug;

use crate::innodb::charset::InnoDBCharset;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct TableDefinition {
    pub name: String,
    pub primary_keys: Vec<Field>,
    pub non_key_fields: Vec<Field>,
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

            // Parse Indexes
            let mut pk_list: HashSet<String> = HashSet::new();
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
                        pk_list.extend(columns.iter().map(|c| c.value.clone()));
                    }
                    _ => {
                        debug!("Ignoring constraint {:?}", constraint);
                    }
                }
            }

            // Actual Columns
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
                let f_type: FieldType = match column.data_type {
                    DataType::Char(len_opt) => {
                        let final_len = match len_opt {
                            Some(l) => match l {
                                CharacterLength::IntegerLength { length, unit: _ } => length,
                                CharacterLength::Max => u8::MAX as u64,
                            },
                            None => u8::MAX as u64,
                        };
                        assert!(final_len <= u8::MAX as u64);
                        if charset.max_len() == 1 {
                            FieldType::Char(final_len as u8)
                        } else {
                            FieldType::VariableChars(final_len as u16)
                        }
                    }
                    DataType::Varchar(len_opt) => {
                        let final_len = match len_opt {
                            Some(l) => match l {
                                CharacterLength::IntegerLength { length, unit: _ } => length,
                                CharacterLength::Max => u16::MAX as u64,
                            },
                            None => u16::MAX as u64,
                        };
                        assert!(final_len <= u16::MAX as u64);
                        FieldType::VariableChars(final_len as u16)
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
                    _ => unimplemented!("mapping of {:?}", column.data_type),
                };

                let nullable = !column
                    .options
                    .iter()
                    .any(|opt| opt.option == ColumnOption::NotNull);

                let field = Field {
                    name: column.name.value.clone(),
                    field_type: f_type,
                    nullable: nullable,
                };

                if pk_list.remove(&field.name) {
                    // TODO: Unsure the ordering here. Maybe it should be in `PRIMARY KEY(`column1`, `column2`)` order?
                    table_def.primary_keys.push(field);
                } else {
                    table_def.non_key_fields.push(field);
                }
            }

            Ok(table_def)
        } else {
            Err(anyhow!("Not Create Table Statement"))
        }
    }

    pub fn names(&self) -> Vec<&str> {
        self.primary_keys
            .iter()
            .chain(self.non_key_fields.iter())
            .map(|f| f.name.as_str())
            .collect()
    }

    pub fn field_count(&self) -> usize {
        self.primary_keys.len() + self.non_key_fields.len()
    }

    pub fn get_field(&self, name: &str) -> Option<&Field> {
        self.primary_keys
            .iter()
            .chain(self.non_key_fields.iter())
            .find(|f| f.name == name)
    }

    pub fn get_field_mut(&mut self, name: &str) -> Option<&mut Field> {
        self.primary_keys
            .iter_mut()
            .chain(self.non_key_fields.iter_mut())
            .find(|f| f.name == name)
    }
}

#[cfg(test)]
mod test {
    use std::{fs::read_to_string, path::PathBuf};

    use crate::innodb::table::field::FieldType;

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

        assert_eq!(def.primary_keys.len(), 1);
        assert_eq!(def.non_key_fields.len(), 2);

        let field1 = def.get_field("field1").unwrap();
        assert_eq!(field1.name, "field1");
        assert_eq!(field1.field_type, FieldType::Int(false));
        assert_eq!(field1.nullable, false);
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
            primary_keys: vec![
                // name, type, nullable, signed, pk
                Field::new("uid", FieldType::MediumInt(false), false),
            ],
            non_key_fields: vec![
                // name, type, nullable, signed, pk
                Field::new("username", FieldType::VariableChars(15), false),
                Field::new("password", FieldType::VariableChars(255), false),
                Field::new("secmobicc", FieldType::VariableChars(3), false),
                Field::new("secmobile", FieldType::VariableChars(12), false),
                Field::new("email", FieldType::VariableChars(255), false),
                Field::new("myid", FieldType::VariableChars(30), false),
                Field::new("myidkey", FieldType::VariableChars(16), false),
                Field::new("regip", FieldType::VariableChars(45), false),
                Field::new("regdate", FieldType::Int(false), false),
                Field::new("lastloginip", FieldType::Int(true), false),
                Field::new("lastlogintime", FieldType::Int(false), false),
                Field::new("salt", FieldType::VariableChars(20), false),
                Field::new("secques", FieldType::VariableChars(8), false),
            ],
        };

        let parsed = TableDefinition::try_from_sql_statement(&sql).expect("Failed to parse SQL");
        assert_eq!(parsed, reference);
    }
}
