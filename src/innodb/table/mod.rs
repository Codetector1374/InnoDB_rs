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

#[derive(Debug, Default)]
pub struct TableDefinition {
    pub name: String,
    pub primary_keys: Vec<Field>,
    pub non_key_fields: Vec<Field>,
}

fn character_length_to_u64(l: Option<CharacterLength>) -> Option<u64> {
    if let Some(len) = l {
        return match len {
            CharacterLength::IntegerLength { length, unit: _ } => Some(length),
            CharacterLength::Max => None,
        };
    }
    None
}

impl TableDefinition {
    pub fn try_from_sql_statement(sql: &str) -> Result<TableDefinition> {
        let mut parser = Parser::new(&MySqlDialect {}).try_with_sql(sql)?;
        let stmt = parser.parse_statement()?;
        if let Statement::CreateTable(parsed_table) = stmt {
            let mut table_def = TableDefinition::default();

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
                let f_type: FieldType = match column.data_type {
                    DataType::Char(len_opt) => {
                        FieldType::Char(character_length_to_u64(len_opt).unwrap_or(255) as u8)
                    }
                    DataType::UnsignedInt(_) => FieldType::Int(false),
                    DataType::Int(_) => FieldType::Int(true),
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
    use crate::innodb::table::field::FieldType;

    use super::TableDefinition;

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
}
