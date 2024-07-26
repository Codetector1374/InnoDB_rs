pub mod field;
pub mod row;

use anyhow::Result;
use field::{Field, FieldValue};
use std::{collections::HashMap, sync::Arc};

use super::page::index::record::{Record, RECORD_HEADER_FIXED_LENGTH};

#[derive(Debug)]
pub struct TableDefinition {
    pub primary_keys: Vec<Field>,
    pub non_key_fields: Vec<Field>,
}

impl TableDefinition {
    pub fn try_from_sql_statement(sql: &str) -> Result<TableDefinition> {

        
        todo!()
    }

    pub fn names(&self) -> Vec<&str> {
        self.primary_keys
            .iter()
            .chain(self.non_key_fields.iter())
            .map(|f| f.name.as_str())
            .collect()
    }
}


#[cfg(test)]
mod test {

}
