use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
};

use crate::innodb::{page::index::record::{Record, RECORD_HEADER_FIXED_LENGTH}, table::blob_header::BlobHeader};

use super::{field::{Field, FieldValue}, TableDefinition};

use anyhow::Result;
use tracing::{debug, warn};

pub struct Row<'a> {
    td: Arc<TableDefinition>,
    // Field Index, Null or Not
    null_map: HashMap<usize, bool>,
    extern_fields: HashSet<usize>,

    // Field Index, length
    field_len_map: HashMap<usize, u64>,
    record: Record<'a>,
}

impl<'a> Debug for Row<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Row")
            .field("null_map", &self.null_map)
            .field("field_len_map", &self.field_len_map)
            .field("record", &self.record)
            .finish()
    }
}

impl<'a> Row<'a> {
    pub fn try_from_record_and_table(r: &Record<'a>, td: &Arc<TableDefinition>) -> Result<Row<'a>> {
        let mut byte_stream = r.buf[..(r.offset - RECORD_HEADER_FIXED_LENGTH)]
            .iter()
            .rev();

        let mut extern_fields: HashSet<usize> = HashSet::new();

        // Map of null bits: <Field Idx, null_bit>
        let mut null_field_map: HashMap<usize, usize> = HashMap::new();
        for (idx, field) in td
            .cluster_columns
            .iter()
            .chain(td.data_columns.iter())
            .enumerate()
        {
            if field.nullable {
                null_field_map.insert(idx, null_field_map.len());
            }
        }

        let num_null_flag_bytes = null_field_map.len().div_ceil(8);
        let mut null_bits_remain = null_field_map.len();
        let mut null_bits: Vec<bool> = Vec::new();
        for i in 0..num_null_flag_bytes {
            let byte = byte_stream.next().unwrap();
            for bit in 0..8 {
                let is_null = ((byte >> bit) & 1) != 0;
                null_bits.push(is_null);
                null_bits_remain -= 1;
                if null_bits_remain == 0 {
                    assert_eq!(i, num_null_flag_bytes - 1);
                    break;
                }
            }
        }
        assert_eq!(null_bits.len(), null_field_map.len());
        let null_map: HashMap<usize, bool> = null_field_map
            .iter()
            .map(|(k, v)| (*k, null_bits[*v]))
            .collect();

        let mut length_map: HashMap<usize, u64> = HashMap::new();
        for (idx, field) in td
            .cluster_columns
            .iter()
            .chain(td.data_columns.iter())
            .enumerate()
        {
            if field.field_type.is_variable() {
                // NULL Fields don't have length?
                if field.nullable && null_map[&idx] {
                    continue;
                }
                let mut len: u64 = *byte_stream.next().unwrap() as u64;

                /* If the maximum length of the field
                is up to 255 bytes, the actual length
                is always stored in one byte. If the
                maximum length is more than 255 bytes,
                the actual length is stored in one
                byte for 0..127.  The length will be
                encoded in two bytes when it is 128 or
                more, or when the field is stored
                externally. */
                if field.field_type.max_len() > 255 {
                    // 2 bytes
                    if (len & 0x80) != 0 {
                        let byte2 = *byte_stream.next().unwrap();
                        let tmp = (len << 8) | byte2 as u64;
                        len = tmp & 0x3FFF;
                        if tmp & 0x4000 != 0 {
                            extern_fields.insert(idx);
                        }
                    }
                }
                length_map.insert(idx, len);
            }
        }

        Ok(Row {
            td: td.clone(),
            null_map,
            field_len_map: length_map,
            record: r.clone(),
            extern_fields,
        })
    }

    fn parse_single_field(&self, f: &Field, buf: &[u8], idx: usize) -> (FieldValue, usize) {
        if self.extern_fields.contains(&idx) {
            let len = *self.field_len_map.get(&idx).unwrap() as usize;
            assert_eq!(len, 20, "Extern header should be 20 bytes long");
            let extern_header = BlobHeader::from_bytes(&buf[0..len]).expect("Can't make blob header");
            debug!("Extern Header: {:?}", &extern_header);
            // Load a page
            (FieldValue::Skipped, len as usize)
        } else {
            let (value, len) = f.parse(
                buf,
                self.field_len_map.get(&idx).cloned(),
            );
            (value, len)
        }
    }

    /// Only call on primary index
    pub fn values(&self) -> Vec<FieldValue> {
        let mut values = Vec::new();
        let mut current_offset = self.record.offset;
        let num_pk = self.td.cluster_columns.len();
        assert_ne!(num_pk, 0, "Table must have PK");

        for (idx, f) in self.td.cluster_columns.iter().enumerate() {
            let (value, consumed) = self.parse_single_field(f, &self.record.buf[current_offset..], idx);
            current_offset += consumed;
            values.push(value);
        }
        // Hidden Columns
        current_offset += 6 + 7;

        let cluster_count = self.td.cluster_columns.len();
        for (idx, f) in self.td.data_columns.iter().enumerate() {
            let idx = idx + cluster_count;
            let (value, consumed) = self.parse_single_field(f, &self.record.buf[current_offset..], idx);
            current_offset += consumed;
            values.push(value);
        }

        values
    }
}
