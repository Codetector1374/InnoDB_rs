use anyhow::Result;
use std::{collections::HashMap, i64, sync::Arc};
use tracing::{trace, warn};

use super::page::index::record::{Record, RECORD_HEADER_FIXED_LENGTH};

#[derive(Debug, Clone)]
pub enum FieldType {
    TinyInt,   // 1
    SmallInt,  // 2
    MediumInt, // 3
    Int,       // 4
    Int6,      // 6
    BigInt,    // 8

    VariableChars(u16), // CHAR type with non-latin charset also uses this apparently
    Char(u8),
}
impl FieldType {
    // Returns how many bytes does the "length" metadata takes up
    pub fn is_variable(&self) -> bool {
        match self {
            FieldType::TinyInt
            | FieldType::SmallInt
            | FieldType::MediumInt
            | FieldType::Int
            | FieldType::Int6
            | FieldType::BigInt => false,
            FieldType::Char(_) => false,
            FieldType::VariableChars(_) => true,
        }
    }

    pub fn max_len(&self) -> u16 {
        match self {
            FieldType::TinyInt => 1,
            FieldType::SmallInt => 2,
            FieldType::MediumInt => 3,
            FieldType::Int => 4,
            FieldType::Int6 => 6,
            FieldType::BigInt => 8,
            FieldType::VariableChars(len) => *len,
            FieldType::Char(len) => *len as u16,
        }
    }
}

#[derive(Clone, Debug)]
pub enum FieldValue {
    SignedInt(i64),
    UnsignedInt(u64),
    String(String),
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub field_type: FieldType,
    pub nullable: bool,
    pub signed: bool,
    pub primary_key: bool,
}

impl Field {
    pub fn new(name: &str, t: FieldType, nullable: bool, signed: bool, pk: bool) -> Self {
        Field {
            name: name.to_owned(),
            field_type: t,
            nullable,
            signed,
            primary_key: pk,
        }
    }

    fn parse_int(&self, buf: &[u8], len: usize) -> FieldValue {
        assert!(len <= 8, "Currently only support upto u64");
        assert!(buf.len() >= len, "buf not long enough");
        let mut num = 0;
        for byte in buf[0..len].iter().cloned() {
            num = (num << 8) | (byte as u64);
        }
        if self.signed {
            if len < std::mem::size_of::<u64>() {
                let sign = (num & (0x80 << ((len-1) * 8))) != 0;
                let mask = u64::MAX & !((1 << (8 * len)) - 1);
                if sign {
                    num |= mask;
                }
            }
            FieldValue::SignedInt(num as i64)
        } else {
            FieldValue::UnsignedInt(num)
        }
    }

    pub fn parse(&self, buf: &[u8], length_opt: Option<u16>) -> (FieldValue, usize) {
        let (val, len) = match self.field_type {
            FieldType::TinyInt => (self.parse_int(buf, 1), 1),
            FieldType::SmallInt => (self.parse_int(buf, 2), 2),
            FieldType::MediumInt => (self.parse_int(buf, 3), 3),
            FieldType::Int => (self.parse_int(buf, 4), 4),
            FieldType::Int6 => (self.parse_int(buf, 6), 6),
            FieldType::BigInt => (self.parse_int(buf, 8), 8),
            FieldType::Char(len) => (
                FieldValue::String(
                    String::from_utf8(buf[0..len as usize].into())
                        .unwrap()
                        .trim_end()
                        .to_string(),
                ),
                len as usize,
            ),
            FieldType::VariableChars(max_len) => {
                let length = length_opt.expect("Must have length");
                assert!(
                    length <= max_len * 4, // TODO: fix this, *4 is hard code for UTF8-MB4
                    "Length larger than expected max? {} > {} in field {:?}",
                    length,
                    max_len,
                    self
                );
                let str = String::from_utf8_lossy(&buf[..length as usize])
                    .trim_end()
                    .to_string();
                (FieldValue::String(str), length as usize)
            }
            _ => {
                unimplemented!("type = {:?}", self.field_type);
            }
        };
        trace!("Parsing field {} -> {:?}", self.name, val);

        (val, len)
    }
}

#[derive(Debug)]
pub struct TableDefinition {
    pub primary_keys: Vec<Field>,
    pub non_key_fields: Vec<Field>,
}

impl TableDefinition {
    pub fn names(&self) -> Vec<&str> {
        self.primary_keys
            .iter()
            .chain(self.non_key_fields.iter())
            .map(|f| f.name.as_str())
            .collect()
    }
}

#[derive(Debug)]
pub struct Row<'a> {
    td: Arc<TableDefinition>,
    // Field Index, Null or Not
    null_map: HashMap<usize, bool>,

    // Field Index, length
    len_vec: HashMap<usize, u16>,
    record: Record<'a>,
}

impl<'a> Row<'a> {
    pub fn try_from_record_and_table(r: &Record<'a>, td: &Arc<TableDefinition>) -> Result<Row<'a>> {
        let mut byte_stream = r.buf[..(r.offset - RECORD_HEADER_FIXED_LENGTH)]
            .iter()
            .rev();

        // Map of null bits: <Field Idx, null_bit>
        let mut null_field_map: HashMap<usize, usize> = HashMap::new();
        for (idx, field) in td
            .primary_keys
            .iter()
            .chain(td.non_key_fields.iter())
            .enumerate()
        {
            assert!(!(field.nullable && field.primary_key), "PK can't be null");
            if field.nullable {
                null_field_map.insert(idx, null_field_map.len());
                todo!("Verify this");
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

        let mut length_map: HashMap<usize, u16> = HashMap::new();
        for (idx, field) in td
            .primary_keys
            .iter()
            .chain(td.non_key_fields.iter())
            .enumerate()
        {
            if field.field_type.is_variable() {
                // NULL Fields don't have length?
                if field.nullable && null_map[&idx] {
                    continue;
                }
                let mut len: u16 = *byte_stream.next().unwrap() as u16;

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
                    if len & 0x80 != 0 {
                        let byte2 = *byte_stream.next().unwrap();
                        let tmp = (len << 8) | byte2 as u16;
                        if tmp & 0x4000 != 0 {
                            warn!("[Unimplemented] Extern!!!");
                        }
                        len = tmp & 0x3FFF;
                    }
                }
                length_map.insert(idx, len);
            }
        }

        Ok(Row {
            td: td.clone(),
            null_map: null_map,
            len_vec: length_map,
            record: r.clone(),
        })
    }

    /// Only call on primary index
    pub fn values(&self) -> Vec<FieldValue> {
        let mut values = Vec::new();
        let mut current_offset = self.record.offset;
        let num_pk = self.td.primary_keys.len();
        assert_ne!(num_pk, 0, "Table must have PK");

        for (idx, f) in self.td.primary_keys.iter().enumerate() {
            let (value, len) = f.parse(
                &self.record.buf[current_offset..],
                self.len_vec.get(&idx).cloned(),
            );
            current_offset += len;
            values.push(value);
        }
        // Hidden Columns
        current_offset += 6 + 7;

        for (idx, f) in self.td.non_key_fields.iter().enumerate() {
            let idx = idx + num_pk;

            let (value, len) = f.parse(
                &self.record.buf[current_offset..],
                self.len_vec.get(&idx).cloned(),
            );

            current_offset += len;
            values.push(value);
        }

        values
    }
}

#[cfg(test)]
mod test {
    use super::{Field, FieldType};

    #[test]
    fn test_field_parse_int() {
        let buf = [0xFFu8, 0xFF, 0xFF];
        let mut field = Field {
            name: Default::default(),
            field_type: FieldType::MediumInt,
            nullable: false,
            signed: true,
            primary_key: false,
        };
        let result = field.parse_int(&buf, 3);
        match result {
            super::FieldValue::SignedInt(val) => assert_eq!(val, -1),
            _ => unreachable!()
        }
    }
}