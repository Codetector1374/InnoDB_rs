use std::u64;

use crate::innodb::charset::InnoDBCharset;
use tracing::{info, trace};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
    TinyInt(bool),   // 1
    SmallInt(bool),  // 2
    MediumInt(bool), // 3
    Int(bool),       // 4
    Int6(bool),      // 6
    BigInt(bool),    // 8

    Enum(Vec<String>),

    Text(usize, InnoDBCharset), // CHAR type with non-latin charset also uses this apparently
    Char(usize, InnoDBCharset),
}
impl FieldType {
    // Returns how many bytes does the "length" metadata takes up
    pub fn is_variable(&self) -> bool {
        match self {
            FieldType::TinyInt(_)
            | FieldType::SmallInt(_)
            | FieldType::MediumInt(_)
            | FieldType::Int(_)
            | FieldType::Int6(_)
            | FieldType::BigInt(_) => false,
            FieldType::Enum(_) => false,
            FieldType::Char(_, _) => false,
            FieldType::Text(_, _) => true,
        }
    }

    pub fn max_len(&self) -> u64 {
        match self {
            FieldType::TinyInt(_) => 1,
            FieldType::SmallInt(_) => 2,
            FieldType::MediumInt(_) => 3,
            FieldType::Int(_) => 4,
            FieldType::Int6(_) => 6,
            FieldType::BigInt(_) => 8,
            FieldType::Enum(_) => 2,
            FieldType::Text(len, charset) => (*len as u64) * charset.max_len(),
            FieldType::Char(len, charset) => (*len as u64) * charset.max_len(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum FieldValue {
    SignedInt(i64),
    UnsignedInt(u64),
    String(String),
    PartialString { partial: String, total_len: usize },
    Null,
    Skipped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub field_type: FieldType,
    pub nullable: bool,
}

impl Field {
    pub fn new(name: &str, t: FieldType, nullable: bool) -> Self {
        Field {
            name: name.to_owned(),
            field_type: t,
            nullable,
        }
    }

    fn parse_int(&self, buf: &[u8], len: usize, signed: bool) -> FieldValue {
        assert!(len <= 8, "Currently only support upto u64");
        assert!(buf.len() >= len, "buf not long enough");
        let mut num = 0u64;
        for byte in buf[0..len].iter().cloned() {
            num = (num << 8) | (byte as u64);
        }
        if signed {
            num ^= 1u64 << (len * 8 - 1); // Filp the sign bit -- I don`t know why but it works

            let signed_value;
            if (num & (1u64 << (len * 8 - 1))) != 0 {
                num = !(num - 1);
                num &= (1u64 << (len * 8)) - 1; // Clear other bits
                signed_value = -(num as i64);
            } else {
                signed_value = num as i64;
            }
            FieldValue::SignedInt(signed_value)
        } else {
            assert!(len == 8 || num < (1 << (len * 8)));
            FieldValue::UnsignedInt(num)
        }
    }

    pub fn parse(&self, buf: &[u8], length_opt: Option<u64>) -> (FieldValue, usize) {
        let (val, len) = match self.field_type {
            FieldType::TinyInt(signed) => (self.parse_int(buf, 1, signed), 1),
            FieldType::SmallInt(signed) => (self.parse_int(buf, 2, signed), 2),
            FieldType::MediumInt(signed) => (self.parse_int(buf, 3, signed), 3),
            FieldType::Int(signed) => (self.parse_int(buf, 4, signed), 4),
            FieldType::Int6(signed) => (self.parse_int(buf, 6, signed), 6),
            FieldType::BigInt(signed) => (self.parse_int(buf, 8, signed), 8),
            FieldType::Char(len, _) => (
                FieldValue::String(
                    String::from_utf8(buf[0..len as usize].into())
                        .expect("Failed parsing UTF-8")
                        .trim_end()
                        .to_string(),
                ),
                len as usize,
            ),
            FieldType::Text(max_len, _) => match length_opt {
                None => (FieldValue::Null, 0),
                Some(length) => {
                    assert!(
                        length <= self.field_type.max_len(),
                        "Length larger than expected max? {} > {} in field {:?}",
                        length,
                        max_len,
                        self
                    );
                    let str = String::from_utf8(buf[..length as usize].into())
                        .expect("Failed parsing UTF-8")
                        .trim_end()
                        .to_string();
                    (FieldValue::String(str), length as usize)
                }
            },
            FieldType::Enum(ref values) => {
                let len = if values.len() <= u8::MAX as usize {
                    1
                } else {
                    2
                };

                if let FieldValue::UnsignedInt(num) = self.parse_int(buf, len, false) {
                    assert!((num as usize) < values.len(), "Enum Value is larger than expected?");
                    (FieldValue::String(values[num as usize].clone()), len)
                } else {
                    panic!("Unexpected Enum Parsing Failure");
                }
            }
            #[allow(unreachable_patterns)]
            _ => {
                unimplemented!("type = {:?}", self.field_type);
            }
        };
        trace!("Parsing field {} -> {:?}", self.name, val);

        (val, len)
    }
}

#[cfg(test)]
mod test {
    use super::{Field, FieldType};

    #[test]
    fn test_field_parse_medium_int() {
        let buf = [0x80, 0x00, 0x00];
        let field = Field {
            name: Default::default(),
            field_type: FieldType::MediumInt(true),
            nullable: false,
        };
        let result = field.parse_int(&buf, 3, true);
        match result {
            super::FieldValue::SignedInt(val) => assert_eq!(val, 0),
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_field_parse_tiny_int() {
        let buf = [0x7F];
        let field = Field {
            name: Default::default(),
            field_type: FieldType::TinyInt(true),
            nullable: false,
        };
        let result = field.parse_int(&buf, 1, true);
        match result {
            super::FieldValue::SignedInt(val) => assert_eq!(val, -1),
            _ => unreachable!(),
        }
    }
}
