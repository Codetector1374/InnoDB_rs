use crate::innodb::charset::InnoDBCharset;
use chrono::DateTime;
use tracing::{info, trace};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
    TinyInt(bool),   // 1
    SmallInt(bool),  // 2
    MediumInt(bool), // 3
    Int(bool),       // 4
    Int6(bool),      // 6
    BigInt(bool),    // 8

    Float,
    Double,

    Enum(Vec<String>),

    Text(usize, InnoDBCharset), // CHAR type with non-latin charset also uses this apparently
    Char(usize, InnoDBCharset),

    Date,
    DateTime,
    Timestamp,
}
impl FieldType {
    // Returns how many bytes does the "length" metadata takes up
    pub fn is_variable(&self) -> bool {
        match self {
            FieldType::Text(_, _) => true,
            _ => false,
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

            FieldType::Float => 4,
            FieldType::Double => 8,

            FieldType::Enum(_) => 2,

            FieldType::Text(len, charset) => (*len as u64) * charset.max_len(),
            FieldType::Char(len, charset) => (*len as u64) * charset.max_len(),

            FieldType::Date => 3,
            FieldType::DateTime => 8,
            FieldType::Timestamp => 4,
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

    fn parse_uint(&self, buf: &[u8], len: usize) -> u64 {
        assert!(len <= 8, "Currently only support upto u64");
        assert!(buf.len() >= len, "buf not long enough");
        let mut num = 0u64;
        for byte in buf[0..len].iter().cloned() {
            num = (num << 8) | (byte as u64);
        }
        num
    }

    fn parse_signed_int(&self, buf: &[u8], len: usize) -> i64 {
        let mut num = self.parse_uint(buf, len);
        num ^= 1u64 << (len * 8 - 1); // Filp the sign bit -- I don`t know why but it works

        let signed_value;
        if (num & (1u64 << (len * 8 - 1))) != 0 {
            num = !(num - 1);
            num &= (1u64 << (len * 8)) - 1; // Clear other bits
            signed_value = -(num as i64);
        } else {
            signed_value = num as i64;
        }
        signed_value
    }

    fn parse_int_field(&self, buf: &[u8], len: usize, signed: bool) -> FieldValue {
        if signed {
            FieldValue::SignedInt(self.parse_signed_int(buf, len))
        } else {
            FieldValue::UnsignedInt(self.parse_uint(buf, len))
        }
    }

    pub fn parse(&self, buf: &[u8], length_opt: Option<u64>) -> (FieldValue, usize) {
        let (val, len) = match self.field_type {
            FieldType::TinyInt(signed) => (self.parse_int_field(buf, 1, signed), 1),
            FieldType::SmallInt(signed) => (self.parse_int_field(buf, 2, signed), 2),
            FieldType::MediumInt(signed) => (self.parse_int_field(buf, 3, signed), 3),
            FieldType::Int(signed) => (self.parse_int_field(buf, 4, signed), 4),
            FieldType::Int6(signed) => (self.parse_int_field(buf, 6, signed), 6),
            FieldType::BigInt(signed) => (self.parse_int_field(buf, 8, signed), 8),
            FieldType::Char(len, _) => (
                FieldValue::String(
                    String::from_utf8(buf[0..len].into())
                        .expect("Failed parsing UTF-8")
                        .trim_end()
                        .to_string(),
                ),
                len,
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
            FieldType::Date => {
                let date_num = self.parse_signed_int(buf, 3);
                let day = date_num & 0x1F;
                let month = (date_num >> 5) & 0xF;
                let year = date_num >> 9;
                (
                    FieldValue::String(format!("{:04}-{:02}-{:02}", year, month, day)),
                    3,
                )
            }
            FieldType::DateTime => {
                let datetime = self.parse_signed_int(buf, 8) as u64;
                let yd = datetime >> 46;
                let year = yd / 13;
                let month = yd - year * 13;
                let day = (datetime >> 41) & 0b11111;
                let hour = (datetime >> 36) & 0b11111;
                let min = (datetime >> 30) & 0b111111;
                let sec = (datetime >> 24) & 0b111111;
                (
                    FieldValue::String(format!(
                        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                        year, month, day, hour, min, sec
                    )),
                    8,
                )
            }
            FieldType::Timestamp => {
                let ts = self.parse_uint(buf, 4);
                if ts == 0 {
                    (FieldValue::String("0000-00-00 00:00:00".to_owned()), 4)
                } else {
                    let datetime =
                        DateTime::from_timestamp(ts as i64, 0).expect("Out of range Datetime");
                    (
                        FieldValue::String(format!("{}", datetime.format("%Y-%m-%d %H:%M:%S"))),
                        4,
                    )
                }
            }
            FieldType::Enum(ref values) => {
                let len = if values.len() <= u8::MAX as usize {
                    1
                } else {
                    2
                };

                let num = self.parse_uint(buf, len);
                if num == 0 {
                    (FieldValue::String("".to_owned()), len)
                } else {
                    let variant_index = num - 1;
                    assert!(
                        (variant_index as usize) < values.len(),
                        "Enum Value is larger than expected? {} vs {}",
                        variant_index,
                        values.len()
                    );
                    (
                        FieldValue::String(values[variant_index as usize].clone()),
                        len,
                    )
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
        let result = field.parse_int_field(&buf, 3, true);
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
        let result = field.parse_int_field(&buf, 1, true);
        match result {
            super::FieldValue::SignedInt(val) => assert_eq!(val, -1),
            _ => unreachable!(),
        }
    }
}
