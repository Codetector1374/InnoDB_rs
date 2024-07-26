use tracing::trace;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldType {
    TinyInt(bool),   // 1
    SmallInt(bool),  // 2
    MediumInt(bool), // 3
    Int(bool),       // 4
    Int6(bool),      // 6
    BigInt(bool),    // 8

    VariableChars(u16), // CHAR type with non-latin charset also uses this apparently
    Char(u8),
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
            FieldType::Char(_) => false,
            FieldType::VariableChars(_) => true,
        }
    }

    pub fn max_len(&self) -> u16 {
        match self {
            FieldType::TinyInt(_) => 1,
            FieldType::SmallInt(_) => 2,
            FieldType::MediumInt(_) => 3,
            FieldType::Int(_) => 4,
            FieldType::Int6(_) => 6,
            FieldType::BigInt(_) => 8,
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
        let mut num = 0;
        for byte in buf[0..len].iter().cloned() {
            num = (num << 8) | (byte as u64);
        }
        if signed {
            if len < std::mem::size_of::<u64>() {
                let sign = (num & (0x80 << ((len - 1) * 8))) != 0;
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
            FieldType::TinyInt(signed) => (self.parse_int(buf, 1, signed), 1),
            FieldType::SmallInt(signed) => (self.parse_int(buf, 2, signed), 2),
            FieldType::MediumInt(signed) => (self.parse_int(buf, 3, signed), 3),
            FieldType::Int(signed) => (self.parse_int(buf, 4, signed), 4),
            FieldType::Int6(signed) => (self.parse_int(buf, 6, signed), 6),
            FieldType::BigInt(signed) => (self.parse_int(buf, 8, signed), 8),
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
    fn test_field_parse_int() {
        let buf = [0xFFu8, 0xFF, 0xFF];
        let field = Field {
            name: Default::default(),
            field_type: FieldType::MediumInt(true),
            nullable: false,
        };
        let result = field.parse_int(&buf, 3, true);
        match result {
            super::FieldValue::SignedInt(val) => assert_eq!(val, -1),
            _ => unreachable!(),
        }
    }
}
