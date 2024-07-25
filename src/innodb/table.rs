#[derive(Debug, Clone)]
pub enum FieldType {
    TinyInt,
    SmallInt,
    MediumInt,
    Int,
    BigInt,

    VarChar(u16),
    Char(u8),
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub field_type: FieldType,
    pub nullable: bool,
    pub signed: bool,
    pub primary_key: bool,
}

pub struct TableDefinition {
    primary_keys: Vec<Field>,
    non_key_fields: Vec<Field>,
}
