use anyhow::{Error, Result};

#[derive(Debug, Clone)]
pub struct IndexHeader {
    pub number_of_directory_slots: u16,           // 2 bytes
    pub heap_top_position: u16,                   // 2 bytes
    pub number_of_heap_records: u16,              // 2 bytes and format Flag
    pub first_garbage_record_offset: u16,         // 2 bytes
    pub garbage_space: u16,                       // 2 bytes
    pub last_insert_position: u16,                // 2 bytes
    pub page_direction: u16,                      // 2 bytes
    pub number_of_inserts_in_page_direction: u16, // 2 bytes
    pub number_of_records: u16,                   // 2 bytes
    pub maximum_transaction_id: u64,              // 8 bytes
    pub page_level: u16,                          // 2 bytes
    pub index_id: u64,                            // 8 bytes
}

impl IndexHeader {
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 36 {
            return Err(Error::msg("Data slice is too short"));
        }

        Ok(IndexHeader {
            number_of_directory_slots: u16::from_be_bytes([data[0], data[1]]),
            heap_top_position: u16::from_be_bytes([data[2], data[3]]),
            number_of_heap_records: u16::from_be_bytes([data[4], data[5]]),
            first_garbage_record_offset: u16::from_be_bytes([data[6], data[7]]),
            garbage_space: u16::from_be_bytes([data[8], data[9]]),
            last_insert_position: u16::from_be_bytes([data[10], data[11]]),
            page_direction: u16::from_be_bytes([data[12], data[13]]),
            number_of_inserts_in_page_direction: u16::from_be_bytes([data[14], data[15]]),
            number_of_records: u16::from_be_bytes([data[16], data[17]]),
            maximum_transaction_id: u64::from_be_bytes([
                data[18], data[19], data[20], data[21], data[22], data[23], data[24], data[25],
            ]),
            page_level: u16::from_be_bytes([data[26], data[27]]),
            index_id: u64::from_be_bytes([
                data[28], data[29], data[30], data[31], data[32], data[33], data[34], data[35],
            ]),
        })
    }
}
