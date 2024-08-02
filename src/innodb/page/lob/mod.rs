use crate::innodb::{file_list::FileListBaseNode, InnoDBError};
use anyhow::{anyhow, Ok, Result};

use super::{Page, PageType};

#[derive(Debug, Clone)]
pub struct LobFirstHeader {
    pub version: u8,
    pub flags: u8,
    pub lob_version: u32,
    pub last_transaction_id: u64, // 6 bytes
    pub last_undo_number: u32,
    pub data_length: u32,
    pub transaction_id: u64, // 6 bytes
    pub index_list_head: FileListBaseNode,
    pub free_list_head: FileListBaseNode,
}

pub const LOB_FIRST_HEADER_SIZE: usize = 54;

impl LobFirstHeader {
    pub fn try_from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < 54 {
            return Err(anyhow!("Buffer is too small for LobHeader"));
        }

        let version = buf[0];
        let flags = buf[1];
        let lob_version = u32::from_be_bytes([buf[2], buf[3], buf[4], buf[5]]);

        // Handle 6-byte last_transaction_id
        let last_transaction_id =
            u64::from_be_bytes([0, 0, buf[6], buf[7], buf[8], buf[9], buf[10], buf[11]]);

        let last_undo_number = u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]);
        let data_length = u32::from_be_bytes([buf[16], buf[17], buf[18], buf[19]]);

        // Handle 6-byte transaction_id
        let transaction_id =
            u64::from_be_bytes([0, 0, buf[20], buf[21], buf[22], buf[23], buf[24], buf[25]]);

        let index_list_head = FileListBaseNode::try_from_bytes(&buf[26..42])?;
        let free_list_head = FileListBaseNode::try_from_bytes(&buf[42..58])?;

        Ok(LobFirstHeader {
            version,
            flags,
            lob_version,
            last_transaction_id,
            last_undo_number,
            data_length,
            transaction_id,
            index_list_head,
            free_list_head,
        })
    }
}

#[derive(Debug)]
pub struct LobFirst<'a> {
    pub page: Page<'a>,
    pub header: LobFirstHeader,
}

impl<'a> LobFirst<'a> {
    pub fn try_from_page(p: Page<'a>) -> Result<Self> {
        match p.header.page_type {
            PageType::LobFirst => Ok(LobFirst {
                header: LobFirstHeader::try_from_bytes(p.body())?,
                page: p,
            }),
            _ => Err(anyhow!(InnoDBError::InvalidPageType {
                expected: PageType::LobFirst,
                has: p.header.page_type
            })),
        }
    }

    pub fn data(&self) -> &[u8] {
        &self.page.body()[LOB_FIRST_HEADER_SIZE..]
    }
}
