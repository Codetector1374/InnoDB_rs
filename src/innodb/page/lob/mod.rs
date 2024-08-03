use crate::innodb::{
    file_list::{FileListBaseNode, FileListInnerNode},
    InnoDBError,
};
use anyhow::{anyhow, Ok, Result};

use super::{Page, PageType};

/*
 * General Flow for reading extern records
 *
 * First Obtain `ExternalReference` from cluster index
 * Load the page number from that ext ref.
 *
 * Load this page:
 *
 * If Type is BLOB or SDI BLOB you have a great time.
 * TODO: Document this "easy route"
 *
 * If Type is LOB_FIRST (assert on this, it gotta be):
 *
 *
 */

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

    pub fn size() -> usize {
        1 + // Version
        1 + // Flags
        4 + // LOB Version
        6 + // trx id
        4 + // undo id
            4 + 6 + FileListBaseNode::size() + FileListBaseNode::size()
    }
}

#[derive(Debug)]
pub struct LobFirst<'a> {
    pub page: &'a Page<'a>,
    pub header: LobFirstHeader,
}

impl<'a> LobFirst<'a> {
    pub fn try_from_page(p: &'a Page<'a>) -> Result<Self> {
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

    pub fn read(&self, offset: usize, buf: &mut [u8]) -> usize {
        let index_array_size = LobIndexEntry::size() * 10; // Hardcoded? somehow see mysql: lob0first.h::node_count()
        let data_len = self.header.data_length as usize;
        let data = &self.body()[index_array_size..][..data_len];
        assert!(offset < data.len(), "offset too large");
        let data = &data[offset..];
        let bytes_to_copy = std::cmp::min(buf.len(), data.len());
        buf[..bytes_to_copy].copy_from_slice(&data[..bytes_to_copy]);
        bytes_to_copy
    }

    pub fn body(&self) -> &[u8] {
        &self.page.body()[LobFirstHeader::size()..]
    }
}

#[derive(Debug, Clone)]
pub struct LobIndexEntry {
    pub file_list_node: FileListInnerNode,
    pub version_list: FileListBaseNode,
    pub creation_transaction_id: u64, // 6 bytes
    pub modify_transaction_id: u64,   // 6 bytes
    pub undo_number: u32,             // Undo Number for the creation transation
    pub undo_number_modify: u32,      // Undo Number for the modify transaction
    pub page_number: u32,
    pub data_length: u16,
    // Two byte gap here
    pub lob_version: u32,
}

impl LobIndexEntry {
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < Self::size() {
            return Err(anyhow!("Buffer not long enough"));
        }

        let mut offset = 0;

        // Parse FileListInnerNode
        let file_list_node_size = FileListInnerNode::size();
        let file_list_node =
            FileListInnerNode::try_from_bytes(&bytes[offset..offset + file_list_node_size])?;
        offset += file_list_node_size;

        // Parse FileListBaseNode
        let version_list_size = FileListBaseNode::size();
        let version_list =
            FileListBaseNode::try_from_bytes(&bytes[offset..offset + version_list_size])?;
        offset += version_list_size;

        // Parse creation_transaction_id (6 bytes)
        let creation_transaction_id = u64::from_be_bytes([
            0,
            0,
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
        ]);
        offset += 6;

        // Parse modify_transaction_id (6 bytes)
        let modify_transaction_id = u64::from_be_bytes([
            0,
            0,
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
        ]);
        offset += 6;

        // Parse undo_number (4 bytes)
        let undo_number = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        // Parse undo_number (4 bytes)
        let undo_number_modify = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        // Parse page_number (4 bytes)
        let page_number = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        // Parse data_length (4 bytes)
        let data_length = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]);
        offset += 2;

        // Gap of 2 byte??? Why
        offset += 2;

        let lob_version = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        Ok(LobIndexEntry {
            file_list_node,
            version_list,
            creation_transaction_id,
            modify_transaction_id,
            undo_number,
            undo_number_modify,
            page_number,
            data_length,
            lob_version,
        })
    }

    pub fn size() -> usize {
        60
    }
}
