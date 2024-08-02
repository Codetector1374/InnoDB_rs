use anyhow::{anyhow, Result};

#[derive(Debug, Clone)]
pub struct FileListBaseNode {
    pub list_len: u32,
    pub first_node_page_number: u32,
    pub first_node_offset: u16,
    pub last_node_page_number: u32,
    pub last_node_offset: u16,
}

impl FileListBaseNode {
    pub fn try_from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < 16 {
            return Err(anyhow!("Buffer is too small"));
        }

        let list_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let first_node_page_number = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let first_node_offset = u16::from_be_bytes([buf[8], buf[9]]);
        let last_node_page_number = u32::from_be_bytes([buf[10], buf[11], buf[12], buf[13]]);
        let last_node_offset = u16::from_be_bytes([buf[14], buf[15]]);

        Ok(FileListBaseNode {
            list_len,
            first_node_page_number,
            first_node_offset,
            last_node_page_number,
            last_node_offset,
        })
    }
}

#[derive(Debug, Clone)]
pub struct FileListInnerNode {
    pub prev_node_page_number: u32,
    pub prev_node_offset: u16,
    pub next_node_page_number: u32,
    pub next_node_offset: u16,
}

impl FileListInnerNode {
    pub fn try_from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < 12 {
            return Err(anyhow!("Buffer is too small"));
        }

        let prev_node_page_number = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let prev_node_offset = u16::from_be_bytes([buf[4], buf[5]]);
        let next_node_page_number = u32::from_be_bytes([buf[6], buf[7], buf[8], buf[9]]);
        let next_node_offset = u16::from_be_bytes([buf[10], buf[11]]);

        Ok(FileListInnerNode {
            prev_node_page_number,
            prev_node_offset,
            next_node_page_number,
            next_node_offset,
        })
    }
}
