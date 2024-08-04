use anyhow::{anyhow, Ok, Result};

pub const FIL_NULL: u32 = 0xFFFF_FFFF;

#[derive(Debug, Clone, Copy)]
pub struct FileAddress {
    pub page_number: u32,
    pub offset: u16,
}

impl FileAddress {
    pub fn new(page_number: u32, offset: u16) -> Self {
        FileAddress {
            page_number,
            offset,
        }
    }

    pub fn try_from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < 6 {
            return Err(anyhow!("Buffer is too small"));
        }

        let page_number = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let offset = u16::from_be_bytes([buf[4], buf[5]]);
        Ok(FileAddress {
            page_number,
            offset,
        })
    }

    pub fn is_null(&self) -> bool {
        self.page_number == FIL_NULL
    }

    fn size() -> usize {
        6
    }
}

#[derive(Debug, Clone)]
pub struct FileListBaseNode {
    pub list_len: u32,
    pub first_node: FileAddress,
    pub last_node: FileAddress,
}

impl FileListBaseNode {
    pub fn try_from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < 16 {
            return Err(anyhow!("Buffer is too small"));
        }

        let list_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let first_node = FileAddress::try_from_bytes(&buf[4..10])?;
        let last_node = FileAddress::try_from_bytes(&buf[10..16])?;

        Ok(FileListBaseNode {
            list_len,
            first_node,
            last_node,
        })
    }

    pub(crate) fn size() -> usize {
        4 + FileAddress::size() + FileAddress::size()
    }
}

#[derive(Debug, Clone)]
pub struct FileListInnerNode {
    pub prev: FileAddress,
    pub next: FileAddress,
}

impl FileListInnerNode {
    pub fn try_from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < 12 {
            return Err(anyhow!("Buffer is too small"));
        }

        let prev = FileAddress::try_from_bytes(&buf[0..6])?;
        let next = FileAddress::try_from_bytes(&buf[6..12])?;

        Ok(FileListInnerNode { prev, next })
    }

    pub fn size() -> usize {
        FileAddress::size() + FileAddress::size()
    }
}
