use anyhow::{anyhow, Result};

use crate::innodb::{
    page::{Page, PageType},
    InnoDBError,
};

#[derive(Debug, Clone)]
pub struct LobDataHeader {
    pub version: u8,
    pub data_len: u32,
    pub trx_id: u64, // 6 bytes
}

impl LobDataHeader {
    pub fn size() -> usize {
        1 + 4 + 6
    }

    pub fn try_from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < 11 {
            return Err(anyhow!("Buffer too short for LobDataHeader"));
        }

        let version = buf[0];
        let data_len = u32::from_be_bytes(buf[1..5].try_into()?);

        // trx_id is 6 bytes, so we need to pad it with two zero bytes for u64
        let trx_id = u64::from_be_bytes([0, 0, buf[5], buf[6], buf[7], buf[8], buf[9], buf[10]]);

        Ok(LobDataHeader {
            version,
            data_len,
            trx_id,
        })
    }
}

#[derive(Debug)]
pub struct LobData<'a> {
    pub page: &'a Page<'a>,
    pub header: LobDataHeader,
}

impl<'a> LobData<'a> {
    pub fn try_from_page(p: &'a Page<'a>) -> Result<Self> {
        match p.header.page_type {
            PageType::LobData => Ok(LobData {
                header: LobDataHeader::try_from_bytes(p.body())?,
                page: p,
            }),
            _ => Err(anyhow!(InnoDBError::InvalidPageType {
                expected: PageType::LobData,
                has: p.header.page_type
            })),
        }
    }

    pub fn read(&self, offset: usize, buf: &mut [u8]) -> usize {
        let data_len = self.header.data_len as usize;
        let data = &self.body()[..data_len];
        assert!(offset < data.len(), "offset too large");
        let data = &data[offset..];
        let bytes_to_copy = std::cmp::min(buf.len(), data.len());
        buf[..bytes_to_copy].copy_from_slice(&data[..bytes_to_copy]);
        bytes_to_copy
    }

    pub fn body(&self) -> &[u8] {
        &self.page.body()[LobDataHeader::size()..]
    }
}
