use std::fmt::Debug;

use anyhow::{Error, Result};
use crc::{Crc, CRC_32_ISCSI};

// #define UT_HASH_RANDOM_MASK     1463735687
// #define UT_HASH_RANDOM_MASK2    1653893711
const HASH_RANDOM_MASK: u32 = 1_463_735_687;
const HASH_RANDOM_MASK2: u32 = 1_653_893_711;

const FIL_PAGE_SIZE: usize = 16384;
const FIL_TRAILER_SIZE: usize = 8;

const FIL_HEADER_OFFSET: usize = 0;
const FIL_HEADER_SIZE: usize = 38;

/// Skips CHECKSUM field (4 bytes)
const FIL_HEADER_PARTIAL_OFFSET: usize = 4;

/// Excludes Checksum(4), FlushLsn(8), SpaceId(4)
const FIL_HEADER_PARTIAL_SIZE: usize = FIL_HEADER_SIZE - 4 - 8 - 4;

const FIL_PAGE_BODY_OFFSET: usize = FIL_HEADER_OFFSET + FIL_HEADER_SIZE;
const FIL_PAGE_BODY_SIZE: usize = FIL_PAGE_SIZE - FIL_HEADER_SIZE - FIL_TRAILER_SIZE;

const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

fn fold_pair(n1: u32, n2: u32) -> u32 {
    ((((n1 ^ n2 ^ HASH_RANDOM_MASK2) << 8).wrapping_add(n1)) ^ HASH_RANDOM_MASK).wrapping_add(n2)
}

fn fold_bytes(buf: &[u8]) -> u32 {
    let mut fold = 0;

    for b in buf {
        fold = fold_pair(fold, (*b) as u32);
    }

    fold
}

#[derive(Default, Clone, PartialEq)]
pub struct Page {
    pub header: FILHeader,
    pub trailer: FILTrailer,
    pub raw_data: Vec<u8>,
}

impl Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Page")
            .field("header", &self.header)
            .field("trailer", &self.trailer)
            .field("data", &"hidden")
            .finish()
    }
}

impl Page {
    pub fn from_bytes(buf: &[u8]) -> Result<Page> {
        if buf.len() != 16384 {
            return Err(Error::msg("Page is 16kB"));
        }

        let mut page = Page::default();
        page.header = FILHeader::from_bytes(&buf[0..38])?;
        page.trailer = FILTrailer::from_bytes(&buf[(FIL_PAGE_SIZE - FIL_TRAILER_SIZE)..])?;
        page.raw_data = Vec::from(buf);

        Ok(page)
    }

    pub fn partial_page_header(&self) -> &[u8] {
        &self.raw_data[FIL_HEADER_PARTIAL_OFFSET..][..FIL_HEADER_PARTIAL_SIZE]
    }

    pub fn page_body(&self) -> &[u8] {
        &self.raw_data[FIL_PAGE_BODY_OFFSET..][..FIL_PAGE_BODY_SIZE]
    }

    pub fn innodb_checksum(&self) -> u32 {
        let header_checksum = fold_bytes(self.partial_page_header());
        let body_checksum = fold_bytes(self.page_body());
        header_checksum.wrapping_add(body_checksum)
    }

    pub fn crc32_checksum(&self) -> u32 {
        CRC32C.checksum(self.partial_page_header()) ^ CRC32C.checksum(self.page_body())
    }
}

#[derive(Default, Debug, Clone, PartialEq)]
pub struct FILHeader {
    pub new_checksum: u32,
    pub offset: u32,
    pub prev: u32,
    pub next: u32,
    pub lsn: u64,
    pub page_type: u16,
    pub flush_lsn: u64,
    pub space_id: u32,
}

impl FILHeader {
    pub fn from_bytes(slice: &[u8]) -> Result<FILHeader> {
        if slice.len() < 38 {
            return Err(Error::msg("Slice is not long enough"));
        }

        let mut h = FILHeader::default();

        let (num, slice) = slice.split_at(std::mem::size_of::<u32>());
        h.new_checksum = u32::from_be_bytes(num.try_into()?);

        let (num, slice) = slice.split_at(std::mem::size_of::<u32>());
        h.offset = u32::from_be_bytes(num.try_into()?);

        let (num, slice) = slice.split_at(std::mem::size_of::<u32>());
        h.prev = u32::from_be_bytes(num.try_into()?);

        let (num, slice) = slice.split_at(std::mem::size_of::<u32>());
        h.next = u32::from_be_bytes(num.try_into()?);

        let (num, slice) = slice.split_at(std::mem::size_of::<u64>());
        h.lsn = u64::from_be_bytes(num.try_into()?);

        let (num, slice) = slice.split_at(std::mem::size_of::<u16>());
        h.page_type = u16::from_be_bytes(num.try_into()?);

        let (num, slice) = slice.split_at(std::mem::size_of::<u64>());
        h.flush_lsn = u64::from_be_bytes(num.try_into()?);

        let (num, _) = slice.split_at(std::mem::size_of::<u32>());
        h.space_id = u32::from_be_bytes(num.try_into()?);

        return Ok(h);
    }
}

#[derive(Default, Debug, Clone, PartialEq)]
pub struct FILTrailer {
    pub old_checksum: u32,
    pub lsn_low_32: u32,
}

impl FILTrailer {
    pub fn from_bytes(slice: &[u8]) -> Result<FILTrailer> {
        if slice.len() != FIL_TRAILER_SIZE {
            return Err(Error::msg("tariler is 8 bytes"));
        }

        let mut t = FILTrailer::default();

        let (num, slice) = slice.split_at(std::mem::size_of::<u32>());
        t.old_checksum = u32::from_be_bytes(num.try_into()?);

        let (num, _) = slice.split_at(std::mem::size_of::<u32>());
        t.lsn_low_32 = u32::from_be_bytes(num.try_into()?);

        Ok(t)
    }
}
