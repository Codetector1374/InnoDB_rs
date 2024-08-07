pub mod index;
pub mod lob;

use std::fmt::Debug;

use anyhow::{Error, Result};
use crc::{Crc, CRC_32_ISCSI};
use num_enum::TryFromPrimitive;
use tracing::debug;

// #define UT_HASH_RANDOM_MASK     1463735687
// #define UT_HASH_RANDOM_MASK2    1653893711
const HASH_RANDOM_MASK: u32 = 1_463_735_687;
const HASH_RANDOM_MASK2: u32 = 1_653_893_711;

pub const FIL_PAGE_SIZE: usize = 16384;
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

#[derive(Default, PartialEq)]
pub struct Page<'a> {
    // pub space_id: u32,
    pub header: FILHeader,
    pub trailer: FILTrailer,
    pub raw_data: &'a [u8],
}

impl<'a> Debug for Page<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Page")
            .field("header", &self.header)
            .field("trailer", &self.trailer)
            .finish()
    }
}

impl<'a> Page<'a> {
    pub fn from_bytes(buf: &'a [u8]) -> Result<Page<'a>> {
        if buf.len() != 16384 {
            return Err(Error::msg("Page is 16kB"));
        }

        let header = FILHeader::from_bytes(&buf[0..38])?;

        Ok(Page {
            // space_id: header.space_id,
            header,
            trailer: FILTrailer::from_bytes(&buf[(FIL_PAGE_SIZE - FIL_TRAILER_SIZE)..])?,
            raw_data: buf,
        })
    }

    pub fn partial_page_header(&self) -> &[u8] {
        &self.raw_data[FIL_HEADER_PARTIAL_OFFSET..][..FIL_HEADER_PARTIAL_SIZE]
    }

    pub fn body(&self) -> &[u8] {
        &self.raw_data[FIL_PAGE_BODY_OFFSET..][..FIL_PAGE_BODY_SIZE]
    }

    pub fn innodb_checksum(&self) -> u32 {
        let header_checksum = fold_bytes(self.partial_page_header());
        let body_checksum = fold_bytes(self.body());
        header_checksum.wrapping_add(body_checksum)
    }

    pub fn crc32_checksum(&self) -> u32 {
        CRC32C.checksum(self.partial_page_header()) ^ CRC32C.checksum(self.body())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, TryFromPrimitive)]
#[repr(u16)]
pub enum PageType {
    /// Freshly allocated
    Allocated = 0,
    /// Undo log
    UndoLog = 2,
    /// File segment inode
    Inode = 3,
    /// Insert buffer free list
    IbufFreeList = 4,
    /// Insert buffer bitmap
    IbufBitmap = 5,
    /// System internal
    Sys = 6,
    /// Transaction system header
    TrxSys = 7,
    /// File space header
    FspHdr = 8,
    /// Extent descriptor
    Xdes = 9,
    /// Uncompressed BLOB
    Blob = 10,
    /// First compressed BLOB
    Zblob = 11,
    /// Subsequent compressed BLOB
    Zblob2 = 12,
    /// Unknown
    Unknown = 13,
    /// Compressed
    Compressed = 14,
    /// Encrypted
    Encrypted = 15,
    /// Compressed and Encrypted
    CompressedAndEncrypted = 16,
    /// Encrypted R-tree
    EncryptedRtree = 17,
    /// Uncompressed SDI BLOB
    SdiBlob = 18,
    /// Compressed SDI BLOB
    SdiZblob = 19,
    /// Legacy doublewrite buffer
    LegacyDblwr = 20,
    /// Rollback Segment Array
    RsegArray = 21,
    /// Index of uncompressed LOB
    LobIndex = 22,
    /// Data of uncompressed LOB
    LobData = 23,
    /// First page of an uncompressed LOB
    LobFirst = 24,
    /// First page of a compressed LOB
    ZlobFirst = 25,
    /// Data of compressed LOB
    ZlobData = 26,
    /// Index of compressed LOB
    ZlobIndex = 27,
    /// Fragment of compressed LOB
    ZlobFrag = 28,
    /// Index of fragment for compressed LOB
    ZlobFragEntry = 29,
    /// Serialized Dictionary Information
    SDI = 17853,
    /// R-tree index
    RTree = 17854,
    /// B+Tree index
    Index = 17855,
}

#[allow(clippy::derivable_impls)]
impl Default for PageType {
    fn default() -> Self {
        PageType::Allocated
    }
}

#[derive(Default, Debug, Clone, PartialEq)]
pub struct FILHeader {
    pub new_checksum: u32,
    pub offset: u32, // offset (page number)
    pub prev: u32,
    pub next: u32,
    pub lsn: u64,
    pub page_type: PageType,
    pub flush_lsn: u64,
    pub space_id: u32,
}

impl FILHeader {
    pub fn from_bytes(buffer: &[u8]) -> Result<FILHeader> {
        if buffer.len() < 38 {
            return Err(Error::msg("Slice is not long enough"));
        }

        let new_checksum = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
        let offset = u32::from_be_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]);
        let prev = u32::from_be_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]);
        let next = u32::from_be_bytes([buffer[12], buffer[13], buffer[14], buffer[15]]);
        let lsn = u64::from_be_bytes([
            buffer[16], buffer[17], buffer[18], buffer[19], buffer[20], buffer[21], buffer[22],
            buffer[23],
        ]);
        let page_type_value = u16::from_be_bytes([buffer[24], buffer[25]]);
        let page_type = match PageType::try_from_primitive(page_type_value) {
            Ok(page_type) => page_type,
            Err(e) => {
                debug!("Invalid FIL PageType: {:?}", e);
                PageType::Unknown
            }
        };
        let flush_lsn = u64::from_be_bytes([
            buffer[26], buffer[27], buffer[28], buffer[29], buffer[30], buffer[31], buffer[32],
            buffer[33],
        ]);
        let space_id = u32::from_be_bytes([buffer[34], buffer[35], buffer[36], buffer[37]]);

        Ok(FILHeader {
            new_checksum,
            offset,
            prev,
            next,
            lsn,
            page_type,
            flush_lsn,
            space_id,
        })
    }
}

#[derive(Default, Debug, Clone, PartialEq)]
pub struct FILTrailer {
    pub old_checksum: u32,
    pub lsn_low_32: u32,
}

impl FILTrailer {
    pub fn from_bytes(buffer: &[u8]) -> Result<FILTrailer> {
        if buffer.len() != FIL_TRAILER_SIZE {
            return Err(Error::msg("tariler is 8 bytes"));
        }

        let old_checksum = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
        let lsn_low_32 = u32::from_be_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]);

        Ok(FILTrailer {
            old_checksum,
            lsn_low_32,
        })
    }
}
