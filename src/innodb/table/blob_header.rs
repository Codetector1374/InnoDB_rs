use anyhow::Result;

#[derive(Debug, Clone)]
pub struct ExternReference {
    pub space_id: u32,
    pub page_number: u32,
    pub offset: u32,
    pub owner: bool,
    pub inherit: bool,
    pub length: u64,
}

/// B-Tree Extern Reference
impl ExternReference {
    pub fn from_bytes(bytes: &[u8]) -> Result<ExternReference> {
        if bytes.len() < 20 {
            anyhow::bail!("Insufficient bytes to construct BlobHeader");
        }

        let space_id = u32::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3]
        ]);

        let page_number = u32::from_be_bytes([
            bytes[4], bytes[5], bytes[6], bytes[7]
        ]);

        let offset = u32::from_be_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11]
        ]);

        let length = u64::from_be_bytes([
            bytes[12], bytes[13], bytes[14], bytes[15],
            bytes[16], bytes[17], bytes[18], bytes[19]
        ]);

        Ok(ExternReference {
            space_id,
            page_number,
            offset,
            owner: (length & 0x8000_0000_0000_0000u64) == 0,
            inherit: (length & 0x4000_0000_0000_0000u64) != 0,
            // There's technically a is being modified bit, idgaf
            length: length & 0x0FFF_FFFF_FFFF_FFFFu64,
        })
    }
}
