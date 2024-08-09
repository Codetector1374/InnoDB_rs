use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    ops::Deref,
    sync::Arc,
};

use crate::innodb::{
    buffer_manager::{BufferManager},
    page::{
        index::record::{Record, RECORD_HEADER_FIXED_LENGTH},
        lob::{data_page::LobData, LobFirst, LobIndexEntry},
    },
    table::blob_header::ExternReference,
    InnoDBError,
};

use super::{
    field::{Field, FieldValue},
    TableDefinition,
};

use anyhow::{anyhow, Result};
use tracing::{trace, warn};

pub struct Row<'a> {
    td: Arc<TableDefinition>,
    // Field Index, Null or Not
    null_map: HashMap<usize, bool>,
    extern_fields: HashSet<usize>,

    // Field Index, length
    field_len_map: HashMap<usize, u64>,
    pub record: Record<'a>,
}

impl<'a> Debug for Row<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Row")
            .field("null_map", &self.null_map)
            .field("field_len_map", &self.field_len_map)
            .field("record", &self.record)
            .finish()
    }
}

impl<'a> Row<'a> {
    pub fn try_from_record_and_table(r: &Record<'a>, td: &Arc<TableDefinition>) -> Result<Row<'a>> {
        let mut byte_stream = r.buf[..(r.offset - RECORD_HEADER_FIXED_LENGTH)]
            .iter()
            .rev();

        let mut extern_fields: HashSet<usize> = HashSet::new();

        // Map of null bits: <Field Idx, null_bit>
        let mut null_field_map: HashMap<usize, usize> = HashMap::new();
        for (idx, field) in td
            .cluster_columns
            .iter()
            .chain(td.data_columns.iter())
            .enumerate()
        {
            if field.nullable {
                null_field_map.insert(idx, null_field_map.len());
            }
        }

        let num_null_flag_bytes = null_field_map.len().div_ceil(8);
        let mut null_bits_remain = null_field_map.len();
        let mut null_bits: Vec<bool> = Vec::new();
        for i in 0..num_null_flag_bytes {
            let byte = byte_stream.next().unwrap();
            for bit in 0..8 {
                let is_null = ((byte >> bit) & 1) != 0;
                null_bits.push(is_null);
                null_bits_remain -= 1;
                if null_bits_remain == 0 {
                    assert_eq!(i, num_null_flag_bytes - 1);
                    break;
                }
            }
        }
        assert_eq!(null_bits.len(), null_field_map.len());
        let null_map: HashMap<usize, bool> = null_field_map
            .iter()
            .map(|(k, v)| (*k, null_bits[*v]))
            .collect();

        let mut length_map: HashMap<usize, u64> = HashMap::new();
        for (idx, field) in td
            .cluster_columns
            .iter()
            .chain(td.data_columns.iter())
            .enumerate()
        {
            if field.field_type.is_variable() {
                // NULL Fields don't have length?
                if field.nullable && null_map[&idx] {
                    continue;
                }
                let mut len: u64 = *byte_stream.next().unwrap() as u64;

                /* If the maximum length of the field
                is up to 255 bytes, the actual length
                is always stored in one byte. If the
                maximum length is more than 255 bytes,
                the actual length is stored in one
                byte for 0..127.  The length will be
                encoded in two bytes when it is 128 or
                more, or when the field is stored
                externally. */
                if field.field_type.max_len() > 255 {
                    // 2 bytes
                    if (len & 0x80) != 0 {
                        let byte2 = *byte_stream.next().unwrap();
                        let tmp = (len << 8) | byte2 as u64;
                        len = tmp & 0x3FFF;
                        if tmp & 0x4000 != 0 {
                            extern_fields.insert(idx);
                        }
                    }
                }
                length_map.insert(idx, len);
            }
        }

        Ok(Row {
            td: td.clone(),
            null_map,
            field_len_map: length_map,
            record: r.clone(),
            extern_fields,
        })
    }

    fn load_extern(
        &self,
        extern_header: &ExternReference,
        buffer_mgr: &dyn BufferManager,
    ) -> Result<Box<[u8]>> {
        let space_id = extern_header.space_id;
        let first_page_number = extern_header.page_number;
        let lob_first_page = buffer_mgr.pin(space_id, first_page_number)?;
        if lob_first_page.header.offset != extern_header.page_number {
            return Err(anyhow!(InnoDBError::InvalidPage));
        }
        let lob_first = LobFirst::try_from_page(lob_first_page.deref())?;
        let index_list = &lob_first.header.index_list_head;
        trace!("LOB First: {:#?}", lob_first);

        let mut node_location = index_list.first_node;
        let mut page_offset = 0;

        let mut output_buffer = Vec::<u8>::new();
        let mut filled = 0usize;
        output_buffer.resize(extern_header.length as usize, 0);

        while !node_location.is_null() {
            trace!("Inspecting Node at offset {}", node_location.offset);
            assert_eq!(
                index_list.first_node.page_number, lob_first.page.header.offset,
                "assumption"
            );
            let buf = &lob_first.page.raw_data[node_location.offset as usize..];
            let node = LobIndexEntry::try_from_bytes(buf)?;
            trace!("Index Node: {:#?}", node);

            let bytes_read;
            if node.page_number == first_page_number {
                bytes_read = lob_first.read(page_offset, &mut output_buffer[filled..]);
                trace!(
                    "Read {} bytes from first page, in total expecting {} bytes",
                    bytes_read,
                    output_buffer.len()
                );
            } else {
                let page_guard = buffer_mgr.pin(space_id, node.page_number)?;
                let data_page = LobData::try_from_page(&page_guard)?;
                trace!("Data page: {:#?}", data_page);
                bytes_read = data_page.read(page_offset, &mut output_buffer[filled..]);
                trace!("Read {} bytes from data page", bytes_read);
            }
            filled += bytes_read;
            page_offset = page_offset.saturating_sub(bytes_read);

            node_location = node.file_list_node.next;
        }

        if filled < output_buffer.len() {
            warn!("huh {}, {}", filled, output_buffer.len());
            return Err(anyhow!("Read incomplete"));
        }

        Ok(output_buffer.into())
    }

    fn parse_extern_field(
        &self,
        f: &Field,
        extern_header: &ExternReference,
        buffer_mgr: &dyn BufferManager,
    ) -> FieldValue {
        // Load a page
        match self.load_extern(extern_header, buffer_mgr) {
            Ok(buf) => f.parse(&buf, Some(extern_header.length)).0,
            Err(err) => {
                warn!(
                    "Failed to open extern {:?}, error: {:?}",
                    extern_header, err
                );
                FieldValue::Skipped
            }
        }
    }

    fn parse_single_field(
        &self,
        f: &Field,
        buf: &[u8],
        idx: usize,
        buf_mgr: &dyn BufferManager,
    ) -> (FieldValue, usize) {
        if self.extern_fields.contains(&idx) {
            let len = *self.field_len_map.get(&idx).unwrap() as usize;
            assert_eq!(len, 20, "Extern header should be 20 bytes long");
            let extern_header =
                ExternReference::from_bytes(&buf[0..len]).expect("Can't make blob header");
            trace!("Extern Header: {:?}", &extern_header);
            (
                self.parse_extern_field(f, &extern_header, buf_mgr),
                len,
            )
        } else {
            let (value, len) = f.parse(buf, self.field_len_map.get(&idx).cloned());
            (value, len)
        }
    }

    /// Only call on primary index
    pub fn parse_values(&self, buffer_mgr: &dyn BufferManager) -> Vec<FieldValue> {
        let mut values = Vec::new();
        let mut current_offset = self.record.offset;
        let num_pk = self.td.cluster_columns.len();
        assert_ne!(num_pk, 0, "Table must have PK");

        for (idx, f) in self.td.cluster_columns.iter().enumerate() {
            let (value, consumed) =
                self.parse_single_field(f, &self.record.buf[current_offset..], idx, buffer_mgr);
            current_offset += consumed;
            values.push(value);
        }
        // Hidden Columns
        current_offset += 6 + 7;

        let cluster_count = self.td.cluster_columns.len();
        for (idx, f) in self.td.data_columns.iter().enumerate() {
            let idx = idx + cluster_count;
            let (value, consumed) =
                self.parse_single_field(f, &self.record.buf[current_offset..], idx, buffer_mgr);
            current_offset += consumed;
            values.push(value);
        }

        values
    }
}
