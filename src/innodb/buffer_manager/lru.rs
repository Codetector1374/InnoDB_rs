use std::{
    borrow::BorrowMut,
    cell::RefCell,
    collections::{HashMap, LinkedList},
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    slice,
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
    time::SystemTime,
    usize,
};

use super::{BufferManager, PageGuard};
use crate::innodb::{
    page::{Page, FIL_PAGE_SIZE},
    InnoDBError,
};
use anyhow::{anyhow, Result};
use tracing::{trace, warn};

const LRU_PAGE_COUNT: usize = 5;

pub struct LRUBufferManager {
    backing_store: Vec<[u8; FIL_PAGE_SIZE]>,
    page_pin_counter: RefCell<Vec<u32>>,
    page_directory: PathBuf,
    page_pin_map: RefCell<HashMap<(u32, u32), usize>>,
    lru_list: RefCell<Vec<u64>>,
}

impl LRUBufferManager {
    pub fn new<P>(dir: P) -> Self
    where
        P: AsRef<Path>,
    {
        let mut buffer_manager = LRUBufferManager {
            backing_store: Vec::new(),
            page_pin_counter: RefCell::new(Vec::new()),
            page_directory: dir.as_ref().to_owned(),
            page_pin_map: RefCell::new(HashMap::new()),
            lru_list: RefCell::new(Vec::new()),
        };
        buffer_manager
            .backing_store
            .resize(LRU_PAGE_COUNT, [0u8; FIL_PAGE_SIZE]);
        buffer_manager
            .page_pin_counter
            .borrow_mut()
            .resize(LRU_PAGE_COUNT, 0);
        buffer_manager
            .lru_list
            .borrow_mut()
            .resize(LRU_PAGE_COUNT, 0);
        return buffer_manager;
    }

    pub fn find_free(&self) -> usize {
        let mut min_timestamp = u64::MAX;
        let mut result_frame = 0;
        let page_pin_counter = self.page_pin_counter.borrow();
        for (idx, timestamp) in self.lru_list.borrow().iter().enumerate() {
            if *timestamp == 0 {
                return idx;
            }
            // find unpinned page
            if *timestamp < min_timestamp && page_pin_counter[idx] == 0 {
                min_timestamp = *timestamp;
                result_frame = idx;
            }
        }
        if min_timestamp != u64::MAX {
            let mut borrowed_pin_map = self.page_pin_map.borrow_mut();
            let ((space_id, offset), _) = borrowed_pin_map
                .iter()
                .find(|(_, val)| **val == result_frame)
                .expect("can't find the frame")
                .to_owned();
            let (space_id, offset) = (*space_id, *offset);
            borrowed_pin_map.remove(&(space_id, offset));
            return result_frame;
        } else {
            panic!("pin too many pages, \nState: {:#?}", self);
        }
    }
}

impl std::fmt::Debug for LRUBufferManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LRUBufferManager")
            .field("page_pin_counter", &self.page_pin_counter)
            .field("page_directory", &self.page_directory)
            .field("page_pin_map", &self.page_pin_map)
            .field("lru_list", &self.lru_list)
            .finish()
    }
}

impl BufferManager for LRUBufferManager {
    fn pin(&self, space_id: u32, offset: u32) -> Result<PageGuard> {
        trace!("Pinning {}, {}", space_id, offset);
        let cur_sys_time = SystemTime::now();
        let current_time = cur_sys_time.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;

        // If we have the page already pinned
        if let Some(frame_number) = self.page_pin_map.borrow().get(&(space_id, offset)) {
            self.page_pin_counter.borrow_mut()[*frame_number] += 1;
            self.lru_list.borrow_mut()[*frame_number] = current_time;
            let page = Page::from_bytes(&self.backing_store[offset as usize])?;
            return Ok(PageGuard::new(page, self));
        }

        // If we don't have page already pinned
        let mut file = File::open(self.page_directory.join(format!("{:08}.pages", space_id)))?;
        file.seek(SeekFrom::Start(offset as u64 * FIL_PAGE_SIZE as u64))?;
        let free_frame = self.find_free();
        file.read_exact(unsafe {
            let selected_frame = &self.backing_store[free_frame];
            slice::from_raw_parts_mut(selected_frame.as_ptr() as *mut u8, FIL_PAGE_SIZE)
        })?;

        self.lru_list.borrow_mut()[free_frame] = current_time;
        let page = Page::from_bytes(&self.backing_store[free_frame as usize])?;
        if page.header.space_id == 0 && page.header.offset == 0 {
            return Err(anyhow!(InnoDBError::PageNotFound));
        }

        assert_eq!(page.header.space_id, space_id);
        assert_eq!(page.header.offset, offset);
        assert_eq!(page.header.new_checksum, page.crc32_checksum());

        self.page_pin_counter.borrow_mut()[free_frame] += 1;

        self.page_pin_map
            .borrow_mut()
            .insert((space_id, offset), free_frame);

        return Ok(PageGuard::new(page, self));
    }

    fn unpin(&self, page: Page) {
        let space_id = page.header.space_id;
        let offset = page.header.offset;
        trace!("Unpinning {}, {}", space_id, offset);
        if let Some(frame_number) = self.page_pin_map.borrow().get(&(space_id, offset)) {
            self.page_pin_counter.borrow_mut()[*frame_number] -= 1;
        } else {
            panic!("Unpinning a non-pinned page");
        }
    }
}
