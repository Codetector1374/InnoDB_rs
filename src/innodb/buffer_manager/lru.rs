use std::{
    cell::RefCell, collections::{HashMap, LinkedList}, fs::File, io::{Read, Seek, SeekFrom}, path::{Path, PathBuf}, slice, sync::atomic::{AtomicU32, AtomicU64, Ordering}, time::SystemTime, usize
};

use super::{BufferManager, PageGuard};
use crate::innodb::page::{Page, FIL_PAGE_SIZE};
use anyhow::{anyhow, Result};

const LRU_PAGE_COUNT: usize = 100;

pub struct LRUBufferManager {
    backing_store: Vec<[u8; FIL_PAGE_SIZE]>,
    page_pin_counter: Vec<AtomicU32>,
    page_directory: PathBuf,
    page_pin_map: RefCell<HashMap<(u32, u32), usize>>,
    lru_list: Vec<AtomicU64>,
}

impl LRUBufferManager {
    pub fn new<P>(dir: P) -> Self
    where
        P: AsRef<Path>,
    {
        let mut buffer_manager = LRUBufferManager {
            backing_store: Vec::new(),
            page_pin_counter: Vec::new(),
            page_directory: dir.as_ref().to_owned(),
            page_pin_map: RefCell::new(HashMap::new()),
            lru_list: Vec::new(),
        };
        buffer_manager
            .backing_store
            .resize(LRU_PAGE_COUNT, [0u8; FIL_PAGE_SIZE]);
        buffer_manager
            .page_pin_counter
            .resize_with(LRU_PAGE_COUNT, || AtomicU32::new(0));
        buffer_manager
            .lru_list
            .resize_with(LRU_PAGE_COUNT, || AtomicU64::new(0));
        return buffer_manager;
    }

    pub fn find_free(&self) -> usize {
        let mut min_timestamp = u64::MAX;
        let mut result_frame = 0;
        for (idx, timestamp) in self.lru_list.iter().enumerate() {
            let cur_timestamp = timestamp.load(Ordering::Acquire);
            if cur_timestamp == 0 {
                return idx;
            }
            // find unpinned page
            if cur_timestamp < min_timestamp
                && self.page_pin_counter[idx].load(Ordering::Acquire) == 0
            {
                min_timestamp = cur_timestamp;
                result_frame = idx;
            }
        }
        if min_timestamp != u64::MAX {
            let mut borrowed_pin_map = self.page_pin_map.borrow_mut();
            let ((space_id, offset), _) = borrowed_pin_map
                .iter()
                .find(|(_, val)| **val == result_frame)
                .expect("can't find the frame").to_owned();
            let (space_id, offset) = (*space_id, *offset);
            borrowed_pin_map.remove(&(space_id, offset));
            return result_frame;
        } else {
            panic!("pin too many pages");
        }
    }
}

impl BufferManager for LRUBufferManager {
    fn pin(&self, space_id: u32, offset: u32) -> Result<PageGuard> {
        let cur_sys_time = SystemTime::now();
        let duration = cur_sys_time.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        if let Some(frame_number) = self.page_pin_map.borrow().get(&(space_id, offset)) {
            self.page_pin_counter[*frame_number].fetch_add(1, Ordering::SeqCst);
            self.lru_list[*frame_number].store(duration.as_nanos() as u64, Ordering::Release);
            let page = Page::from_bytes(&self.backing_store[offset as usize])?;
            return Ok(PageGuard::new(page, self));
        } else {
            let mut file = File::open(self.page_directory.join(format!("{:08}.pages", space_id)))?;
            file.seek(SeekFrom::Start(offset as u64 * FIL_PAGE_SIZE as u64))?;
            let free_frame = self.find_free();
            file.read_exact(unsafe {
                slice::from_raw_parts_mut(
                    self.backing_store[free_frame].as_ptr() as *mut u8,
                    FIL_PAGE_SIZE,
                )
            })?;
            self.lru_list[free_frame].store(duration.as_nanos() as u64, Ordering::Release);
            let page = Page::from_bytes(&self.backing_store[offset as usize])?;
            assert_eq!(page.header.space_id, space_id);
            assert_eq!(page.header.offset, offset);
            self.page_pin_map.borrow_mut().insert((space_id, offset), free_frame);
            return Ok(PageGuard::new(page, self));
        }
    }

    fn unpin(&self, page: Page) {
        let space_id = page.header.space_id;
        let offset = page.header.offset;
        if let Some(frame_number) = self.page_pin_map.borrow().get(&(space_id, offset)) {
            self.page_pin_counter[*frame_number].fetch_sub(1, Ordering::SeqCst);
        }
    }
}
