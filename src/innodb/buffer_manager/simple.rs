use anyhow::Result;
use tracing::trace;
use std::{
    cell::RefCell,
    collections::HashMap,
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    slice,
};

use crate::innodb::page::{Page, FIL_PAGE_SIZE};

use super::{BufferManager, PageGuard};

pub struct SimpleBufferManager {
    page_directory: PathBuf,
    page_cache: RefCell<HashMap<(u32, u32), Box<[u8]>>>,
}

impl SimpleBufferManager {
    pub fn new<P>(dir: P) -> Self
    where
        P: AsRef<Path>,
    {
        SimpleBufferManager {
            page_directory: dir.as_ref().to_owned(),
            page_cache: RefCell::new(HashMap::new()),
        }
    }

    fn get_page(&self, space_id: u32, offset: u32) -> Result<&[u8]> {
        if let Some(buf) = self.page_cache.borrow().get(&(space_id, offset)) {
            assert_eq!(buf.len(), FIL_PAGE_SIZE);
            let ptr = buf.as_ptr();
            return Ok(unsafe { slice::from_raw_parts(ptr, FIL_PAGE_SIZE) });
        }

        let path_path = self.page_directory.join(format!("{:08}.pages", space_id));
        let mut buf_reader = BufReader::new(File::open(&path_path)?);
        buf_reader.seek(SeekFrom::Start(offset as u64 * FIL_PAGE_SIZE as u64))?;
        let mut buf = Box::new([0u8; FIL_PAGE_SIZE]);
        buf_reader.read_exact(buf.as_mut())?;
        self.page_cache.borrow_mut().insert((space_id, offset), buf);
        let ptr = self
            .page_cache
            .borrow()
            .get(&(space_id, offset))
            .expect("???")
            .as_ptr();
        return Ok(unsafe { slice::from_raw_parts(ptr, FIL_PAGE_SIZE) });
    }
}

impl BufferManager for SimpleBufferManager {
    fn pin(&self, space_id: u32, offset: u32) -> Result<PageGuard> {
        let buf = self.get_page(space_id, offset)?;
        trace!("Opened ({}, {})", space_id, offset);
        Ok(PageGuard::new(Page::from_bytes(buf)?, self))
    }

    fn unpin(&self, page: Page) {
        trace!("Closed ({:?}, {})", page.header.space_id, page.header.offset);
    }
}
