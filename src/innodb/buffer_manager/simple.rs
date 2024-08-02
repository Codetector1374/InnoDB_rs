use anyhow::Result;
use std::{
    collections::HashMap, fs::File, io::{BufRead, BufReader, Read, Seek, SeekFrom}, path::{Path, PathBuf}, rc::Rc, slice
};

use crate::innodb::page::{Page, FIL_PAGE_SIZE};

use super::BufferManager;

pub struct SimpleBufferManager {
    page_directory: PathBuf,
    page_cache: HashMap<(u32, u32), Box<[u8]>>,
}

impl SimpleBufferManager {
    pub fn new<P>(dir: P) -> Self where P: AsRef<Path> {
        SimpleBufferManager {
            page_directory: dir.as_ref().to_owned(),
            page_cache: HashMap::new(),
        }
    }

    fn get_page(&mut self, space_id: u32, offset: u32) -> Result<&[u8]> {
        if let Some(buf) = self.page_cache.get(&(space_id, offset)) {
            assert_eq!(buf.len(), FIL_PAGE_SIZE);
            let ptr = buf.as_ptr();
            return Ok(unsafe { slice::from_raw_parts(ptr, FIL_PAGE_SIZE)});
        } else {
            let path_path = self.page_directory.join(format!("{:08}.pages", space_id));
            let mut buf_reader = BufReader::new(File::open(&path_path)?);
            buf_reader.seek(SeekFrom::Start(offset as u64 * FIL_PAGE_SIZE as u64))?;
            let mut buf = Box::new([0u8; FIL_PAGE_SIZE]);
            buf_reader.read_exact(buf.as_mut())?;
            self.page_cache.insert((space_id, offset), buf);
            let ptr = self.page_cache.get(&(space_id, offset)).expect("???").as_ptr();
            return Ok(unsafe { slice::from_raw_parts(ptr, FIL_PAGE_SIZE)});
        }
    }
}

impl BufferManager for SimpleBufferManager {
    fn open_page<'a>(
        &'a mut self,
        space_id: u32,
        offset: u32,
    ) -> Result<crate::innodb::page::Page<'a>> {
        let buf = self.get_page(space_id, offset)?;
        Page::from_bytes(buf)
    }

    fn close_page(&mut self, page: &crate::innodb::page::Page) {
        // Nothing
    }
}
