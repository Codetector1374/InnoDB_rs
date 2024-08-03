use super::{BufferManager, PageGuard};
use anyhow::{anyhow, Result};
use crate::innodb::page::Page;

pub struct LRUBufferManager {}

impl BufferManager for LRUBufferManager {
    fn open_page(&self, space_id: u32, offset: u32) -> Result<PageGuard> {
        todo!()
    }

    fn close_page(&self, page: Page) {
        todo!()
    }
}
