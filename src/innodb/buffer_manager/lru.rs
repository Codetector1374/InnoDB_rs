use super::BufferManager;
use anyhow::{anyhow, Result};
use crate::innodb::page::Page;

pub struct LRUBufferManager {}

impl BufferManager for LRUBufferManager {
    fn open_page<'a>(&'a mut self, space_id: u32, offset: u32) -> Result<Page<'a>> {
        todo!()
    }

    fn close_page(&mut self, page: &Page) {
        todo!()
    }
}
