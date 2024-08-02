use super::BufferManager;
use anyhow::Result;

pub struct LRUBufferManager {}

impl BufferManager for LRUBufferManager {
    fn open_page<'a>(&'a mut self, space_id: u32, offset: u32) -> Result<crate::innodb::page::Page<'a>> {
        todo!()
    }

    fn close_page(&mut self, page: &crate::innodb::page::Page) {
        todo!()
    }
}
