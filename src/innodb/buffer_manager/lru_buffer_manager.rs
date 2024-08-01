use super::BufferManager;

pub struct LRUBufferManager {}

impl BufferManager for LRUBufferManager {
    fn open_page<'a>(&'a mut self, space_id: u64, offset: u64) -> crate::innodb::page::Page<'a> {
        todo!()
    }

    fn close_page(&mut self, page: &crate::innodb::page::Page) {
        todo!()
    }
}
