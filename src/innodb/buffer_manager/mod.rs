use super::page::Page;

pub mod heap_buffer_manager;

pub trait BufferManager {
    fn open_page<'a>(&'a mut self, space_id: u64, offset: u64) -> Page<'a>;
    fn close_page(&mut self, page: &Page);
}