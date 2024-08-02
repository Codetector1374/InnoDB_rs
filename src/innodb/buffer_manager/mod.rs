use super::page::Page;
use anyhow::Result;

pub mod lru;
pub mod simple;

pub trait BufferManager {
    fn open_page<'a>(&'a mut self, space_id: u32, offset: u32) -> Result<Page<'a>>;
    fn close_page(&mut self, page: &Page);
}