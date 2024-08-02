use super::page::Page;
use anyhow::{Result, anyhow};

pub mod lru;
pub mod simple;

pub trait BufferManager {
    fn open_page<'a>(&'a mut self, space_id: u32, offset: u32) -> Result<Page<'a>>;
    fn close_page(&mut self, page: &Page);
}

pub struct DummyBufferMangaer;

impl BufferManager for DummyBufferMangaer {
    fn open_page<'a>(&'a mut self, _space_id: u32, _offset: u32) -> Result<Page<'a>> {
        Err(anyhow!("Dummy buffer manager can't open"))
    }

    fn close_page(&mut self, _: &Page) {
    }
}