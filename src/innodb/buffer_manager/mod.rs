use std::ops::Deref;

use super::page::Page;
use anyhow::{Result, anyhow};

pub mod lru;
pub mod simple;

pub trait BufferManager {
    fn open_page(&self, space_id: u32, offset: u32) -> Result<Page>;
    fn close_page(&self, page: Page);
}

pub struct PageGuard<'a, B: BufferManager> {
    page: Page<'a>,
    buffer_manager: &'a B
}

impl <'a, B: BufferManager> Deref for PageGuard<'a, B> {
    type Target = Page<'a>;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

impl <'a, B: BufferManager> Drop for PageGuard<'a, B> {
    fn drop(&mut self) {
        self.buffer_manager.close_page(std::mem::take(&mut self.page));
    }
}

pub struct DummyBufferMangaer;

impl BufferManager for DummyBufferMangaer {
    fn open_page(&self, _space_id: u32, _offset: u32) -> Result<Page> {
        Err(anyhow!("Dummy buffer manager can't open"))
    }

    fn close_page(&self, _: Page) {
        panic!("This doens't open how can we close");
    }
}