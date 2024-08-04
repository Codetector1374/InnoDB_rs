use std::ops::Deref;

use super::page::Page;
use anyhow::{Result, anyhow};

pub mod lru;
pub mod simple;

pub trait BufferManager {
    fn pin(&self, space_id: u32, offset: u32) -> Result<PageGuard>;
    fn unpin(&self, page: Page);
}

pub struct PageGuard<'a> {
    page: Page<'a>,
    buffer_manager: &'a dyn BufferManager
}

impl <'a> PageGuard<'a> {
    pub fn new(page: Page<'a>, buffer_manager: &'a dyn BufferManager) -> Self {
        PageGuard {
            page,
            buffer_manager,
        }
    }
}

impl <'a> Deref for PageGuard<'a> {
    type Target = Page<'a>;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

impl <'a> Drop for PageGuard<'a> {
    fn drop(&mut self) {
        self.buffer_manager.unpin(std::mem::take(&mut self.page));
    }
}

pub struct DummyBufferMangaer;

impl BufferManager for DummyBufferMangaer {
    fn pin(&self, _space_id: u32, _offset: u32) -> Result<PageGuard> {
        Err(anyhow!("Dummy buffer manager can't open"))
    }

    fn unpin(&self, _: Page) {
        panic!("This doens't open how can we close");
    }
}
