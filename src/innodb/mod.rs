pub mod buffer_manager;
pub mod charset;
pub mod file_list;
pub mod page;
pub mod table;

use std::{
    error::Error,
    fmt::{Debug, Display},
};

use page::PageType;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InnoDBError {
    InvalidLength,
    InvalidChecksum,
    InvalidPage,
    PageNotFound,
    InvalidPageType { expected: PageType, has: PageType },
}

impl Display for InnoDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Error for InnoDBError {}
