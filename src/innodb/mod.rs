pub mod charset;
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
    InvalidPageType { expected: PageType, has: PageType },
}

impl Display for InnoDBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Error for InnoDBError {}
