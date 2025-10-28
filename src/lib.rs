pub mod codec;
mod config;
mod error;
mod format;
mod index_inmem;
mod io;
mod reader;
mod tests;
mod transaction;
mod writer;
mod writer_inner;

pub use crate::config::StreamConfig;
pub use crate::error::StreamError;
pub use crate::index_inmem::{InMemIndex, IndexRef};
pub use crate::reader::AsyncStreamReader;
pub use crate::transaction::{TailFrom, Transaction};
pub use crate::writer::AsyncStreamWriter;

#[cfg(feature = "serde-bincode")]
pub use crate::codec::serde_bincode::SerdeBincode;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Offset(pub u64);

impl Offset {
    pub fn saturating_add(&self, o: u32) -> Self {
        Self(self.0.saturating_add(o as u64))
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Compression {
    None,
    Zstd { level: i32 },
}
