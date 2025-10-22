pub mod codec;
mod config;
mod error;
mod format;
mod index_inmem;
mod io;
mod open_block;
mod reader;
mod tests;
mod writer;
mod writer_inner;

pub use crate::config::StreamConfig;
pub use crate::error::StreamError;
pub use crate::index_inmem::{InMemIndex, IndexRef};
pub use crate::reader::AsyncStreamReader;
pub use crate::writer::AsyncStreamWriter;

#[cfg(feature = "serde-bincode")]
pub use crate::codec::serde_bincode::SerdeBincode;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Offset(pub u64);

#[derive(Clone, Copy, Debug)]
pub enum Compression {
    None,
    Zstd { level: i32 },
}
