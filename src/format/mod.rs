//! On-disk format constants and shared types.

pub(crate) mod headers;
pub(crate) mod index;
mod tests;

pub const VERSION_1: u32 = 1;

/// Compression codec IDs (serialized as u32 LE)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum CodecId {
    None = 0,
    Zstd = 1,
}
impl From<u32> for CodecId {
    fn from(v: u32) -> Self {
        match v {
            0 => CodecId::None,
            1 => CodecId::Zstd,
            _ => CodecId::None,
        }
    }
}
impl From<CodecId> for u32 {
    fn from(c: CodecId) -> u32 {
        c as u32
    }
}
