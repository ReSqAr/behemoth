use crate::format::VERSION_1;
use std::io::{self, Read, Write};

pub const MAGIC_INDEX_FILE: [u8; 8] = *b"RSEVIDX\0";
pub const MAGIC_INDEX_ENTRY: [u8; 8] = *b"RSEVIDE\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexHeader {
    pub version: u32,
    pub flags: u32,
}

impl Default for IndexHeader {
    fn default() -> Self {
        Self {
            version: VERSION_1,
            flags: 0,
        }
    }
}

impl IndexHeader {
    pub const SIZE: usize = 48;

    pub fn encode_to<W: Write>(&self, mut w: W) -> io::Result<()> {
        w.write_all(&MAGIC_INDEX_FILE)?;
        w.write_all(&self.version.to_le_bytes())?;
        w.write_all(&self.flags.to_le_bytes())?;
        w.write_all(&[0u8; Self::SIZE - (8 + 4 + 4)])?;
        Ok(())
    }
    pub fn decode_from<R: Read>(mut r: R) -> io::Result<Self> {
        let mut magic = [0u8; 8];
        r.read_exact(&mut magic)?;
        let version = read_u32_le(&mut r)?;
        let flags = read_u32_le(&mut r)?;
        let mut pad = [0u8; Self::SIZE - (8 + 4 + 4)];
        r.read_exact(&mut pad)?;
        if magic != MAGIC_INDEX_FILE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad index file magic",
            ));
        }
        if version != VERSION_1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported index version",
            ));
        }
        let hdr = Self { version, flags };
        Ok(hdr)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub version: u32,
    pub first_id: u64,
    pub last_id: u64,
    pub file_offset: u64,
    pub block_len: u32,
    pub records: u32,
    pub uncompressed_len: u32,
    pub compressed_len: u32,
    pub flags: u32,
}

impl IndexEntry {
    pub const SIZE: usize = 64;

    pub fn encode_to<W: Write>(&self, mut w: W) -> io::Result<()> {
        w.write_all(&MAGIC_INDEX_ENTRY)?;
        w.write_all(&self.version.to_le_bytes())?;
        w.write_all(&self.first_id.to_le_bytes())?;
        w.write_all(&self.last_id.to_le_bytes())?;
        w.write_all(&self.file_offset.to_le_bytes())?;
        w.write_all(&self.block_len.to_le_bytes())?;
        w.write_all(&self.records.to_le_bytes())?;
        w.write_all(&self.uncompressed_len.to_le_bytes())?;
        w.write_all(&self.compressed_len.to_le_bytes())?;
        w.write_all(&self.flags.to_le_bytes())?;
        w.write_all(&[0u8; Self::SIZE - (8 + 4 + 8 + 8 + 8 + 4 + 4 + 4 + 4 + 4)])?;
        Ok(())
    }

    pub fn decode_from<R: Read>(mut r: R) -> io::Result<Self> {
        let mut magic = [0u8; 8];
        r.read_exact(&mut magic)?;
        let version = read_u32_le(&mut r)?;
        let first_id = read_u64_le(&mut r)?;
        let last_id = read_u64_le(&mut r)?;
        let file_offset = read_u64_le(&mut r)?;
        let block_len = read_u32_le(&mut r)?;
        let records = read_u32_le(&mut r)?;
        let uncompressed_len = read_u32_le(&mut r)?;
        let compressed_len = read_u32_le(&mut r)?;
        let flags = read_u32_le(&mut r)?;
        let mut pad = [0u8; Self::SIZE - (8 + 4 + 8 + 8 + 8 + 4 + 4 + 4 + 4 + 4)];
        r.read_exact(&mut pad)?;

        if magic != MAGIC_INDEX_ENTRY {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad index file magic",
            ));
        }
        if version != VERSION_1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported index entry version",
            ));
        }

        let s = Self {
            version,
            first_id,
            last_id,
            file_offset,
            block_len,
            records,
            uncompressed_len,
            compressed_len,
            flags,
        };
        Ok(s)
    }
}

#[inline]
fn read_u32_le<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}
#[inline]
fn read_u64_le<R: Read>(r: &mut R) -> io::Result<u64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}
