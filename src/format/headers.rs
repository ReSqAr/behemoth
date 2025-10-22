use crate::format::{CodecId, VERSION_1};
use std::io::{self, Read, Write};

/// All integers are little-endian.
/// Magics are 8 ASCII bytes, zero-padded.

pub const MAGIC_LOG_FILE: [u8; 8] = *b"RSEVLOG\0";
pub const MAGIC_BLOCK_HDR: [u8; 8] = *b"RSEVBLK\0";
pub const MAGIC_BLOCK_FTR: [u8; 8] = *b"RSEVFLR\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileHeader {
    pub version: u32,
    pub flags: u32,
    pub reserved: [u8; 8],
}

impl Default for FileHeader {
    fn default() -> Self {
        Self {
            version: VERSION_1,
            flags: 0,
            reserved: [0; 8],
        }
    }
}

impl FileHeader {
    pub const SIZE: usize = 32;

    pub fn encode_to<W: Write>(&self, mut w: W) -> io::Result<()> {
        w.write_all(&MAGIC_LOG_FILE)?;
        w.write_all(&self.version.to_le_bytes())?;
        w.write_all(&self.flags.to_le_bytes())?;
        w.write_all(&self.reserved)?;
        // pad to 32 bytes
        w.write_all(&[0u8; Self::SIZE - (8 + 4 + 4 + 8)])?;
        Ok(())
    }

    pub fn decode_from<R: Read>(mut r: R) -> io::Result<Self> {
        let mut magic = [0u8; 8];
        r.read_exact(&mut magic)?;
        let version = read_u32_le(&mut r)?;
        let flags = read_u32_le(&mut r)?;
        let mut reserved = [0u8; 8];
        r.read_exact(&mut reserved)?;
        // consume padding
        let mut pad = [0u8; Self::SIZE - (8 + 4 + 4 + 8)];
        r.read_exact(&mut pad)?;

        if magic != MAGIC_LOG_FILE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad log file magic",
            ));
        }
        if version != VERSION_1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported log file version",
            ));
        }
        let hdr = Self {
            version,
            flags,
            reserved,
        };
        Ok(hdr)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHeader {
    pub version: u32,
    pub codec: CodecId, // u32
}

impl BlockHeader {
    pub const SIZE: usize = 32;

    pub fn new(codec: CodecId) -> Self {
        Self {
            version: VERSION_1,
            codec,
        }
    }

    pub fn encode_to<W: Write>(&self, mut w: W) -> io::Result<()> {
        w.write_all(&MAGIC_BLOCK_HDR)?;
        w.write_all(&self.version.to_le_bytes())?;
        w.write_all(&u32::from(self.codec).to_le_bytes())?;
        // pad to 32 bytes total
        w.write_all(&[0u8; Self::SIZE - (8 + 4 + 4)])?;
        Ok(())
    }

    pub fn decode_from<R: Read>(mut r: R) -> io::Result<Self> {
        let mut magic = [0u8; 8];
        r.read_exact(&mut magic)?;
        let version = read_u32_le(&mut r)?;
        let codec = CodecId::from(read_u32_le(&mut r)?);
        let mut reserved = [0u8; 8];
        r.read_exact(&mut reserved)?;
        let mut pad = [0u8; Self::SIZE - (8 + 4 + 4 + 8)];
        r.read_exact(&mut pad)?;

        if magic != MAGIC_BLOCK_HDR {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad block header magic",
            ));
        }
        if version != VERSION_1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported block header version",
            ));
        }
        let hdr = Self { version, codec };
        Ok(hdr)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockFooter {
    pub version: u32,
}

impl Default for BlockFooter {
    fn default() -> Self {
        Self { version: VERSION_1 }
    }
}

impl BlockFooter {
    pub const SIZE: usize = 16;

    pub fn encode_to<W: Write>(&self, mut w: W) -> io::Result<()> {
        w.write_all(&MAGIC_BLOCK_FTR)?;
        w.write_all(&self.version.to_le_bytes())?;
        // pad to 16 bytes
        w.write_all(&[0u8; Self::SIZE - (8 + 4)])?;
        Ok(())
    }

    pub fn decode_from<R: Read>(mut r: R) -> io::Result<Self> {
        let mut magic = [0u8; 8];
        r.read_exact(&mut magic)?;
        let version = read_u32_le(&mut r)?;
        let mut pad = [0u8; Self::SIZE - (8 + 4)];
        r.read_exact(&mut pad)?;
        if magic != MAGIC_BLOCK_FTR {
            let msg = format!("bad block footer magic: {:?}", magic);
            return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
        }
        if version != VERSION_1 {
            let msg = format!("unsupported block footer version: {:?}", version);
            return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
        }
        let ftr = Self { version };
        Ok(ftr)
    }
}

#[inline]
fn read_u32_le<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}
