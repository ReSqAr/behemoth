use crate::format::headers::{BlockFooter, BlockHeader};
use crate::io::payload_reader::PayloadReaderOwned;
use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    path::Path,
};

/// Open a block (by path/offset/len) and return header + an owned streaming payload reader.
/// The footer is validated on `finish()` using a separate file handle seek.
pub fn open_block_stream_at_path(
    path: &Path,
    offset: u64,
    block_len: u32,
    bufread: usize,
) -> io::Result<(BlockHeader, PayloadReaderOwned)> {
    let file = File::open(path)?;
    open_block_stream_with_file(&file, offset, block_len, bufread)
}

/// Open a block using an already-opened file handle.
pub fn open_block_stream_with_file(
    file: &File,
    offset: u64,
    block_len: u32,
    bufread: usize,
) -> io::Result<(BlockHeader, PayloadReaderOwned)> {
    // --- Read and validate header ---
    let mut fh = file.try_clone()?;
    fh.seek(SeekFrom::Start(offset))?;
    let mut hdr_buf = vec![0u8; BlockHeader::SIZE];
    fh.read_exact(&mut hdr_buf)?;
    let hdr = BlockHeader::decode_from(&hdr_buf[..])?;

    let payload_len = (block_len as usize)
        .saturating_sub(BlockHeader::SIZE)
        .saturating_sub(BlockFooter::SIZE);
    let payload_off = offset + BlockHeader::SIZE as u64;
    let footer_off = payload_off + payload_len as u64;

    // Build the owned payload reader (no &mut borrows!)
    let mut pf = file.try_clone()?;
    pf.seek(SeekFrom::Start(payload_off))?;
    let take = pf.take(payload_len as u64);
    let reader: Box<dyn Read + Send> = Box::new(take);

    // separate handle for footer validation
    let ff = file.try_clone()?;

    let payload_reader = PayloadReaderOwned::new(hdr.codec, reader, bufread, ff, footer_off)?;

    Ok((hdr, payload_reader))
}
