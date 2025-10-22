use crate::format::CodecId;
use crate::format::headers::{BlockFooter, BlockHeader};
use std::{
    fs::File,
    io::{self, BufReader, Read, Seek, SeekFrom},
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

    let inner = match hdr.codec {
        CodecId::None => {
            // Box the *Take<File>* and then buffer it → implements BufRead.
            let boxed: Box<dyn Read + Send> = Box::new(take);
            InnerOwned::Plain(BufReader::with_capacity(bufread, boxed))
        }
        CodecId::Zstd => {
            // Decoder needs BufRead → wrap the boxed reader in BufReader first.
            let boxed: Box<dyn Read + Send> = Box::new(take);
            let br = BufReader::with_capacity(bufread, boxed);
            let dec = zstd::stream::read::Decoder::with_buffer(br).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, format!("zstd: {e}"))
            })?;
            // Erase Decoder to Box<dyn Read + Send>
            let erased: Box<dyn Read + Send> = Box::new(dec);
            InnerOwned::Zstd(erased)
        }
    };

    // separate handle for footer validation
    let ff = file.try_clone()?;

    Ok((
        hdr,
        PayloadReaderOwned {
            inner,
            footer_file: ff,
            footer_offset: footer_off,
            footer_checked: false,
        },
    ))
}

/// Owned payload reader (Send), with separate handle to read footer.
pub struct PayloadReaderOwned {
    inner: InnerOwned,
    footer_file: File,
    footer_offset: u64,
    footer_checked: bool,
}

enum InnerOwned {
    // Plain path keeps a buffered reader over the Take<File>
    Plain(std::io::BufReader<Box<dyn std::io::Read + Send>>),
    // Zstd path: we erase the decoder to a trait object that implements Read + Send
    Zstd(Box<dyn std::io::Read + Send>),
}

impl PayloadReaderOwned {
    /// Get next record as raw bytes (bounded to exactly the record length).
    pub fn next_record_bytes(&mut self) -> io::Result<Option<Vec<u8>>> {
        // Read u32 length
        let mut len_buf = [0u8; 4];
        match self.read_exact_all(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                self.finish()?;
                return Ok(None);
            }
            Err(e) => return Err(e),
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut data = vec![0u8; len];
        self.read_exact_all(&mut data)?;
        Ok(Some(data))
    }

    /// Finish payload and structurally validate the footer.
    pub fn finish(&mut self) -> io::Result<()> {
        if self.footer_checked {
            return Ok(());
        }

        // Drain any remaining decompressed bytes (normally none).
        let mut drain = [0u8; 8192];
        while self.read_some(&mut drain)? > 0 {}

        // Read & validate footer from the separate handle at known offset.
        let mut ff = &self.footer_file;
        ff.seek(SeekFrom::Start(self.footer_offset))?;
        let mut ftr_buf = vec![0u8; BlockFooter::SIZE];
        ff.read_exact(&mut ftr_buf)?;
        let _ = BlockFooter::decode_from(&ftr_buf[..])?;

        self.footer_checked = true;
        Ok(())
    }

    #[inline]
    fn read_exact_all(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let mut off = 0;
        while off < buf.len() {
            let n = self.read_some(&mut buf[off..])?;
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
            }
            off += n;
        }
        Ok(())
    }

    #[inline]
    fn read_some(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &mut self.inner {
            InnerOwned::Plain(r) => r.read(buf),
            InnerOwned::Zstd(r) => r.read(buf),
        }
    }
}
