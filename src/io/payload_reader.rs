use crate::format::CodecId;
use crate::format::headers::BlockFooter;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};

pub(crate) enum PayloadReader {
    Plain(BufReader<Box<dyn Read + Send>>),
    Zstd(Box<dyn Read + Send>),
}

impl PayloadReader {
    pub(crate) fn new(
        codec: CodecId,
        reader: Box<dyn Read + Send>,
        bufread: usize,
    ) -> io::Result<Self> {
        match codec {
            CodecId::None => {
                let br = BufReader::with_capacity(bufread, reader);
                Ok(PayloadReader::Plain(br))
            }
            CodecId::Zstd => {
                let br = BufReader::with_capacity(bufread, reader);
                let dec = zstd::stream::read::Decoder::with_buffer(br).map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("zstd: {e}"))
                })?;
                let erased: Box<dyn Read + Send> = Box::new(dec);
                Ok(PayloadReader::Zstd(erased))
            }
        }
    }

    #[inline]
    pub(crate) fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            PayloadReader::Plain(r) => r.read(buf),
            PayloadReader::Zstd(r) => r.read(buf),
        }
    }
}

pub(crate) struct PayloadReaderOwned {
    inner: PayloadReader,
    footer_file: File,
    footer_offset: u64,
    footer_checked: bool,
}

impl PayloadReaderOwned {
    pub(crate) fn new(
        codec: CodecId,
        reader: Box<dyn Read + Send>,
        bufread: usize,
        footer_file: File,
        footer_offset: u64,
    ) -> io::Result<Self> {
        let inner = PayloadReader::new(codec, reader, bufread)?;
        Ok(Self {
            inner,
            footer_file,
            footer_offset,
            footer_checked: false,
        })
    }

    /// Get next record as raw bytes (bounded to exactly the record length).
    pub(crate) fn next_record_bytes(&mut self) -> io::Result<Option<Vec<u8>>> {
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
    pub(crate) fn finish(&mut self) -> io::Result<()> {
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
        self.inner.read(buf)
    }
}
