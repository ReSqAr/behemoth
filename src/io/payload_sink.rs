use crate::Compression;
use std::io;
use std::io::Write;

pub(crate) enum PayloadSink<W: Write> {
    Plain(W),
    Zstd(zstd::stream::write::Encoder<'static, W>),
}

impl<W: Write> PayloadSink<W> {
    pub(crate) fn new(w: W, compression: Compression) -> io::Result<Self> {
        Ok(match compression {
            Compression::None => PayloadSink::Plain(w),
            Compression::Zstd { level } => {
                PayloadSink::Zstd(zstd::stream::write::Encoder::new(w, level)?)
            }
        })
    }

    pub(crate) fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        match self {
            PayloadSink::Plain(w) => w.write_all(buf),
            PayloadSink::Zstd(e) => e.write_all(buf),
        }
    }

    pub(crate) fn finish(self) -> io::Result<W> {
        match self {
            PayloadSink::Plain(mut w) => {
                w.flush()?;
                Ok(w)
            }
            PayloadSink::Zstd(e) => {
                let mut inner = e.finish()?;
                inner.flush()?;
                Ok(inner)
            }
        }
    }
}
