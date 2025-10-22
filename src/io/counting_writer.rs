use std::io;
use std::io::Write;

pub(crate) struct CountingWriter<W: Write> {
    w: W,
    written: u64,
}

impl<W: Write> CountingWriter<W> {
    pub(crate) fn new(w: W) -> CountingWriter<W> {
        Self { w, written: 0 }
    }
    pub(crate) fn written(&self) -> u64 {
        self.written
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.w.write(buf)?;
        self.written += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.w.flush()
    }
}
