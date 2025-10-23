use crate::format::headers::BlockHeader;
use crate::io::payload_reader::PayloadReaderOwned;
use std::fs::File;
use std::io;
use std::path::PathBuf;

pub(crate) struct TailLogCache {
    dir: PathBuf,
    current: Option<(u64, File)>,
}

impl TailLogCache {
    pub(crate) fn new(dir: PathBuf) -> Self {
        Self { dir, current: None }
    }

    fn get_file(&mut self, segment_id: u64) -> io::Result<&File> {
        use crate::io::segment::segment_filename;

        let needs_open = match &self.current {
            Some((id, _)) => *id != segment_id,
            None => true,
        };

        if needs_open {
            let (log_name, _) = segment_filename(segment_id);
            let path = self.dir.join(log_name);
            let file = File::open(path)?;
            self.current = Some((segment_id, file));
        }

        Ok(&self.current.as_ref().expect("just set").1)
    }

    pub(crate) fn open_block(
        &mut self,
        segment_id: u64,
        offset: u64,
        block_len: u32,
        bufread: usize,
    ) -> io::Result<(BlockHeader, PayloadReaderOwned)> {
        use crate::io::reader_block::open_block_stream_with_file;

        let file = self.get_file(segment_id)?;
        open_block_stream_with_file(file, offset, block_len, bufread)
    }
}
