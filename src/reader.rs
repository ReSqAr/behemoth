use crate::codec::Codec;
use crate::index_inmem::InMemIndex;
use crate::io::reader_block::open_block_stream_at_path;
use crate::{Offset, StreamError};
use async_stream::try_stream;
use futures_core::Stream;
use futures_util::StreamExt;
use std::{
    io::{self},
    path::PathBuf,
    sync::Arc,
};

#[derive(Clone)]
pub struct AsyncStreamReader<C: Codec> {
    dir: PathBuf,
    bufread: usize,
    codec: C,
    index: Arc<InMemIndex>,
}

impl<C: Codec + Clone + Send + Sync + 'static> AsyncStreamReader<C> {
    pub async fn open(cfg: crate::config::StreamConfig, codec: C) -> Result<Self, StreamError> {
        // Pure read-only open (no recovery step here).
        let index = InMemIndex::load_all(&cfg.dir, cfg.read_buffer)?;
        Ok(Self {
            dir: cfg.dir,
            bufread: cfg.read_buffer,
            codec,
            index: Arc::new(index),
        })
    }

    /// Snapshot watermark (last committed id) at time of call.
    pub fn snapshot_watermark(&self) -> Option<Offset> {
        self.index.watermark().map(Offset)
    }

    /// Bounded pass: yields records with id >= `from` and id <= snapshot watermark, then ends.
    pub fn from(
        &self,
        from: Offset,
    ) -> impl Stream<Item = Result<(Offset, C::Value), StreamError>> + Unpin + Send + 'static {
        let dir = self.dir.clone();
        let codec = self.codec.clone();
        let index = self.index.clone();
        let upper = index.watermark();
        let bufread = self.bufread;

        try_stream! {
            let upper = match upper {
                Some(u) => u,
                None => return, // empty
            };

            // Find starting block
            let start = index.lower_bound(from.0).unwrap_or(index.entries.len());
            let mut cur_id = from.0;

            let mut cur_seg_id: Option<u64> = None;

            for i in start..index.entries.len() {
                let e = index.entries[i];
                if e.first_id > upper { break; }

                // lazily open the segment log if not open or changed
                if cur_seg_id != Some(e.segment_id) {
                    cur_seg_id = Some(e.segment_id);
                }

                // Open a streaming view of this block (no big allocation, no decode_all)
                let (log_name, _) = crate::io::segment::segment_filename(e.segment_id);
                let path = dir.join(log_name);
                let (_hdr, mut payload) = open_block_stream_at_path(&path, e.file_offset, e.block_len, bufread)?;

                // Skip records < cur_id within this block
                let mut id = e.first_id;
                while id < cur_id {
                    if payload.next_record_bytes()?.is_none() { break; }
                    id += 1;
                }

                // Yield records up to min(e.last_id, upper).
                let limit = upper.min(e.last_id);
                while id <= limit {
                    if let Some(mut bytes) = payload.next_record_bytes()? {
                        // Pass a cursor over the record's bytes to the codec (trait expects &mut dyn Read)
                        let mut cur = std::io::Cursor::new(&mut bytes[..]);
                        let v = codec
                            .decode_from(&mut cur)
                            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                        yield (Offset(id), v);
                        id += 1;
                    } else {
                        Err::<_, StreamError>(io::Error::new(io::ErrorKind::Other, "unexpected end of block payload").into())?;
                    }
                }
                // Ensure footer is consumed/validated before moving to the next block
                payload.finish()?;

                cur_id = id;
                if cur_id > upper { break; }
            }
        }.boxed()
    }
}
