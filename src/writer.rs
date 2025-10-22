//! writer.rs — append-only, streaming writer with no background task.
//! - No positioned writes; strictly append-only to .log
//! - All sizes/ids live in the index; headers/footers are minimal per spec
//! - Bounded memory: per-record temp buffer only; payload streams to disk
//! - Concurrency: serialized via an async Mutex + block_in_place for sync IO

use async_stream::try_stream;
use futures_core::Stream;
use futures_util::StreamExt;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tokio::sync::{Mutex, watch};

use crate::codec::Codec;
use crate::config::StreamConfig;
use crate::error::StreamError;
use crate::format::headers::{BlockFooter, BlockHeader};
use crate::format::index::IndexEntry;
use crate::format::{CodecId, VERSION_1};
use crate::index_inmem::{InMemIndex, IndexRef};
use crate::io::counting_writer::CountingWriter;
use crate::io::segment::{SegmentFiles, open_active_segment, rotate_segment};
use crate::io::tail_log_cache::TailLogCache;
use crate::{Compression, Offset};

/// Public async writer. All operations serialize through an async Mutex, and
/// perform the actual IO inside `block_in_place` to avoid holding an async
/// lock across .await points.
#[derive(Clone)]
pub struct AsyncStreamWriter<C: Codec> {
    codec: C,
    dir: PathBuf,
    bufread: usize,
    index: Arc<RwLock<InMemIndex>>,
    inner: Arc<Mutex<WriterInner<C>>>,
    watermark_rx: watch::Receiver<Option<u64>>,
}

enum WriterState {
    Idle,
    Open(OpenBlock),
    Closed,
}

struct WriterInner<C: Codec> {
    cfg: StreamConfig,
    codec: C,

    seg: Option<SegmentFiles>, // active segment (.log opened O_APPEND, .idx read+write)
    index: Arc<RwLock<InMemIndex>>, // RAM mirror for watermark/lookup

    next_id: u64,
    watermark: Option<u64>,
    watermark_tx: Option<watch::Sender<Option<u64>>>,
    pending: Vec<IndexEntry>, // soft-rolled, not-yet-committed entries

    state: WriterState,
}

struct OpenBlock {
    header_offset: u64, // position of BlockHeader
    first_id: u64,
    records: u32,
    uncompressed_len: u32,
    sink: PayloadSink<CountingWriter<BufWriter<File>>>,
}

impl OpenBlock {
    fn begin(
        segment: &mut SegmentFiles,
        compression: Compression,
        write_buffer: usize,
        next_id: u64,
    ) -> io::Result<Self> {
        let codec = match compression {
            Compression::None => CodecId::None,
            Compression::Zstd { .. } => CodecId::Zstd,
        };

        let header_offset = segment.append_block_header(codec)?;
        let writer = segment.log.try_clone()?;
        let buf = BufWriter::with_capacity(write_buffer, writer);
        let sink = PayloadSink::new(CountingWriter::new(buf), compression)?;
        Ok(Self {
            header_offset,
            first_id: next_id,
            records: 0,
            uncompressed_len: 0,
            sink,
        })
    }

    fn write_record<C: Codec>(&mut self, codec: &C, value: &C::Value) -> Result<(), StreamError> {
        let mut tmp = Vec::with_capacity(256);
        codec
            .encode_into(value, &mut tmp)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
            .map_err(StreamError::Io)?;
        if tmp.len() > u32::MAX as usize {
            return Err(StreamError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "record too large",
            )));
        }

        let len = tmp.len() as u32;
        self.sink
            .write_all(&len.to_le_bytes())
            .map_err(StreamError::Io)?;
        self.sink.write_all(&tmp).map_err(StreamError::Io)?;
        self.records = self.records.saturating_add(1);
        self.uncompressed_len = self.uncompressed_len.saturating_add(4).saturating_add(len);
        Ok(())
    }

    fn finish(self, segment: &mut SegmentFiles) -> io::Result<Option<IndexEntry>> {
        if self.records == 0 {
            return Ok(None);
        }

        let writer = self.sink.finish()?;
        let compressed_len = writer.written() as u32;

        segment.append_block_footer()?;

        let block_len = (BlockHeader::SIZE as u32)
            .saturating_add(compressed_len)
            .saturating_add(BlockFooter::SIZE as u32);
        let last_id = self
            .first_id
            .saturating_add((self.records as u64).saturating_sub(1));

        let entry = IndexEntry {
            version: VERSION_1,
            first_id: self.first_id,
            last_id,
            file_offset: self.header_offset,
            block_len,
            records: self.records,
            uncompressed_len: self.uncompressed_len,
            compressed_len,
            flags: 0,
        };

        Ok(Some(entry))
    }
}

enum PayloadSink<W: Write> {
    Plain(W),
    Zstd(zstd::stream::write::Encoder<'static, W>),
}

impl<W: Write> PayloadSink<W> {
    fn new(w: W, compression: Compression) -> io::Result<Self> {
        Ok(match compression {
            Compression::None => PayloadSink::Plain(w),
            Compression::Zstd { level } => {
                PayloadSink::Zstd(zstd::stream::write::Encoder::new(w, level)?)
            }
        })
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        match self {
            PayloadSink::Plain(w) => w.write_all(buf),
            PayloadSink::Zstd(e) => e.write_all(buf),
        }
    }

    fn finish(self) -> io::Result<W> {
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

impl<C: Codec> AsyncStreamWriter<C> {
    pub async fn open(cfg: StreamConfig, codec: C) -> Result<Self, StreamError> {
        // Recovery + open active segment
        let seg = open_active_segment(&cfg.dir)?; // truncates to safe_end & writes headers if new

        // Load all index entries into RAM
        let index = InMemIndex::load_all(&cfg.dir, cfg.read_buffer)?;
        let watermark = index.watermark();
        let index = Arc::new(RwLock::new(index));
        let next_id = match watermark {
            Some(wm) => wm.saturating_add(1),
            None => 0,
        };

        let dir = cfg.dir.clone();
        let bufread = cfg.read_buffer;

        let (tx, rx) = watch::channel(watermark);
        let inner = WriterInner {
            cfg,
            codec: codec.clone(),
            seg: Some(seg),
            index: index.clone(),
            next_id,
            watermark,
            watermark_tx: Some(tx),
            pending: Vec::new(),
            state: WriterState::Idle,
        };
        Ok(Self {
            codec,
            dir,
            bufread,
            index,
            inner: Arc::new(Mutex::new(inner)),
            watermark_rx: rx,
        })
    }

    /// Append one value (length-prefixed, encoded via `Codec`) to the current block,
    /// starting a new block lazily if needed. Streams directly to disk.
    pub async fn push(&self, value: &C::Value) -> Result<(), StreamError> {
        let mut guard = self.inner.lock().await;
        tokio::task::block_in_place(|| guard.push_sync(value))
    }

    /// Seal the current block (if any), write footer, fsync(log), append index entry,
    /// fsync(idx), update watermark. Returns new watermark.
    pub async fn flush(&self) -> Result<Option<Offset>, StreamError> {
        let mut guard = self.inner.lock().await;
        let off = tokio::task::block_in_place(|| guard.flush_sync())?;
        Ok(off)
    }

    pub async fn close(&self) -> Result<(), StreamError> {
        let mut guard = self.inner.lock().await;
        tokio::task::block_in_place(|| guard.close_sync())?; // no-op if idle
        // Drop sender so tailers complete
        drop(guard.watermark_tx.clone());
        Ok(())
    }

    /// Durable watermark at this moment.
    pub fn watermark(&self) -> Option<u64> {
        *self.watermark_rx.borrow()
    }

    /// Tailing stream (not implemented here; assumed elsewhere in crate)
    pub fn subscribe_watermark(&self) -> watch::Receiver<Option<u64>> {
        self.watermark_rx.clone()
    }

    pub fn tail(
        &self,
        from: Offset,
    ) -> impl Stream<Item = Result<(Offset, C::Value), StreamError>> + Unpin + Send + 'static {
        let codec = self.codec.clone();
        let dir = self.dir.clone();
        let mut watermark_rx = self.watermark_rx.clone();
        let bufread = self.bufread;
        let index = self.index.clone();

        // Stream committed records from the shared in-memory index, then wait for the watermark to advance.
        try_stream! {
            let mut next_id = from.0;
            let mut cache = TailLogCache::new(dir.clone());

            loop {
                let upper = *watermark_rx.borrow();

                let mut yielded_any = false;
                if let Some(upper_bound) = upper && next_id <= upper_bound {
                    let entries: Vec<IndexRef> =  {
                        let guard = index.read().expect("index lock poisoned");
                        let start = guard
                            .lower_bound(next_id)
                            .unwrap_or(guard.entries.len());
                        guard.entries[start..]
                            .iter()
                            .take_while(|entry| entry.first_id <= upper_bound)
                            .copied()
                            .collect()
                    };

                    let mut consumed = next_id;
                    for entry in entries {
                        if entry.first_id > upper_bound {
                            break;
                        }

                        let (_, mut payload) = cache
                            .open_block(entry.segment_id, entry.file_offset, entry.block_len, bufread)
                            .map_err(StreamError::Io)?;

                        let mut id = entry.first_id;
                        while id < next_id {
                            if payload.next_record_bytes().map_err(StreamError::Io)?.is_none() {
                                break;
                            }
                            id = id.saturating_add(1);
                        }

                        let limit = upper_bound.min(entry.last_id);
                        while id <= limit {
                            if let Some(mut bytes) =
                                payload.next_record_bytes().map_err(StreamError::Io)?
                            {
                                let mut cur = std::io::Cursor::new(&mut bytes[..]);
                                let value = codec
                                    .decode_from(&mut cur)
                                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                                    .map_err(StreamError::Io)?;
                                yield (Offset(id), value);
                                id = id.saturating_add(1);
                                consumed = id;
                                yielded_any = true;
                            } else {
                                Err::<_, StreamError>(
                                    io::Error::new(
                                        io::ErrorKind::Other,
                                        "unexpected end of block payload",
                                    )
                                    .into(),
                                )?;
                            }
                        }

                        payload.finish().map_err(StreamError::Io)?;
                        next_id = consumed;
                        if next_id > upper_bound {
                            break;
                        }
                    }
                }

                if yielded_any {
                    continue;
                }

                // Wait for watermark to increase; if sender dropped & no change → end.
                let before = *watermark_rx.borrow();
                if watermark_rx.changed().await.is_err() {
                    if *watermark_rx.borrow() <= before { break; }
                }
            }
        }
        .boxed()
    }
}

impl<C: Codec> WriterInner<C> {
    fn segment(&self) -> &SegmentFiles {
        self.seg.as_ref().expect("active segment missing")
    }

    fn segment_mut(&mut self) -> &mut SegmentFiles {
        self.seg.as_mut().expect("active segment missing")
    }

    fn start_block(&mut self) -> io::Result<()> {
        let compression = self.cfg.compression;
        let write_buffer = self.cfg.write_buffer;
        let next_id = self.next_id;
        let block = {
            let segment = self.segment_mut();
            OpenBlock::begin(segment, compression, write_buffer, next_id)?
        };
        self.state = WriterState::Open(block);
        Ok(())
    }

    fn push_sync(&mut self, value: &C::Value) -> Result<(), StreamError> {
        // Reject after close
        if matches!(self.state, WriterState::Closed) {
            return Err(StreamError::Closed);
        }

        // Lazily start a block: write BlockHeader only when the FIRST record arrives
        if matches!(self.state, WriterState::Idle) {
            self.start_block()?;
        }

        {
            let block = match &mut self.state {
                WriterState::Open(block) => block,
                _ => unreachable!(),
            };
            block.write_record(&self.codec, value)?;
        }

        self.next_id = self.next_id.saturating_add(1);

        // Auto-roll if we've reached/exceeded the target:
        // - seal current block (footer + index entry), NO fsync, push to pending
        // - immediately start a fresh block so the next push continues smoothly
        let target = self.cfg.block_target_uncompressed as u32;
        let should_roll = target > 0
            && matches!(&self.state, WriterState::Open(block) if block.uncompressed_len >= target);
        if should_roll {
            self.roll_block_soft()?; // leaves state = Open(new) because we start_block() at end
        }

        Ok(())
    }

    fn flush_sync(&mut self) -> Result<Option<Offset>, StreamError> {
        if matches!(self.state, WriterState::Closed) {
            return Err(StreamError::Closed);
        }

        // If there's an open block:
        // - if it has records, soft-seal it (footer + index entry, NO fsync)
        // - if it has zero records (e.g., started and then nothing wrote), drop it silently
        let had_open = matches!(self.state, WriterState::Open(_));
        if had_open {
            let drop_empty = matches!(&self.state, WriterState::Open(ob) if ob.records == 0);
            if drop_empty {
                self.state = WriterState::Idle; // no footer, no index entry
            } else {
                self.roll_block_soft()?; // writes footer + idx entry, pushes to pending, starts new block
                // We *did* start a new block above; we don't want an empty block sitting open
                // on flush, so drop it back to Idle if it has zero records.
                if matches!(&self.state, WriterState::Open(ob) if ob.records == 0) {
                    self.state = WriterState::Idle;
                }
            }
        }

        if self.pending.is_empty() {
            return Ok(self.watermark.map(Offset));
        }

        self.commit_pending_entries()?;

        if self.segment().should_rotate(self.cfg.segment_max_bytes)? {
            let current = self.segment();
            self.seg = Some(rotate_segment(current)?);
        }

        Ok(self.watermark.map(Offset))
    }

    fn close_sync(&mut self) -> Result<(), StreamError> {
        if matches!(self.state, WriterState::Closed) {
            return Ok(()); // idempotent
        }

        // Commit any pending work (will also drop empty open block)
        let _ = self.flush_sync()?;

        // Transition to Closed and drop the watermark sender to end tailers
        self.state = WriterState::Closed;
        if let Some(tx) = self.watermark_tx.take() {
            drop(tx);
        }

        Ok(())
    }

    fn roll_block_soft(&mut self) -> io::Result<()> {
        // Take current block; if none, nothing to do
        let block = match std::mem::replace(&mut self.state, WriterState::Idle) {
            WriterState::Open(block) => block,
            _ => return Ok(()),
        };

        if let Some(entry) = block.finish(self.segment_mut())? {
            self.pending.push(entry);
            self.start_block()?;
        }

        Ok(())
    }

    fn commit_pending_entries(&mut self) -> io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }

        {
            let segment = self.segment_mut();
            segment.sync_log()?;
            segment.sync_index()?;
        }

        for entry in std::mem::take(&mut self.pending) {
            self.publish_entry(entry)?;
        }

        Ok(())
    }

    fn publish_entry(&mut self, entry: IndexEntry) -> io::Result<()> {
        let segment_id = self.segment().id;
        {
            let segment = self.segment_mut();
            segment.append_index_entry(&entry)?;
        }
        {
            let mut index = self.index.write().expect("index lock poisoned");
            index.push_entry(segment_id, &entry);
        }
        self.watermark = Some(entry.last_id);
        if let Some(tx) = &self.watermark_tx {
            let _ = tx.send(self.watermark);
        }
        Ok(())
    }
}
