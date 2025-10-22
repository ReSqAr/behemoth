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
use std::sync::Arc;
use tokio::sync::{Mutex, watch};

use crate::codec::Codec;
use crate::config::StreamConfig;
use crate::error::StreamError;
use crate::format::headers::{BlockFooter, BlockHeader};
use crate::format::index::IndexEntry;
use crate::format::{CodecId, VERSION_1};
use crate::index_inmem::{InMemIndex, IndexRef};
use crate::io::segment::{SegmentFiles, open_active_segment, rotate_segment};
use crate::io::snapshot::stream_index_snapshot;
use crate::{Compression, Offset};

/// Public async writer. All operations serialize through an async Mutex, and
/// perform the actual IO inside `block_in_place` to avoid holding an async
/// lock across .await points.
#[derive(Clone)]
pub struct AsyncStreamWriter<C: Codec> {
    codec: C,
    dir: PathBuf,
    bufread: usize,
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
    index: InMemIndex,         // RAM mirror for watermark/lookup

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

struct CountingWriter<W: Write> {
    w: W,
    written: u64,
}

impl<W: Write> CountingWriter<W> {
    fn new(w: W) -> CountingWriter<W> {
        Self { w, written: 0 }
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
            index,
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

        // Snapshot → read up to watermark → await change → repeat.
        try_stream! {
            let mut next_id = from.0;

            loop {
                let index = Arc::new(InMemIndex::load_all(&dir, bufread)?);
                if let Some(upper) = index.watermark() {
                    if next_id <= upper {
                        let mut snapshot = stream_index_snapshot(
                            dir.clone(),
                            bufread,
                            codec.clone(),
                            index.clone(),
                            next_id,
                            upper,
                        );

                        let mut consumed = next_id;
                        while let Some(item) = snapshot.next().await {
                            let (offset, value) = item?;
                            consumed = offset.0.saturating_add(1);
                            yield (offset, value);
                        }
                        next_id = consumed;
                    }
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
        // Compute starting offset as current file length
        let header_offset = self.segment().log.metadata().map(|m| m.len())?; // O_APPEND; safe with serialized access

        // Write minimal BlockHeader (codec only)
        let bh = BlockHeader::new(match self.cfg.compression {
            Compression::None => CodecId::None,
            Compression::Zstd { .. } => CodecId::Zstd,
        });
        let mut buf = Vec::with_capacity(BlockHeader::SIZE);
        bh.encode_to(&mut buf)?;
        self.segment_mut().log.write_all(&buf)?;

        // Build streaming sink
        let w = self.segment().log.try_clone()?;
        let bw = BufWriter::with_capacity(self.cfg.write_buffer, w);
        let cw = CountingWriter::new(bw);
        let sink = PayloadSink::new(cw, self.cfg.compression)?;
        self.state = WriterState::Open(OpenBlock {
            header_offset,
            first_id: self.next_id,
            records: 0,
            uncompressed_len: 0,
            sink,
        });
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

        // Must be open now
        let ob = match &mut self.state {
            WriterState::Open(b) => b,
            _ => unreachable!(),
        };

        // Encode into a bounded temp buffer to know the length prefix.
        let mut tmp = Vec::with_capacity(256);
        self.codec
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
        let len_bytes = len.to_le_bytes();
        ob.sink.write_all(&len_bytes).map_err(StreamError::Io)?;
        ob.sink.write_all(&tmp).map_err(StreamError::Io)?;
        ob.records = ob.records.saturating_add(1);
        ob.uncompressed_len = ob.uncompressed_len.saturating_add(4).saturating_add(len);
        self.next_id = self.next_id.saturating_add(1);

        // Auto-roll if we've reached/exceeded the target:
        // - seal current block (footer + index entry), NO fsync, push to pending
        // - immediately start a fresh block so the next push continues smoothly
        let target = self.cfg.block_target_uncompressed as u32;
        if target > 0 {
            if let WriterState::Open(ref ob2) = self.state {
                if ob2.uncompressed_len >= target {
                    self.roll_block_soft()?; // leaves state = Open(new) because we start_block() at end
                }
            }
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

        // Durability: first .log, then .idx (publishes ALL pending entries)
        self.segment().log.sync_all()?;
        self.segment().idx.sync_all()?;

        // Mirror committed entries to RAM index + bump watermark + notify
        let mut pending = std::mem::take(&mut self.pending);
        for entry in pending.drain(..) {
            let mut ebuf = Vec::with_capacity(IndexEntry::SIZE);
            entry.encode_to(&mut ebuf)?;
            self.segment_mut().idx.write_all(&ebuf)?;

            self.index.entries.push(IndexRef {
                segment_id: self.segment().id,
                first_id: entry.first_id,
                last_id: entry.last_id,
                file_offset: entry.file_offset,
                block_len: entry.block_len,
                uncompressed_len: entry.uncompressed_len,
                records: entry.records,
            });
            self.watermark = Some(entry.last_id);
            if let Some(tx) = &self.watermark_tx {
                let _ = tx.send(self.watermark);
            }
        }
        self.pending = pending;

        // Rotate after commit if needed (between blocks)
        let file_len = self.segment().log.metadata()?.len();
        if file_len >= self.cfg.segment_max_bytes {
            self.seg = Some(rotate_segment(self.segment())?);
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
        let ob = match std::mem::replace(&mut self.state, WriterState::Idle) {
            WriterState::Open(ob) => ob,
            _ => return Ok(()),
        };

        // If no records were written, emit nothing (no footer, no index entry)
        if ob.records == 0 {
            return Ok(());
        }

        // Finish compressor, compute sizes
        let cw = ob.sink.finish()?;
        let compressed_len = cw.written as u32;

        // Footer
        let ftr = BlockFooter::default();
        let mut buf = Vec::with_capacity(BlockFooter::SIZE);
        ftr.encode_to(&mut buf)?;
        self.segment_mut().log.write_all(&buf)?;

        // Final sizes
        let block_len = (BlockHeader::SIZE as u32)
            .saturating_add(compressed_len)
            .saturating_add(BlockFooter::SIZE as u32);

        // IDs
        let first_id = ob.first_id;
        let last_id = first_id + (ob.records as u64).saturating_sub(1);

        // Append index entry to current .idx (NO fsync here)
        let entry = IndexEntry {
            version: VERSION_1,
            first_id,
            last_id,
            file_offset: ob.header_offset,
            block_len,
            records: ob.records,
            uncompressed_len: ob.uncompressed_len,
            compressed_len,
            flags: 0,
        };

        // Keep it pending; publish on flush()
        self.pending.push(entry);

        // Immediately start the next block so subsequent push() continues smoothly
        self.start_block()?;

        Ok(())
    }
}
