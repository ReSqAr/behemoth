use crate::codec::Codec;
use crate::format::index::IndexEntry;
use crate::io::open_block::OpenBlock;
use crate::io::segment::{SegmentFiles, rotate_segment};
use crate::{InMemIndex, Offset, StreamConfig, StreamError};
use std::io;
use std::sync::{Arc, RwLock};
use tokio::sync::watch;

pub enum WriterState {
    Idle,
    Open(OpenBlock),
    Closed,
}

pub(crate) struct WriterInner<C: Codec> {
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

impl<C: Codec> WriterInner<C> {
    pub(crate) fn new(
        cfg: StreamConfig,
        codec: C,
        seg: Option<SegmentFiles>,
        index: Arc<RwLock<InMemIndex>>,
        next_id: u64,
        watermark: Option<u64>,
        watermark_tx: Option<watch::Sender<Option<u64>>>,
    ) -> Self {
        Self {
            cfg,
            codec,
            seg,
            index,
            next_id,
            watermark,
            watermark_tx,
            pending: Vec::new(),
            state: WriterState::Idle,
        }
    }

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

    pub(crate) fn push_sync(&mut self, value: &C::Value) -> Result<(), StreamError> {
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
            && matches!(&self.state, WriterState::Open(block) if block.uncompressed_len() >= target);
        if should_roll {
            self.roll_block_soft()?; // leaves state = Open(new) because we start_block() at end
        }

        Ok(())
    }

    pub(crate) fn flush_sync(&mut self) -> Result<Option<Offset>, StreamError> {
        if matches!(self.state, WriterState::Closed) {
            return Err(StreamError::Closed);
        }

        // If there's an open block:
        // - if it has records, soft-seal it (footer + index entry, NO fsync)
        // - if it has zero records (e.g., started and then nothing wrote), drop it silently
        let had_open = matches!(self.state, WriterState::Open(_));
        if had_open {
            let drop_empty = matches!(&self.state, WriterState::Open(ob) if ob.records() == 0);
            if drop_empty {
                self.state = WriterState::Idle; // no footer, no index entry
            } else {
                self.roll_block_soft()?; // writes footer + idx entry, pushes to pending, starts new block
                // We *did* start a new block above; we don't want an empty block sitting open
                // on flush, so drop it back to Idle if it has zero records.
                if matches!(&self.state, WriterState::Open(ob) if ob.records() == 0) {
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

    pub(crate) fn close_sync(&mut self) -> Result<(), StreamError> {
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
