use crate::codec::Codec;
use crate::io::tail_log_cache::TailLogCache;
use crate::writer_inner::WriterInner;
use crate::{InMemIndex, IndexRef, Offset, StreamError};
use async_stream::try_stream;
use futures_core::Stream;
use futures_util::StreamExt;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tokio::sync::{Mutex, OwnedSemaphorePermit, watch};

pub struct Transaction<C: Codec> {
    codec: C,
    dir: PathBuf,
    bufread: usize,
    index: Arc<RwLock<InMemIndex>>,
    inner: Mutex<WriterInner<C>>,
    watermark_rx: watch::Receiver<Option<Offset>>,
    permit: OwnedSemaphorePermit,
}

pub enum TailFrom {
    Offset(Offset),
    Head,
    Start,
}

impl From<Offset> for TailFrom {
    fn from(offset: Offset) -> Self {
        Self::Offset(offset)
    }
}

impl<C: Codec> Transaction<C> {
    pub(crate) fn new(
        codec: C,
        dir: PathBuf,
        bufread: usize,
        index: Arc<RwLock<InMemIndex>>,
        inner: Mutex<WriterInner<C>>,
        watermark_rx: watch::Receiver<Option<Offset>>,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            codec,
            dir,
            bufread,
            index,
            inner,
            watermark_rx,
            permit,
        }
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

    pub async fn close(self) -> Result<(), StreamError> {
        let Transaction { inner, permit, .. } = self;
        let mut guard = inner.lock().await;
        tokio::task::block_in_place(|| guard.close_sync())?; // no-op if idle
        drop(permit);
        Ok(())
    }

    pub fn subscribe_watermark(&self) -> watch::Receiver<Option<Offset>> {
        self.watermark_rx.clone()
    }

    pub fn tail(
        &self,
        from: impl Into<TailFrom>,
    ) -> impl Stream<Item = Result<(Offset, C::Value), StreamError>> + Unpin + Send + 'static {
        let codec = self.codec.clone();
        let dir = self.dir.clone();
        let mut watermark_rx = self.watermark_rx.clone();
        let bufread = self.bufread;
        let index = self.index.clone();
        let from = match from.into() {
            TailFrom::Offset(from) => from,
            TailFrom::Start => Offset(0),
            TailFrom::Head => match *watermark_rx.borrow() {
                Some(head) => head.saturating_add(1),
                None => Offset(0),
            },
        };

        // Stream committed records from the shared in-memory index, then wait for the watermark to advance.
        try_stream! {
            let mut next_id = from;
            let mut cache = TailLogCache::new(dir.clone());

            loop {
                let upper = *watermark_rx.borrow();

                let mut yielded_any = false;
                if let Some(upper_bound) = upper
                    && next_id <= upper_bound
                {
                    let entries: Vec<IndexRef> = {
                        let guard = index.read().expect("index lock poisoned");
                        let start = guard.lower_bound(next_id).unwrap_or(guard.entries.len());
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
                            .open_block(
                                entry.segment_id,
                                entry.file_offset,
                                entry.block_len,
                                bufread,
                            )
                            .map_err(StreamError::Io)?;

                        let mut id = entry.first_id;
                        while id < next_id {
                            if payload
                                .next_record_bytes()
                                .map_err(StreamError::Io)?
                                .is_none()
                            {
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
                                yield (id, value);
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
                    if *watermark_rx.borrow() <= before {
                        break;
                    }
                }
            }
        }
        .boxed()
    }
}
