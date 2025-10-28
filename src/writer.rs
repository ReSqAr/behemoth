use std::sync::{Arc, RwLock};
use tokio::sync::{Mutex, Semaphore, watch};

use crate::Offset;
use crate::codec::Codec;
use crate::config::StreamConfig;
use crate::error::StreamError;
use crate::index_inmem::InMemIndex;
use crate::io::segment::open_active_segment;
use crate::reader::AsyncStreamReader;
use crate::transaction::Transaction;
use crate::writer_inner::WriterInner;

/// Public async writer. All operations serialize through an async Mutex, and
/// perform the actual IO inside `block_in_place` to avoid holding an async
/// lock across .await points.
#[derive(Clone)]
pub struct AsyncStreamWriter<C: Codec> {
    codec: C,
    cfg: StreamConfig,
    index: Arc<RwLock<InMemIndex>>,
    transaction_semaphore: Arc<Semaphore>,
}

impl<C: Codec> AsyncStreamWriter<C> {
    pub async fn open(cfg: StreamConfig, codec: C) -> Result<Self, StreamError> {
        // Recovery + open active segment
        let _ = open_active_segment(&cfg.dir)?; // truncates to safe_end & writes headers if new

        // Load all index entries into RAM
        let index = InMemIndex::load_all(&cfg.dir, cfg.read_buffer)?;
        let index = Arc::new(RwLock::new(index));

        Ok(Self {
            codec,
            cfg,
            index,
            transaction_semaphore: Arc::new(Semaphore::new(1)),
        })
    }

    /// Durable watermark at this moment.
    pub fn watermark(&self) -> Option<Offset> {
        let guard = self.index.read().expect("index lock poisoned");
        guard.watermark()
    }

    pub fn reader(&self) -> AsyncStreamReader<C> {
        AsyncStreamReader::with_shared_index(
            self.cfg.dir.clone(),
            self.cfg.read_buffer,
            self.codec.clone(),
            self.index.clone(),
        )
    }

    pub fn transaction(&self) -> Result<Transaction<C>, StreamError> {
        let permit = self
            .transaction_semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| StreamError::TransactionInProgress)?;

        let watermark = {
            let guard = self.index.read().expect("index lock poisoned");
            guard.watermark()
        };
        let next_id = match watermark {
            Some(wm) => wm.0.saturating_add(1),
            None => 0,
        };

        let seg = open_active_segment(&self.cfg.dir)?;
        let codec = self.codec.clone();
        let (tx, rx) = watch::channel(watermark);
        let inner = WriterInner::new(
            self.cfg.clone(),
            codec.clone(),
            Some(seg),
            self.index.clone(),
            next_id,
            watermark,
            Some(tx),
        );

        Ok(Transaction::new(
            codec,
            self.cfg.dir.clone(),
            self.cfg.read_buffer,
            self.index.clone(),
            Mutex::new(inner),
            rx,
            permit,
        ))
    }
}
