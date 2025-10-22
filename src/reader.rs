use crate::codec::Codec;
use crate::index_inmem::InMemIndex;
use crate::io::snapshot::stream_index_snapshot;
use crate::{Offset, StreamError};
use futures_core::Stream;
use futures_util::{StreamExt, stream};
use std::{path::PathBuf, sync::Arc};

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
        let bufread = self.bufread;

        match index.watermark() {
            Some(upper) => stream_index_snapshot(dir, bufread, codec, index, from.0, upper),
            None => stream::empty().boxed(),
        }
    }
}
