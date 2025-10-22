use std::io::{self};
use std::path::PathBuf;
use std::sync::Arc;

use async_stream::try_stream;
use futures_util::StreamExt;
use futures_util::stream::BoxStream;

use crate::codec::Codec;
use crate::index_inmem::InMemIndex;
use crate::io::reader_block::open_block_stream_at_path;
use crate::io::segment::segment_filename;
use crate::{Offset, StreamError};

/// Build a stream over records in a snapshot of the in-memory index.
///
/// The produced stream yields all records with IDs in [`from`, `upper`], decoding
/// them using the provided codec while streaming directly off disk.
pub(crate) fn stream_index_snapshot<C>(
    dir: PathBuf,
    bufread: usize,
    codec: C,
    index: Arc<InMemIndex>,
    from: u64,
    upper: u64,
) -> BoxStream<'static, Result<(Offset, C::Value), StreamError>>
where
    C: Codec,
{
    try_stream! {
        let mut next_id = from;
        let start = index.lower_bound(from).unwrap_or(index.entries.len());

        for entry in index.entries[start..].iter().copied() {
            if entry.first_id > upper {
                break;
            }

            let (log_name, _) = segment_filename(entry.segment_id);
            let path = dir.join(log_name);
            let (_hdr, mut payload) =
                open_block_stream_at_path(&path, entry.file_offset, entry.block_len, bufread)?;

            // Skip already-consumed records in the first block (if needed).
            let mut id = entry.first_id;
            while id < next_id {
                if payload.next_record_bytes()?.is_none() {
                    break;
                }
                id += 1;
            }

            let limit = upper.min(entry.last_id);
            while id <= limit {
                if let Some(mut bytes) = payload.next_record_bytes()? {
                    let mut cur = std::io::Cursor::new(&mut bytes[..]);
                    let value = codec
                        .decode_from(&mut cur)
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    yield (Offset(id), value);
                    id += 1;
                } else {
                    Err::<_, StreamError>(
                        io::Error::new(io::ErrorKind::Other, "unexpected end of block payload")
                            .into(),
                    )?;
                }
            }

            payload.finish()?;
            next_id = id;

            if next_id > upper {
                break;
            }
        }
    }
    .boxed()
}
