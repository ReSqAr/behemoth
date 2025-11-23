#[cfg(test)]
mod tests {
    use crate::transaction::Transaction;
    use crate::{
        AsyncStreamReader, AsyncStreamWriter, Compression, InMemIndex, Offset, StreamConfig, codec,
    };
    use futures_util::TryStreamExt;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
    struct Ev {
        n: u64,
        pad: Vec<u8>,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
    struct IdEvent {
        id: u64,
    }

    fn assert_id_sequence(label: &str, events: &[(u64, IdEvent)], expected: u64) {
        assert_eq!(
            events.len(),
            expected as usize,
            "{label}: expected {expected} events, got {}",
            events.len()
        );
        for (idx, (off, ev)) in events.iter().enumerate() {
            let idx_u64 = idx as u64;
            assert_eq!(
                *off, idx_u64,
                "{label}: offset mismatch at {idx} (expected {idx_u64}, got {off})"
            );
            assert_eq!(
                ev.id, idx_u64,
                "{label}: event id mismatch at {idx} (expected {idx_u64}, got {})",
                ev.id
            );
        }
    }

    // Helper to push ~N bytes (uncompressed) via multiple events.
    async fn push_bytes<C: crate::codec::Codec<Value = Ev>>(
        txn: &Transaction<C>,
        target_bytes: usize,
    ) {
        let payload = vec![0u8; 8 * 1024]; // 8 KiB/event
        let mut sent = 0usize;
        while sent < target_bytes {
            for i in 0..16 {
                let ev = Ev {
                    n: i,
                    pad: payload.clone(),
                };
                let _ = txn.push(&ev).await.unwrap();
                sent += 4 + bincode::serde::encode_to_vec(&ev, bincode::config::standard())
                    .unwrap()
                    .len();
            }
            let _wm = txn.flush().await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rotates_and_reads_across_segments() {
        let dir = tempfile::tempdir().unwrap();

        // Tiny segment threshold to force rotation quickly.
        let cfg = StreamConfig::builder(dir.path())
            .block_target_uncompressed(128 * 1024) // 128 KiB blocks
            .segment_max_bytes(512 * 1024) // 512 KiB segment -> will rotate soon
            .compression(Compression::None) // keep sizes predictable
            .build();

        let writer = AsyncStreamWriter::<codec::SerdeBincode<Ev>>::open(
            cfg.clone(),
            codec::SerdeBincode::<Ev>::new(),
        )
        .await
        .unwrap();

        let txn = writer.transaction().unwrap();

        // Push enough to exceed ~1.2 MiB so we should land in segment 00000002.*
        push_bytes(&txn, 1_300_000).await;

        // Verify new segment files exist
        let p0_log = dir.path().join("00000000.log");
        let p0_idx = dir.path().join("00000000.idx");
        let p1_log = dir.path().join("00000001.log");
        let p1_idx = dir.path().join("00000001.idx");
        assert!(p0_log.exists() && p0_idx.exists(), "segment 0 missing");
        assert!(
            p1_log.exists() && p1_idx.exists(),
            "segment 1 missing (rotation failed)"
        );

        // Reader should seamlessly scan across segments
        let reader = AsyncStreamReader::<codec::SerdeBincode<Ev>>::open(
            cfg.clone(),
            codec::SerdeBincode::<Ev>::new(),
        )
        .await
        .unwrap();

        let mut ids = Vec::new();
        let mut s = reader.from(Offset(0));
        while let Some((off, _ev)) = s.try_next().await.unwrap() {
            ids.push(off.0);
        }

        // IDs must be contiguous starting at 0
        for (i, id) in ids.iter().enumerate() {
            assert_eq!(*id as usize, i);
        }

        txn.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tail_and_reader_handle_extreme_block_rotation() {
        const TOTAL: u64 = 100;
        let dir = tempfile::tempdir().unwrap();

        let cfg = StreamConfig::builder(dir.path())
            .block_target_uncompressed(1)
            .segment_max_bytes(1 << 20)
            .compression(Compression::None)
            .build();

        let writer = AsyncStreamWriter::<codec::SerdeBincode<IdEvent>>::open(
            cfg.clone(),
            codec::SerdeBincode::<IdEvent>::new(),
        )
        .await
        .unwrap();

        let txn = writer.transaction().unwrap();

        let tail_handle = {
            let tail_stream = txn.tail(Offset(0));
            tokio::spawn(async move {
                tail_stream
                    .map_ok(|(off, ev)| (off.0, ev))
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap()
            })
        };

        for id in 0..TOTAL {
            txn.push(&IdEvent { id }).await.unwrap();
            let wm = txn.flush().await.unwrap().expect("watermark");
            assert_eq!(wm.0, id, "watermark mismatch after flushing id {id}");
        }
        assert_eq!(writer.watermark(), Some(Offset(TOTAL - 1)));

        txn.close().await.unwrap();

        let tail_events = tail_handle.await.unwrap();
        assert_id_sequence("tail", &tail_events, TOTAL);

        let reader = AsyncStreamReader::<codec::SerdeBincode<IdEvent>>::open(
            cfg.clone(),
            codec::SerdeBincode::<IdEvent>::new(),
        )
        .await
        .unwrap();

        let mut reader_events = Vec::new();
        let mut stream = reader.from(Offset(0));
        while let Some((off, ev)) = stream.try_next().await.unwrap() {
            reader_events.push((off.0, ev));
        }
        assert_id_sequence("reader", &reader_events, TOTAL);

        assert_eq!(tail_events, reader_events, "tail and reader diverged");

        let index = InMemIndex::load_all(&cfg.dir, cfg.read_buffer).unwrap();
        assert_eq!(index.entries.count(), TOTAL as usize);
        for (i, entry) in index.entries.iter() {
            assert_eq!(entry.segment_id, 0, "expected single segment for entry {i}");
            let id = Offset(i as u64);
            assert_eq!(entry.first_id, id, "first_id mismatch at entry {i}");
            assert_eq!(entry.last_id, id, "last_id mismatch at entry {i}");
            assert_eq!(
                entry.records, 1,
                "expected one record per block at entry {i}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tail_and_reader_handle_extreme_segment_rotation() {
        const TOTAL: u64 = 100;
        let dir = tempfile::tempdir().unwrap();

        let cfg = StreamConfig::builder(dir.path())
            .block_target_uncompressed(1 << 20)
            .segment_max_bytes(1)
            .compression(Compression::None)
            .build();

        let writer = AsyncStreamWriter::<codec::SerdeBincode<IdEvent>>::open(
            cfg.clone(),
            codec::SerdeBincode::<IdEvent>::new(),
        )
        .await
        .unwrap();

        let txn = writer.transaction().unwrap();

        let tail_handle = {
            let tail_stream = txn.tail(Offset(0));
            tokio::spawn(async move {
                tail_stream
                    .map_ok(|(off, ev)| (off.0, ev))
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap()
            })
        };

        for id in 0..TOTAL {
            txn.push(&IdEvent { id }).await.unwrap();
            let wm = txn.flush().await.unwrap().expect("watermark");
            assert_eq!(wm.0, id, "watermark mismatch after flushing id {id}");
        }
        assert_eq!(writer.watermark(), Some(Offset(TOTAL - 1)));

        txn.close().await.unwrap();

        let tail_events = tail_handle.await.unwrap();
        assert_id_sequence("tail", &tail_events, TOTAL);

        let reader = AsyncStreamReader::<codec::SerdeBincode<IdEvent>>::open(
            cfg.clone(),
            codec::SerdeBincode::<IdEvent>::new(),
        )
        .await
        .unwrap();

        let mut reader_events = Vec::new();
        let mut stream = reader.from(Offset(0));
        while let Some((off, ev)) = stream.try_next().await.unwrap() {
            reader_events.push((off.0, ev));
        }
        assert_id_sequence("reader", &reader_events, TOTAL);
        assert_eq!(tail_events, reader_events, "tail and reader diverged");

        let index = InMemIndex::load_all(&cfg.dir, cfg.read_buffer).unwrap();
        assert_eq!(index.entries.count(), TOTAL as usize);
        for (i, entry) in index.entries.iter() {
            let id = i as u64;
            assert_eq!(entry.segment_id, id, "segment mismatch at entry {i}");
            assert_eq!(entry.first_id, Offset(id), "first_id mismatch at entry {i}");
            assert_eq!(entry.last_id, Offset(id), "last_id mismatch at entry {i}");
            assert_eq!(
                entry.records, 1,
                "expected single-record segments at entry {i}"
            );
        }

        // The next empty segment should already exist because each flush rotated.
        let next_segment = format!("{TOTAL:08}.idx");
        assert!(
            std::fs::metadata(cfg.dir.join(&next_segment)).is_ok(),
            "expected placeholder index file {} for next segment",
            next_segment
        );
    }
}
