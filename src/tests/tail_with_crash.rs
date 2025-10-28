#[cfg(test)]
mod tests {
    use crate::transaction::Transaction;
    use crate::{AsyncStreamReader, AsyncStreamWriter, Compression, Offset, StreamConfig, codec};
    use futures_util::TryStreamExt;
    use serde::{Deserialize, Serialize};
    use std::time::Duration;
    use tokio::time::sleep;

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
    struct Ev {
        id: u64,
        pad: Vec<u8>,
    }

    // Utility: push a range [start, end) *without* flush; caller decides when to flush.
    async fn push_range<C: crate::codec::Codec<Value = Ev>>(
        txn: &Transaction<C>,
        start: u64,
        end: u64,
        flush: Option<u64>,
    ) {
        for i in start..end {
            txn.push(&Ev {
                id: i,
                pad: vec![0u8; 8 * 1024],
            })
            .await
            .unwrap(); // ~8 KiB/event
            if let Some(flush) = flush
                && i % flush == 0
            {
                txn.flush().await.unwrap();
            }
        }
        if flush.is_some() {
            txn.flush().await.unwrap();
        }
    }

    // Utility: collect a whole stream until it ends.
    async fn collect_all<S, T, E>(mut s: S) -> Vec<T>
    where
        S: futures_core::Stream<Item = Result<T, E>> + Unpin,
        E: std::fmt::Debug,
    {
        let mut out = Vec::new();
        while let Some(item) = s.try_next().await.unwrap() {
            out.push(item);
        }
        out
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn integration_covers_tail_crash_recover_rotation_and_bounds() {
        let dir = tempfile::tempdir().unwrap();

        // Small thresholds to force multiple blocks + rotation quickly; enable Zstd path.
        let cfg = StreamConfig::builder(dir.path())
            .block_target_uncompressed(32 * 1024) // tiny blocks
            .segment_max_bytes(128 * 1024) // tiny segment → rotates soon
            .compression(Compression::None)
            .build();

        // --- Phase 1: create writer1 and a tailer before any data exists ---
        let writer1 = AsyncStreamWriter::<codec::SerdeBincode<Ev>>::open(
            cfg.clone(),
            codec::SerdeBincode::<Ev>::new(),
        )
        .await
        .unwrap();

        let txn1 = writer1.transaction().unwrap();

        let tail1 = txn1.tail(Offset(0)); // should see only committed data and end if writer drops

        // Wave A: write [0..40), flush → committed (tail should see these)
        push_range(&txn1, 0, 40, Some(10)).await;

        // Wave B: write [40..60) but DO NOT flush — simulate a crash afterwards.
        push_range(&txn1, 40, 60, None).await;

        // Give the tail a tiny moment to interleave reads (still only sees [0..39]).
        sleep(Duration::from_millis(10)).await;

        // Simulate crash: drop writer without close() or flush().
        drop(txn1);
        drop(writer1);

        // The tail’s watermark sender is dropped; it should now end naturally and
        // must have yielded exactly [0..40) records.
        let got_tail1: Vec<(Offset, Ev)> = collect_all(tail1).await;
        assert_eq!(got_tail1.len(), 40);
        for (i, (off, ev)) in got_tail1.iter().enumerate() {
            assert_eq!(off.0 as usize, i);
            assert_eq!(ev.id as usize, i);
        }

        // --- Phase 2: recovery check with a brand-new read-only open ---
        // Reopen a reader; the open path truncates the .log to the last valid index entry.
        let reader_after_crash = AsyncStreamReader::<codec::SerdeBincode<Ev>>::open(
            cfg.clone(),
            codec::SerdeBincode::<Ev>::new(),
        )
        .await
        .unwrap();

        // Bounded read should see exactly [0..40).
        let mut s = reader_after_crash.from(Offset(0));
        let mut after_crash = Vec::new();
        while let Some((off, ev)) = s.try_next().await.unwrap() {
            after_crash.push((off.0, ev.id));
        }
        assert_eq!(after_crash.len(), 40);
        for (i, (off, val)) in after_crash.iter().enumerate() {
            assert_eq!(*off as usize, i);
            assert_eq!(*val as usize, i);
        }

        // --- Phase 3: continue with writer2, force rotation, then close cleanly ---
        let writer2 = AsyncStreamWriter::<codec::SerdeBincode<Ev>>::open(
            cfg.clone(),
            codec::SerdeBincode::<Ev>::new(),
        )
        .await
        .unwrap();

        let txn2 = writer2.transaction().unwrap();

        // Continue IDs from 60 upward to make continuity easy to spot later.
        // Do multiple flushes to cross block and segment thresholds.
        let total_more = 100u64; // write 100 more records
        let batch = 25u64;
        let mut start = 60u64;
        while start < 60 + total_more {
            let end = (start + batch).min(60 + total_more);
            push_range(&txn2, start, end, Some(10)).await;
            start = end;
        }

        // Close cleanly; this must flush any final open block and end of stream.
        txn2.close().await.unwrap();

        // Sanity: rotated files should exist (00000001.* at least) because thresholds are tiny.
        let p0_log = dir.path().join("00000000.log");
        let p1_log = dir.path().join("00000001.log");
        assert!(p0_log.exists(), "segment 0 log missing");
        assert!(
            p1_log.exists(),
            "expected rotation; segment 1 log not found (thresholds too large?)"
        );

        // --- Phase 4: final bounded read of everything that survived + new writes ---
        let final_reader = AsyncStreamReader::<codec::SerdeBincode<Ev>>::open(
            cfg.clone(),
            codec::SerdeBincode::<Ev>::new(),
        )
        .await
        .unwrap();

        let mut all = Vec::new();
        let mut s2 = final_reader.from(Offset(0));
        while let Some((off, ev)) = s2.try_next().await.unwrap() {
            all.push((off.0, ev.id));
        }

        // We expect 40 records from before the crash, *no* unflushed [40..60),
        // plus 100 records from writer2: total = 140, with contiguous IDs except the intentional gap.
        assert_eq!(all.len(), 140, "expected 140 committed records total");

        // Check the first 40
        for i in 0..40 {
            assert_eq!(all[i as usize], (i, i));
        }
        // Check the last 100 (starting at 60)
        for (k, pair) in all.iter().skip(40).enumerate() {
            let expected_idx = 40 + k as u64;
            let expected_id = 60 + k as u64;
            assert_eq!(*pair, (expected_idx, expected_id));
        }
    }
}
