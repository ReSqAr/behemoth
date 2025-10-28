#[cfg(test)]
mod tests {
    use crate::{AsyncStreamWriter, Offset, StreamConfig, codec};
    use futures_util::TryStreamExt;
    use serde::{Deserialize, Serialize};
    use std::time::Duration;
    use tokio::time::sleep;

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
    struct Ev(u32);

    // This tests `Writer::from` (the tailing/bounded-by-close stream on the writer).
    // We start the stream first, then push + flush in waves, and finally close().
    // Expect: all records [0..N) are yielded in order and the stream ends.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writer_from_reads_everything_until_close() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = StreamConfig::builder(dir.path()).build();

        let writer = AsyncStreamWriter::<codec::SerdeBincode<Ev>>::open(
            cfg.clone(),
            codec::SerdeBincode::<Ev>::new(),
        )
        .await
        .unwrap();

        // Start reading from 0 before any data is committed.
        let txn = writer.transaction().unwrap();
        let stream = txn.tail(Offset(0));

        // In the background: push in waves, flushing between waves.
        let bg = tokio::spawn(async move {
            let total = 20u32;
            let wave = 5u32;

            for start in (0..total).step_by(wave as usize) {
                for i in start..(start + wave).min(total) {
                    txn.push(&Ev(i)).await.unwrap();
                }
                // Commit this wave.
                let _ = txn.flush().await.unwrap();
                // Small pause so the reader can interleave.
                sleep(Duration::from_millis(10)).await;
            }

            // Finalize: close() must flush last open block (if any) and end the stream.
            txn.close().await.unwrap();
        });

        // Collect everything the stream produces until it naturally ends (on close()).
        let got: Vec<(Offset, Ev)> = stream.try_collect().await.unwrap();

        // Background task should be done.
        bg.await.unwrap();

        // We expect exactly 20 contiguous records [0..20)
        assert_eq!(got.len(), 20, "expected 20 records, got {}", got.len());
        for (i, (off, ev)) in got.iter().enumerate() {
            assert_eq!(*off, Offset(i as u64), "offset mismatch at {}", i);
            assert_eq!(*ev, Ev(i as u32), "payload mismatch at {}", i);
        }
    }
}
