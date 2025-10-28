#[cfg(test)]
mod tests {
    use crate::{AsyncStreamReader, AsyncStreamWriter, Offset, StreamConfig, codec};
    use futures_util::TryStreamExt;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
    struct Ev(u64);

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bounded_reads_skip_to_requested_offset() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = StreamConfig::builder(dir.path())
            .block_target_uncompressed(32 * 1024)
            .build();

        let writer = AsyncStreamWriter::<codec::SerdeBincode<Ev>>::open(
            cfg.clone(),
            codec::SerdeBincode::<Ev>::new(),
        )
        .await
        .unwrap();

        let txn = writer.transaction().unwrap();

        for i in 0..30u64 {
            txn.push(&Ev(i)).await.unwrap();
            if i % 5 == 4 {
                // Seal blocks frequently so the index has multiple entries to scan.
                txn.flush().await.unwrap();
            }
        }
        // Ensure no buffered records remain.
        txn.flush().await.unwrap();
        txn.close().await.unwrap();

        let reader = AsyncStreamReader::<codec::SerdeBincode<Ev>>::open(
            cfg.clone(),
            codec::SerdeBincode::<Ev>::new(),
        )
        .await
        .unwrap();

        let watermark = reader.snapshot_watermark().expect("watermark should exist");
        assert_eq!(watermark, Offset(29));

        let subset: Vec<(Offset, Ev)> = reader.from(Offset(10)).try_collect().await.unwrap();
        assert_eq!(subset.len(), 20, "expected records [10..30)");
        for (idx, (off, ev)) in subset.iter().enumerate() {
            let expected = 10 + idx as u64;
            assert_eq!(off.0, expected, "offset mismatch at {idx}");
            assert_eq!(ev.0, expected, "value mismatch at {idx}");
        }

        let empty: Vec<(Offset, Ev)> = reader.from(Offset(30)).try_collect().await.unwrap();
        assert!(empty.is_empty(), "no records should exist beyond watermark");
    }
}
