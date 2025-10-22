#[cfg(test)]
mod tests {
    use crate::{AsyncStreamReader, AsyncStreamWriter, Compression, Offset, StreamConfig, codec};
    use futures_util::TryStreamExt;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
    struct Ev {
        n: u64,
        pad: Vec<u8>,
    }

    // Helper to push ~N bytes (uncompressed) via multiple events.
    async fn push_bytes<C: crate::codec::Codec<Value = Ev>>(
        writer: &crate::AsyncStreamWriter<C>,
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
                let _ = writer.push(&ev).await.unwrap();
                sent += 4 + bincode::serde::encode_to_vec(&ev, bincode::config::standard())
                    .unwrap()
                    .len();
            }
            let _wm = writer.flush().await.unwrap();
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

        // Push enough to exceed ~1.2 MiB so we should land in segment 00000002.*
        push_bytes(&writer, 1_300_000).await;

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

        writer.close().await.unwrap();
    }
}
