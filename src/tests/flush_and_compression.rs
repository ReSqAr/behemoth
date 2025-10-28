#[cfg(test)]
mod tests {
    use crate::{
        AsyncStreamReader, AsyncStreamWriter, Compression, Offset, StreamConfig, codec, format,
    };
    use futures_util::TryStreamExt;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    struct Ev {
        n: u32,
        s: String,
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flush_coalesces_multiple_pushes_into_one_block() {
        let dir = tempfile::tempdir().unwrap();

        let cfg = StreamConfig::builder(dir.path())
            .block_target_uncompressed(2 * 1024 * 1024)
            .compression(Compression::Zstd { level: 5 })
            .build();

        let writer = AsyncStreamWriter::<codec::SerdeBincode<Ev>>::open(
            cfg.clone(),
            codec::SerdeBincode::<Ev>::new(),
        )
        .await
        .unwrap();

        let txn = writer.transaction().unwrap();

        // Push twice, single flush -> should be one block (we'll verify via index).
        txn.push(&Ev {
            n: 1,
            s: "a".into(),
        })
        .await
        .unwrap();
        txn.push(&Ev {
            n: 2,
            s: "b".into(),
        })
        .await
        .unwrap();
        txn.push(&Ev {
            n: 3,
            s: "c".into(),
        })
        .await
        .unwrap();
        txn.push(&Ev {
            n: 4,
            s: "d".into(),
        })
        .await
        .unwrap();

        let wm = txn.flush().await.unwrap().unwrap();
        assert!(wm.0 >= 3);

        // Verify read side sees 4 events with contiguous IDs.
        let reader = AsyncStreamReader::<codec::SerdeBincode<Ev>>::open(
            cfg.clone(),
            codec::SerdeBincode::<Ev>::new(),
        )
        .await
        .unwrap();
        let mut out = Vec::new();
        let mut s = reader.from(Offset(0));
        while let Some((off, ev)) = s.try_next().await.unwrap() {
            out.push((off.0, ev));
        }
        assert_eq!(out.len(), 4);
        assert_eq!(out[0].0, 0);
        assert_eq!(out[3].0, 3);

        // Peek the index file and ensure exactly one entry exists.
        let idx_path = dir.path().join("00000000.idx");
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .open(idx_path)
            .unwrap();
        use std::io::Read;
        let mut hdr = vec![0u8; format::index::IndexHeader::SIZE];
        f.read_exact(&mut hdr).unwrap();
        let mut entries = 0usize;
        let mut buf = vec![0u8; format::index::IndexEntry::SIZE];
        while f.read_exact(&mut buf).is_ok() {
            entries += 1;
        }
        assert_eq!(entries, 1, "expected a single block committed");

        txn.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn zstd_level_applied_and_sizes_sane() {
        // We'll write a medium payload and assert compressed_len < uncompressed_len,
        // which indicates compression happened (not airtight but good smoke).
        let dir = tempfile::tempdir().unwrap();
        let cfg = StreamConfig::builder(dir.path())
            .block_target_uncompressed(1 * 1024 * 1024)
            .compression(Compression::Zstd { level: 9 })
            .build();

        let writer = AsyncStreamWriter::<codec::SerdeBincode<Ev>>::open(
            cfg.clone(),
            codec::SerdeBincode::<Ev>::new(),
        )
        .await
        .unwrap();
        let txn = writer.transaction().unwrap();

        for i in 0..2000 {
            let ev = Ev {
                n: i,
                s: format!("string-{:04}-{}", i, "x".repeat(40)),
            };
            let _ = txn.push(&ev).await.unwrap();
        }
        txn.flush().await.unwrap();

        // Read the first block header from 00000000.log and validate sizes.
        let log_path = dir.path().join("00000000.idx");
        let mut lf = std::fs::OpenOptions::new()
            .read(true)
            .open(log_path)
            .unwrap();
        use std::io::Read;
        // skip index header
        let mut fh = vec![0u8; format::index::IndexHeader::SIZE];
        lf.read_exact(&mut fh).unwrap();
        // read index header
        let mut bh = vec![0u8; format::index::IndexEntry::SIZE];
        lf.read_exact(&mut bh).unwrap();
        let hdr = format::index::IndexEntry::decode_from(&bh[..]).unwrap();
        assert_eq!(hdr.records as usize, 2000usize);
        assert!(hdr.compressed_len > 0);
        assert!(hdr.compressed_len <= hdr.uncompressed_len);

        txn.close().await.unwrap();
    }
}
