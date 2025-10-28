#[cfg(test)]
mod tests {
    use crate::{AsyncStreamReader, AsyncStreamWriter, Offset, StreamConfig, codec, format};
    use serde::{Deserialize, Serialize};
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::io::Write;

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    struct Ev(u32);

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partial_index_entry_is_discarded_on_startup() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = StreamConfig::builder(dir.path()).build();

        // 1) write a first committed block
        let writer = AsyncStreamWriter::<codec::SerdeBincode<Ev>>::open(
            cfg.clone(),
            codec::SerdeBincode::<Ev>::new(),
        )
        .await
        .unwrap();
        let txn = writer.transaction().unwrap();
        txn.push(&Ev(1)).await.unwrap();
        txn.push(&Ev(2)).await.unwrap();
        let wm1 = txn.flush().await.unwrap().unwrap();
        assert_eq!(wm1.0, 1);
        txn.close().await.unwrap();

        // 2) Corrupt the index by appending half of an IndexEntry to 00000000.idx
        let idx_path = dir.path().join("00000000.idx");
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&idx_path)
            .unwrap();
        let len = f.metadata().unwrap().len();
        f.seek(SeekFrom::Start(len)).unwrap();
        let garbage = vec![0xABu8; format::index::IndexEntry::SIZE / 2];
        f.write_all(&garbage).unwrap();
        f.flush().unwrap();

        // 3) Re-open (this triggers truncate of .log to last *valid* index entry)
        //    and load a reader; watermark must still be wm1.
        let reader = AsyncStreamReader::<codec::SerdeBincode<Ev>>::open(
            cfg.clone(),
            codec::SerdeBincode::<Ev>::new(),
        )
        .await
        .unwrap();
        assert_eq!(reader.snapshot_watermark().unwrap().0, wm1.0);

        // 4) And scanning should yield exactly the original 2 records, not more.
        use futures_util::TryStreamExt;
        let mut got = Vec::new();
        let mut s = reader.from(Offset(0));
        while let Some((off, ev)) = s.try_next().await.unwrap() {
            got.push((off.0, ev));
        }
        assert_eq!(got, vec![(0, Ev(1)), (1, Ev(2))]);
    }
}
