#[cfg(test)]
mod tests {
    use crate::AsyncStreamReader;
    use crate::AsyncStreamWriter;
    use crate::Offset;
    use crate::StreamConfig;
    use crate::codec;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    struct Ev(u32);

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn push_flush_read() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = StreamConfig::builder(dir.path()).build();

        let writer = AsyncStreamWriter::<codec::SerdeBincode<Ev>>::open(
            cfg.clone(),
            codec::SerdeBincode::<Ev>::new(),
        )
        .await
        .unwrap();

        writer.push(&Ev(1)).await.unwrap();
        writer.push(&Ev(2)).await.unwrap();
        writer.push(&Ev(3)).await.unwrap();

        let wm = writer.flush().await.unwrap().unwrap();
        assert_eq!(wm.0, 2);

        // bounded reader
        let reader = AsyncStreamReader::<codec::SerdeBincode<Ev>>::open(
            cfg,
            codec::SerdeBincode::<Ev>::new(),
        )
        .await
        .unwrap();
        let mut got = Vec::new();
        use futures_util::TryStreamExt;
        let mut s = reader.from(Offset(0));
        while let Some((off, ev)) = s.try_next().await.unwrap() {
            got.push((off.0, ev));
        }
        assert_eq!(got, vec![(0, Ev(1)), (1, Ev(2)), (2, Ev(3))]);

        writer.close().await.unwrap();
    }
}
