use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tempfile::TempDir;

use behemoth::{AsyncStreamWriter, Compression, Offset, StreamConfig, codec::SerdeBincode};

#[inline]
fn xorshift64(mut x: u64) -> u64 {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

#[inline]
fn incompressible_ascii(len: usize, seed: u64) -> String {
    const ALPH: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut s = String::with_capacity(len);
    let mut x = seed;
    for _ in 0..len {
        x = xorshift64(x);
        s.push(ALPH[(x as usize) & 63] as char);
    }
    s
}

#[derive(Serialize, Deserialize, Clone)]
struct BenchRecord {
    id: u64,
    a: u64,
    b: u64,
    name: String, // ~232 bytes, randomized
}

fn bench_writer(c: &mut Criterion) {
    let mut group = c.benchmark_group("writer_256B_records");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(8));

    let n: u64 = 1_000_000;
    let flush_after: u64 = 100_000;
    let cases = [
        ("none", Compression::None),
        ("zstd3", Compression::Zstd { level: 3 }),
    ];

    for (label, compression) in cases {
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(
            BenchmarkId::from_parameter(label),
            &compression,
            |b, &comp| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| {
                        let n = n;
                        async move {
                            let dir = TempDir::new().unwrap();
                            let cfg = StreamConfig::builder(dir.path())
                                .compression(comp.clone())
                                .block_target_uncompressed(2 * 1024 * 1024) // 2 MiB
                                .segment_max_bytes(512 * 1024 * 1024) // 512 MiB
                                .build();

                            let codec = SerdeBincode::<BenchRecord>::new();
                            let writer = AsyncStreamWriter::open(cfg, codec).await.unwrap();
                            let txn = writer.transaction().unwrap();
                            for i in 0..n {
                                let rec = BenchRecord {
                                    id: i,
                                    a: 1,
                                    b: 2,
                                    name: incompressible_ascii(232, 0x9E37_79B9_7F4A_7C15 ^ i),
                                };
                                txn.push(&rec).await.unwrap();

                                if i % flush_after == 0 {
                                    txn.flush().await.unwrap();
                                }
                            }

                            let _wm: Option<Offset> = txn.flush().await.unwrap();
                            txn.close().await.unwrap();
                        }
                    });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_writer);
criterion_main!(benches);
