use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use clap::{Parser, Subcommand};

use behemoth::{
    AsyncStreamReader, AsyncStreamWriter, Compression, InMemIndex, Offset, SerdeBincode,
    StreamConfig,
};

#[derive(clap::Parser, Debug)]
#[command(name = "amber-log-cli", version, about = "Example CLI for amber-log")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// add <path> <n> <flush>: append n elements, flushing every <flush> elements
    Add {
        /// Directory path for the stream (segment files live here)
        path: PathBuf,
        /// Number of elements to add
        n: u64,
        /// Flush interval (e.g., 1000 means call flush() every 1000 pushes)
        flush: u64,
    },
    /// list-blocks <path>: list all blocks and their metadata (IndexRef)
    ListBlocks {
        /// Directory path for the stream
        path: PathBuf,
    },
    /// read <path> <from>: print "<offset> <id> <first16hex>"
    Read {
        /// Directory path for the stream
        path: PathBuf,
        /// Starting logical offset to read from
        from: u64,
        /// Optional number of elements to read (default = all)
        n: Option<u64>,
    },
}

/// One record in the stream.
/// - `seq`: position within this run/batch [0..n)
/// - `id`:  absolute/global id = (count so far) + seq
/// - `data`: 256 bytes randomness (stored as Vec<u8> for serde/bincode)
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct Element {
    seq: u64,
    id: u64,
    data: Vec<u8>, // always len()==256
}

/* ------------------------ ultra-fast RNG (no deps) ------------------------ */

#[inline(always)]
fn rotl(x: u64, k: u32) -> u64 {
    (x << k) | (x >> (64 - k))
}

/// SplitMix64 — great one-shot seeder (public domain).
struct SplitMix64 {
    state: u64,
}
impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }
    #[inline(always)]
    fn next(&mut self) -> u64 {
        let mut z = {
            self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            self.state
        };
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// xoroshiro128++ — tiny, fast, good quality (not crypto).
struct Xoroshiro128pp {
    s0: u64,
    s1: u64,
}
impl Xoroshiro128pp {
    fn from_seed(seed: u64) -> Self {
        // expand 64-bit seed into 128-bit state via splitmix64
        let mut sm = SplitMix64::new(seed);
        Self {
            s0: sm.next(),
            s1: sm.next(),
        }
    }
    #[inline(always)]
    fn next_u64(&mut self) -> u64 {
        let s0 = self.s0;
        let mut s1 = self.s1;
        let result = rotl(s0.wrapping_add(s1), 17).wrapping_add(s0);
        s1 ^= s0;
        self.s0 = rotl(s0, 49) ^ s1 ^ (s1 << 21);
        self.s1 = rotl(s1, 28);
        result
    }
    #[inline(always)]
    fn fill_256(&mut self, out: &mut [u8]) {
        debug_assert_eq!(out.len(), 256);
        // write 32 u64s (256 bytes)
        for chunk in out.chunks_exact_mut(8) {
            let v = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&v);
        }
    }
}

/// cheap, stable hash for seeding (fxhash-like mix).
#[inline(always)]
fn mix64(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^ (x >> 33)
}

fn seed_from(path: &Path, watermark: u64) -> u64 {
    let nanos = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u128)
        .unwrap_or(0) as u64;

    // incorporate path bytes
    let mut h: u64 =
        0x1234_5678_9abc_def0 ^ nanos ^ (watermark.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    if let Some(s) = path.to_str() {
        for &b in s.as_bytes() {
            h = h.rotate_left(5) ^ (b as u64);
            h = h.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        }
    }
    mix64(h)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.cmd {
        Cmd::Add { path, n, flush } => cmd_add(path, n, flush).await?,
        Cmd::ListBlocks { path } => cmd_list_blocks(path)?,
        Cmd::Read { path, from, n } => cmd_read(path, from, n).await?,
    }

    Ok(())
}

/// add <path> <n> <flush>
async fn cmd_add(
    path: PathBuf,
    n: u64,
    flush_every: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = StreamConfig::builder(&path)
        .compression(Compression::Zstd { level: 3 })
        .build();
    let writer =
        AsyncStreamWriter::<SerdeBincode<Element>>::open(cfg.clone(), SerdeBincode::new()).await?;

    // Compute base id = current watermark + 1, or 0 if empty.
    let base = writer
        .watermark()
        .map(|wm| wm.saturating_add(1))
        .map(|o| o.0)
        .unwrap_or(0);

    let mut rng = Xoroshiro128pp::from_seed(seed_from(&path, base));

    let started = Instant::now();
    let mut last_tick = started;
    let mut encoded_bytes_since_tick: usize = 0;
    let mut pushed_since_tick: u64 = 0;

    let txn = writer.transaction()?;

    for i in 0..n {
        // Build element
        let mut buf = vec![0u8; 256];
        rng.fill_256(&mut buf);

        let el = Element {
            seq: i,
            id: base + i,
            data: buf,
        };

        // For throughput stats, pre-encode with bincode (same config as codec).
        // This is purely for measuring; the writer will encode again via the codec.
        let enc = bincode::serde::encode_to_vec(&el, bincode::config::standard())?;
        encoded_bytes_since_tick += enc.len();
        pushed_since_tick += 1;

        // Push (using the streaming writer)
        txn.push(&el).await?;

        // Periodic flush
        if flush_every > 0 && (i + 1) % flush_every == 0 {
            let _wm = txn.flush().await?;
        }

        // Progress + speed every ~500ms
        if last_tick.elapsed() >= Duration::from_millis(500) || i + 1 == n {
            let elapsed = last_tick.elapsed().as_secs_f64();
            let recs_per_sec = (pushed_since_tick as f64) / elapsed;
            let mb_per_sec = (encoded_bytes_since_tick as f64) / (1024.0 * 1024.0) / elapsed;

            print!(
                "\rwriting: {}/{} ({:.1}%) | {:.0} rec/s | {:.2} MiB/s",
                i + 1,
                n,
                ((i + 1) as f64) * 100.0 / (n as f64),
                recs_per_sec,
                mb_per_sec
            );
            std::io::Write::flush(&mut std::io::stdout())?;

            last_tick = Instant::now();
            encoded_bytes_since_tick = 0;
            pushed_since_tick = 0;
        }
    }

    // Final flush if the last batch didn't land on the boundary
    let _ = txn.flush().await?;
    txn.close().await?;
    println!("\nfinished in {:.2}s", started.elapsed().as_secs_f64());

    Ok(())
}

/// list-blocks <path>
fn cmd_list_blocks(path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    // Load the in-memory index and print every entry (IndexRef).
    // (InMemIndex is re-exported from the library.)
    let cfg = StreamConfig::builder(&path).build();
    let index = InMemIndex::load_all(&cfg.dir, cfg.read_buffer)?;

    if index.entries.is_empty() {
        println!("(no blocks)");
        return Ok(());
    }

    println!(
        "{:>4} {:>10} {:>10} {:>12} {:>10} {:>10} {:>8}",
        "seg", "first_id", "last_id", "file_offset", "blk_len", "uncomp", "recs"
    );
    for (_, e) in index.entries.iter() {
        println!(
            "{:>4} {:>10} {:>10} {:>12} {:>10} {:>10} {:>8}",
            e.segment_id,
            e.first_id.0,
            e.last_id.0,
            e.file_offset,
            e.block_len,
            e.uncompressed_len,
            e.records
        );
    }
    Ok(())
}

/// read <path> <from>
async fn cmd_read(
    path: PathBuf,
    from: u64,
    n: Option<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = StreamConfig::builder(&path).build();
    let reader = AsyncStreamReader::<SerdeBincode<Element>>::open(cfg, SerdeBincode::new()).await?;

    let mut i = 0u64;
    let max = n.unwrap_or(u64::MAX);

    use futures_util::TryStreamExt;
    let mut s = reader.from(Offset(from));
    while let Some((off, el)) = s.try_next().await? {
        // first 16 hex chars = first 8 bytes as hex
        let n = el.data.len().min(8);
        let mut hex = String::with_capacity(n * 2);
        for b in &el.data[..n] {
            use std::fmt::Write as _;
            let _ = write!(hex, "{:02x}", b);
        }
        println!("{} {} {} {}", off.0, el.id, el.seq, hex);
        i += 1;
        if i >= max {
            break;
        }
    }
    Ok(())
}
