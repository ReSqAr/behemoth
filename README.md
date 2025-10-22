# Behemoth

**Behemoth** is a durable, append-only log engine for structured data — an experimental async storage substrate for event streams.

It’s built in Rust, with a focus on correctness, simplicity, and low-latency block commits.
Not production-ready (yet) — but it’s *largely functional*, passes all 15 tests, and already writes around **170 MB/s** for 256 B objects (with compression).

---

## What it does

* Append structured records (`serde`-serializable values) to disk-backed segments
* Compress blocks using `zstd` (or store uncompressed)
* Maintain an on-disk index per segment for fast sequential reads
* Expose both:

    * a **bounded reader** for replay/catch-up up to the current watermark
    * a **writer tailer** that streams newly flushed data until the writer closes
* Recover automatically from partial writes by truncating to the last valid block
* Segment rotation out of the box

---

## Use cases

* durable local event logs for analytics or ingest pipelines
* local-first queue or WAL for embedded databases
* lightweight testbed for high-throughput async I/O patterns
* foundation for stream processors or state snapshots

Essentially, it’s a small **“commit log engine”** you can embed into a service without pulling in Kafka, NATS, or RocksDB.

---

## CLI playground

The repo includes a small CLI (`behemoth-cli`) for benchmarking and poking at the format.

```bash
# Add 10M random 256B records, flushing every 100k
cargo run --release --bin behemoth-cli -- add ./behemoth-test 10000000 100000

# List committed blocks in the stream
cargo run --release --bin behemoth-cli -- list-blocks ./behemoth-test

# Read records starting at offset 6,591,900 (read 200 records)
cargo run --release --bin behemoth-cli -- read ./behemoth-test 6591900 200
```

You’ll see throughput, segment rotation, and decompression in action.

---

## Name origin

> *“Behemoth”* was originally the **generation ship** *Nauvoo* in *The Expanse* —
> a symbol of massive engineering repurposed for real work.
>
> This crate follows the same spirit: heavy, honest machinery —
> built to move data steadily through the void.

---

## ⚠️ Status

* **Not production-ready**
* **“Vibe-coded”** - test coverage is at 92%
* **No compaction, no recovery beyond truncation**
* **Unoptimized** yet still fast (≈ 170 MB/s in synthetic tests)

Expect rough edges, but it’s a strong base for experimentation.

### TODOs

* refactor and 'own' the code
* more tests - in particular concerning crash recovery
* simplify & optimise the tailer
* optimise the code and be multiprocessing friendly - the theoretical performance is the speed of the disk, that means GB/s

---

