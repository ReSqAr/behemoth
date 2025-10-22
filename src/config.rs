// src/config.rs
use std::path::PathBuf;

/// Stream runtime/configuration parameters.
/// Build with `StreamConfig::builder(dir).foo(...).build()`.
#[derive(Clone, Debug)]
pub struct StreamConfig {
    /// Directory that contains the segment files: 00000000.log/.idx, 00000001.*, ...
    pub dir: PathBuf,

    /// Target uncompressed block size (bytes). The writer may coalesce multiple push_batch calls
    /// into one block until `flush()` is called; this is a sizing hint for block assembly.
    /// Typical HDD-friendly value: 1–4 MiB.
    pub block_target_uncompressed: usize,

    /// Max size of a single segment `.log` before rotating to the next segment (bytes).
    /// E.g. 512 MiB by default.
    pub segment_max_bytes: u64,

    /// Compression to use for blocks.
    pub compression: crate::Compression,

    pub read_buffer: usize,
    pub write_buffer: usize,
}

impl StreamConfig {
    /// Start building a config with sane defaults.
    ///
    /// Defaults:
    /// - block_target_uncompressed = 2 MiB
    /// - segment_max_bytes         = 512 MiB
    /// - compression               = Zstd { level: 3 }
    /// - read_ahead_blocks         = 2
    pub fn builder(dir: impl Into<PathBuf>) -> StreamConfigBuilder {
        StreamConfigBuilder {
            dir: dir.into(),
            block_target_uncompressed: 2 * 1024 * 1024,
            segment_max_bytes: 512 * 1024 * 1024,
            compression: crate::Compression::Zstd { level: 3 },
            read_buffer: 1024 * 1024,
            write_buffer: 1024 * 1024,
        }
    }
}

/// Fluent builder for `StreamConfig`.
#[derive(Clone, Debug)]
pub struct StreamConfigBuilder {
    dir: PathBuf,
    block_target_uncompressed: usize,
    segment_max_bytes: u64,
    compression: crate::Compression,
    read_buffer: usize,
    write_buffer: usize,
}

impl StreamConfigBuilder {
    pub fn block_target_uncompressed(mut self, bytes: usize) -> Self {
        self.block_target_uncompressed = bytes;
        self
    }
    pub fn segment_max_bytes(mut self, bytes: u64) -> Self {
        self.segment_max_bytes = bytes;
        self
    }
    pub fn compression(mut self, c: crate::Compression) -> Self {
        self.compression = c;
        self
    }
    pub fn read_buffer(mut self, bufsize: usize) -> Self {
        self.read_buffer = bufsize;
        self
    }
    pub fn write_buffer(mut self, bufsize: usize) -> Self {
        self.write_buffer = bufsize;
        self
    }

    /// Finalize with basic validation and return the config.
    pub fn build(self) -> StreamConfig {
        let block = self.block_target_uncompressed;
        let seg = self.segment_max_bytes;
        StreamConfig {
            dir: self.dir,
            block_target_uncompressed: block,
            segment_max_bytes: seg,
            compression: self.compression,
            read_buffer: self.read_buffer,
            write_buffer: self.write_buffer,
        }
    }
}
