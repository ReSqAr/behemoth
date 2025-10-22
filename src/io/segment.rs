#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    fs::{File, OpenOptions, read_dir},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crate::format::CodecId;
use crate::format::headers::{BlockFooter, BlockHeader, FileHeader};
use crate::format::index::{IndexEntry, IndexHeader};

/// Pair of opened files for the active segment.
pub struct SegmentFiles {
    pub id: u64,
    pub log: File,
    pub idx: File,
    pub dir: PathBuf,
}

impl SegmentFiles {
    /// Current length of the active log file.
    pub fn log_len(&self) -> io::Result<u64> {
        self.log.metadata().map(|m| m.len())
    }

    /// Append a block header for the given codec, returning its starting offset.
    pub fn append_block_header(&mut self, codec: CodecId) -> io::Result<u64> {
        let offset = self.log_len()?;
        let header = BlockHeader::new(codec);
        let mut buf = Vec::with_capacity(BlockHeader::SIZE);
        header.encode_to(&mut buf)?;
        self.log.write_all(&buf)?;
        Ok(offset)
    }

    /// Append the trailing block footer marker.
    pub fn append_block_footer(&mut self) -> io::Result<()> {
        let footer = BlockFooter::default();
        let mut buf = Vec::with_capacity(BlockFooter::SIZE);
        footer.encode_to(&mut buf)?;
        self.log.write_all(&buf)
    }

    /// Append a new index entry to the active index file.
    pub fn append_index_entry(&mut self, entry: &IndexEntry) -> io::Result<()> {
        let mut buf = Vec::with_capacity(IndexEntry::SIZE);
        entry.encode_to(&mut buf)?;
        self.idx.write_all(&buf)
    }

    /// Sync the log file contents to disk.
    pub fn sync_log(&mut self) -> io::Result<()> {
        fsync_file(&mut self.log)
    }

    /// Sync the index file contents to disk.
    pub fn sync_index(&mut self) -> io::Result<()> {
        fsync_file(&mut self.idx)
    }

    /// Determine whether the active log should rotate based on `max_bytes`.
    pub fn should_rotate(&self, max_bytes: u64) -> io::Result<bool> {
        if max_bytes == 0 {
            return Ok(false);
        }
        Ok(self.log.metadata()?.len() >= max_bytes)
    }
}

pub fn segment_filename(id: u64) -> (String, String) {
    (format!("{id:08}.log"), format!("{id:08}.idx"))
}

pub(crate) fn fsync_file(file: &mut File) -> io::Result<()> {
    file.sync_all()
}

fn fsync_dir(path: &Path) -> io::Result<()> {
    // Open the directory and fsync it to persist entry creates/renames.
    let file = OpenOptions::new().read(true).open(path)?;
    file.sync_all()
}

fn write_new_log_header(f: &mut File) -> io::Result<()> {
    let hdr = FileHeader::default();
    hdr.encode_to(f)
}

fn write_new_idx_header(f: &mut File) -> io::Result<()> {
    let hdr = IndexHeader::default();
    hdr.encode_to(f)
}

/// List available segment ids in `dir` by scanning `*.idx` files.
pub fn list_segments(dir: &Path) -> io::Result<Vec<u64>> {
    let mut out = Vec::new();
    for ent in read_dir(dir)? {
        let ent = ent?;
        if !ent.file_type()?.is_file() {
            continue;
        }
        let name = ent.file_name();
        let name = name.to_string_lossy();
        if let Some(stripped) = name.strip_suffix(".idx") {
            if stripped.len() == 8 && stripped.chars().all(|c| c.is_ascii_digit()) {
                if let Ok(id) = stripped.parse::<u64>() {
                    out.push(id);
                }
            }
        }
    }
    out.sort_unstable();
    Ok(out)
}

/// Open existing segment, validate/truncate log to last complete block per index, return files.
/// If none exist, create segment 00000000.
pub fn open_active_segment(dir: &Path) -> io::Result<SegmentFiles> {
    std::fs::create_dir_all(dir)?;
    let mut segs = list_segments(dir)?;
    let seg_id = if let Some(last) = segs.pop() {
        last
    } else {
        // Create segment 0
        let (log_name, idx_name) = segment_filename(0);
        let log = create_log(dir.join(&log_name))?;
        let idx = create_idx(dir.join(&idx_name))?;
        fsync_dir(dir)?;
        // Trivial header write already done in create_*
        return Ok(SegmentFiles {
            id: 0,
            log,
            idx,
            dir: dir.to_path_buf(),
        });
    };

    let (log_name, idx_name) = segment_filename(seg_id);
    let log_path = dir.join(&log_name);
    let idx_path = dir.join(&idx_name);

    // Open log RW (not append yet; we want to ftruncate first), open idx RDWR append.
    let mut log = OpenOptions::new().read(true).write(true).open(&log_path)?;
    let mut idx = OpenOptions::new().read(true).write(true).open(&idx_path)?;

    // Validate headers exist for both files (best-effort).
    validate_log_header(&mut log)?;
    validate_idx_header(&mut idx)?;

    // Find last complete index entry to compute safe_end.
    let safe_end = find_safe_end_in_index(&mut idx)?;

    // Truncate log to the safe end and fsync.
    log.set_len(safe_end)?;
    fsync_file(&mut log)?;

    // Reopen log in O_APPEND mode for fast appends.
    #[cfg(unix)]
    let log = {
        let mut oo = OpenOptions::new();
        oo.read(true)
            .write(true)
            .create(false)
            .custom_flags(libc::O_APPEND);
        oo.open(&log_path)?
    };
    #[cfg(not(unix))]
    let log = OpenOptions::new().read(true).append(true).open(&log_path)?;

    Ok(SegmentFiles {
        id: seg_id,
        log,
        idx,
        dir: dir.to_path_buf(),
    })
}

fn create_log(path: PathBuf) -> io::Result<File> {
    #[cfg(unix)]
    let f = {
        let mut oo = OpenOptions::new();
        oo.read(true).write(true).create_new(true);
        let mut f = oo.open(&path)?;
        write_new_log_header(&mut f)?;
        fsync_file(&mut f)?;
        f
    };
    #[cfg(not(unix))]
    let f = {
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        write_new_log_header(&mut f)?;
        fsync_file(&mut f)?;
        f
    };
    Ok(f)
}

fn create_idx(path: PathBuf) -> io::Result<File> {
    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)?;
    write_new_idx_header(&mut f)?;
    fsync_file(&mut f)?;
    Ok(f)
}

/// Create and switch to a new segment (id+1). Caller should close old files first.
pub fn rotate_segment(current: &SegmentFiles) -> io::Result<SegmentFiles> {
    let new_id = current.id + 1;
    let (log_name, idx_name) = segment_filename(new_id);

    let _log = create_log(current.dir.join(&log_name))?;
    let idx = create_idx(current.dir.join(&idx_name))?;
    // Persist directory entries
    fsync_dir(&current.dir)?;
    // Reopen log with O_APPEND
    #[cfg(unix)]
    let log = {
        let mut oo = OpenOptions::new();
        oo.read(true).write(true).custom_flags(libc::O_APPEND);
        oo.open(current.dir.join(&log_name))?
    };
    #[cfg(not(unix))]
    let log = OpenOptions::new()
        .read(true)
        .append(true)
        .open(current.dir.join(&log_name))?;

    Ok(SegmentFiles {
        id: new_id,
        log,
        idx,
        dir: current.dir.clone(),
    })
}

/// Validate and skip the fixed-size log header.
fn validate_log_header(f: &mut File) -> io::Result<()> {
    f.seek(SeekFrom::Start(0))?;
    let mut buf = vec![0u8; crate::format::headers::FileHeader::SIZE];
    f.read_exact(&mut buf)?;
    let _ = FileHeader::decode_from(&buf[..])?;
    Ok(())
}

/// Validate and skip the fixed-size index header.
fn validate_idx_header(f: &mut File) -> io::Result<()> {
    f.seek(SeekFrom::Start(0))?;
    let mut buf = vec![0u8; crate::format::index::IndexHeader::SIZE];
    f.read_exact(&mut buf)?;
    let _ = IndexHeader::decode_from(&buf[..])?;
    Ok(())
}

/// Scan the index to the last complete entry and compute safe_end (offset + block_len).
fn find_safe_end_in_index(idx: &mut File) -> io::Result<u64> {
    use crate::format::index::IndexEntry;
    let meta = idx.metadata()?;
    let file_len = meta.len();
    let header = crate::format::index::IndexHeader::SIZE as u64;
    if file_len < header {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "index too small",
        ));
    }
    let mut pos = header;
    let mut last_end = 0u64;
    let mut buf = vec![0u8; IndexEntry::SIZE];
    while pos + IndexEntry::SIZE as u64 <= file_len {
        idx.seek(SeekFrom::Start(pos))?;
        idx.read_exact(&mut buf)?;
        match IndexEntry::decode_from(&buf[..]) {
            Ok(entry) => {
                last_end = entry.file_offset + entry.block_len as u64;
                pos += IndexEntry::SIZE as u64;
            }
            Err(_) => break, // hit a torn/invalid entry — stop at previous
        }
    }
    Ok(last_end.max(crate::format::headers::FileHeader::SIZE as u64))
}
