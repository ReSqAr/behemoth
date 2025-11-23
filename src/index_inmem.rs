use crate::Offset;
use crate::format::index::{IndexEntry, IndexHeader};
use crate::io::segment::{list_segments, segment_filename};
use boxcar::Vec as BoxcarVec;
use std::io::BufReader;
use std::{
    fs::File,
    io::{self, Read},
    path::Path,
};

#[derive(Clone, Copy, Debug)]
pub struct IndexRef {
    pub segment_id: u64,
    pub first_id: Offset,
    pub last_id: Offset,
    pub file_offset: u64,
    pub block_len: u32,
    pub uncompressed_len: u32,
    pub records: u32,
}

#[derive(Clone)]
pub struct InMemIndex {
    pub entries: BoxcarVec<IndexRef>, // sorted by first_id
}

impl InMemIndex {
    pub fn load_all(dir: &Path, bufread: usize) -> io::Result<Self> {
        let out = BoxcarVec::new();
        for seg_id in list_segments(dir)? {
            let (_, idx_name) = segment_filename(seg_id);
            let path = dir.join(idx_name);
            let f = File::open(&path)?;
            let file_len = f.metadata()?.len();
            let mut w = BufReader::with_capacity(bufread, f);

            // Skip header
            let mut hdr_buf = vec![0u8; IndexHeader::SIZE];
            w.read_exact(&mut hdr_buf)?;
            let _ = IndexHeader::decode_from(&hdr_buf[..])?;

            // Read entries
            let mut pos = IndexHeader::SIZE as u64;
            let mut buf = vec![0u8; IndexEntry::SIZE];
            while pos + IndexEntry::SIZE as u64 <= file_len {
                w.read_exact(&mut buf)?;
                let e = IndexEntry::decode_from(&buf[..])?;
                out.push(IndexRef {
                    segment_id: seg_id,
                    first_id: Offset(e.first_id),
                    last_id: Offset(e.last_id),
                    file_offset: e.file_offset,
                    block_len: e.block_len,
                    uncompressed_len: e.uncompressed_len,
                    records: e.records,
                });
                pos += IndexEntry::SIZE as u64;
            }
        }
        Ok(Self { entries: out })
    }

    pub fn len(&self) -> usize {
        self.entries.count()
    }

    pub fn push_entry(&self, segment_id: u64, entry: &IndexEntry) {
        self.entries.push(IndexRef {
            segment_id,
            first_id: Offset(entry.first_id),
            last_id: Offset(entry.last_id),
            file_offset: entry.file_offset,
            block_len: entry.block_len,
            uncompressed_len: entry.uncompressed_len,
            records: entry.records,
        });
    }

    /// Lower bound: first index whose last_id >= target.
    pub fn lower_bound(&self, target: Offset) -> Option<usize> {
        let mut lo = 0usize;
        let mut hi = self.entries.count();
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.entries[mid].last_id < target {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo < self.entries.count() {
            Some(lo)
        } else {
            None
        }
    }

    /// Peek last committed id across all segments (watermark).
    pub fn watermark(&self) -> Option<Offset> {
        if self.entries.is_empty() {
            None
        } else {
            self.entries
                .get(self.entries.count() - 1)
                .copied()
                .map(|e| e.last_id)
        }
    }
}
