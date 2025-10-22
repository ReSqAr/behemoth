use crate::codec::Codec;
use crate::format::headers::{BlockFooter, BlockHeader};
use crate::format::index::IndexEntry;
use crate::format::{CodecId, VERSION_1};
use crate::io::counting_writer::CountingWriter;
use crate::io::payload_sink::PayloadSink;
use crate::io::segment::SegmentFiles;
use crate::{Compression, StreamError};
use std::fs::File;
use std::io;
use std::io::BufWriter;

pub(crate) struct OpenBlock {
    header_offset: u64, // position of BlockHeader
    first_id: u64,
    records: u32,
    uncompressed_len: u32,
    sink: PayloadSink<CountingWriter<BufWriter<File>>>,
}

impl OpenBlock {
    pub(crate) fn records(&self) -> u32 {
        self.records
    }

    pub(crate) fn uncompressed_len(&self) -> u32 {
        self.uncompressed_len
    }

    pub(crate) fn begin(
        segment: &mut SegmentFiles,
        compression: Compression,
        write_buffer: usize,
        next_id: u64,
    ) -> io::Result<Self> {
        let codec = match compression {
            Compression::None => CodecId::None,
            Compression::Zstd { .. } => CodecId::Zstd,
        };

        let header_offset = segment.append_block_header(codec)?;
        let writer = segment.log.try_clone()?;
        let buf = BufWriter::with_capacity(write_buffer, writer);
        let sink = PayloadSink::new(CountingWriter::new(buf), compression)?;
        Ok(Self {
            header_offset,
            first_id: next_id,
            records: 0,
            uncompressed_len: 0,
            sink,
        })
    }

    pub(crate) fn write_record<C: Codec>(
        &mut self,
        codec: &C,
        value: &C::Value,
    ) -> Result<(), StreamError> {
        let mut tmp = Vec::with_capacity(256);
        codec
            .encode_into(value, &mut tmp)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
            .map_err(StreamError::Io)?;
        if tmp.len() > u32::MAX as usize {
            return Err(StreamError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "record too large",
            )));
        }

        let len = tmp.len() as u32;
        self.sink
            .write_all(&len.to_le_bytes())
            .map_err(StreamError::Io)?;
        self.sink.write_all(&tmp).map_err(StreamError::Io)?;
        self.records = self.records.saturating_add(1);
        self.uncompressed_len = self.uncompressed_len.saturating_add(4).saturating_add(len);
        Ok(())
    }

    pub(crate) fn finish(self, segment: &mut SegmentFiles) -> io::Result<Option<IndexEntry>> {
        if self.records == 0 {
            return Ok(None);
        }

        let writer = self.sink.finish()?;
        let compressed_len = writer.written() as u32;

        segment.append_block_footer()?;

        let block_len = (BlockHeader::SIZE as u32)
            .saturating_add(compressed_len)
            .saturating_add(BlockFooter::SIZE as u32);
        let last_id = self
            .first_id
            .saturating_add((self.records as u64).saturating_sub(1));

        let entry = IndexEntry {
            version: VERSION_1,
            first_id: self.first_id,
            last_id,
            file_offset: self.header_offset,
            block_len,
            records: self.records,
            uncompressed_len: self.uncompressed_len,
            compressed_len,
            flags: 0,
        };

        Ok(Some(entry))
    }
}
