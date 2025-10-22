#[cfg(test)]
mod tests {
    use super::super::headers::*;
    use super::super::index::*;
    use crate::format::{CodecId, VERSION_1};
    use std::io::Cursor;

    #[test]
    fn file_header_roundtrip() {
        let hdr = FileHeader::default();
        let mut buf = Vec::new();
        hdr.encode_to(&mut buf).unwrap();
        assert_eq!(buf.len(), FileHeader::SIZE);
        let got = FileHeader::decode_from(Cursor::new(&buf)).unwrap();
        assert_eq!(hdr, got);
    }

    #[test]
    fn block_header_roundtrip() {
        let hdr = BlockHeader::new(CodecId::Zstd);
        let mut buf = Vec::new();
        hdr.encode_to(&mut buf).unwrap();
        assert_eq!(buf.len(), BlockHeader::SIZE);
        let got = BlockHeader::decode_from(Cursor::new(&buf)).unwrap();
        assert_eq!(hdr, got);
    }

    #[test]
    fn block_footer_roundtrip() {
        let ftr = BlockFooter::default();
        let mut buf = Vec::new();
        ftr.encode_to(&mut buf).unwrap();
        assert_eq!(buf.len(), BlockFooter::SIZE);
        let got = BlockFooter::decode_from(Cursor::new(&buf)).unwrap();
        assert_eq!(ftr, got);
    }

    #[test]
    fn index_header_roundtrip() {
        let hdr = IndexHeader::default();
        let mut buf = Vec::new();
        hdr.encode_to(&mut buf).unwrap();
        assert_eq!(buf.len(), IndexHeader::SIZE);
        let got = IndexHeader::decode_from(Cursor::new(&buf)).unwrap();
        assert_eq!(hdr, got);
    }

    #[test]
    fn index_entry_roundtrip_and_crc() {
        let e = IndexEntry {
            version: VERSION_1,
            first_id: 10,
            last_id: 19,
            file_offset: 4096,
            block_len: 12345,
            records: 10,
            uncompressed_len: 20000,
            compressed_len: 9000,
            flags: 0,
        };
        let mut buf = Vec::new();
        e.encode_to(&mut buf).unwrap();
        assert_eq!(buf.len(), IndexEntry::SIZE);

        // Verify CRC mismatch detection
        let mut bad = buf.clone();
        bad[0] ^= 0xFF; // flip a bit in first_id
        assert!(IndexEntry::decode_from(std::io::Cursor::new(&bad)).is_err());

        // Roundtrip success
        let got = IndexEntry::decode_from(std::io::Cursor::new(&buf)).unwrap();
        assert_eq!(got, e);
    }
}
