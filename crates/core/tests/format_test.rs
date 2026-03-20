use gqlite_core::storage::format::{
    page_checksum, verify_page_header, write_page_header, FileHeader, PageType, CHUNK_CAPACITY,
    FILE_HEADER_SIZE, FORMAT_VERSION, MAGIC, NODE_GROUP_SIZE, PAGE_HEADER_SIZE, PAGE_PAYLOAD_SIZE,
    PAGE_SIZE, VECTOR_CAPACITY,
};
use std::io::Cursor;

#[test]
fn header_roundtrip() {
    let header = FileHeader::new();
    let mut buf = Vec::new();
    header.write_to(&mut buf).unwrap();
    assert_eq!(buf.len(), FILE_HEADER_SIZE);

    let mut cursor = Cursor::new(&buf);
    let loaded = FileHeader::read_from(&mut cursor).unwrap();
    assert_eq!(loaded.magic, MAGIC);
    assert_eq!(loaded.version, FORMAT_VERSION); // now version 2
    assert_eq!(loaded.page_size, PAGE_SIZE);
    assert_eq!(loaded.page_count, header.page_count);
    assert_eq!(loaded.database_id, header.database_id);
    assert_eq!(loaded.catalog_page_idx, header.catalog_page_idx);
    assert_eq!(loaded.free_list_page_idx, header.free_list_page_idx);
}

#[test]
fn header_size_is_128() {
    assert_eq!(FILE_HEADER_SIZE, 128);
}

#[test]
fn validate_bad_magic() {
    let mut header = FileHeader::new();
    header.magic = [0, 0, 0, 0];
    assert!(header.validate().is_err());
}

#[test]
fn validate_bad_version() {
    let mut header = FileHeader::new();
    header.version = 999;
    assert!(header.validate().is_err());
}

#[test]
fn validate_bad_page_size() {
    let mut header = FileHeader::new();
    header.page_size = 1000; // not a power of 2
    assert!(header.validate().is_err());
}

#[test]
fn constants() {
    assert_eq!(NODE_GROUP_SIZE, 131072);
    assert_eq!(CHUNK_CAPACITY, 2048);
    assert_eq!(VECTOR_CAPACITY, 2048);
}

// ── Page header and checksum tests ──────────────────────────

#[test]
fn page_header_size_is_8() {
    assert_eq!(PAGE_HEADER_SIZE, 8);
}

#[test]
fn page_payload_size() {
    assert_eq!(PAGE_PAYLOAD_SIZE, PAGE_SIZE as usize - PAGE_HEADER_SIZE);
    assert_eq!(PAGE_PAYLOAD_SIZE, 4088);
}

#[test]
fn page_type_roundtrip() {
    assert_eq!(PageType::from_u8(0x00), PageType::Free);
    assert_eq!(PageType::from_u8(0x03), PageType::ColumnData);
    assert_eq!(PageType::from_u8(0x06), PageType::CsrHeader);
    assert_eq!(PageType::from_u8(0xFF), PageType::RawData);
    // Unknown type maps to RawData
    assert_eq!(PageType::from_u8(0xFE), PageType::RawData);
}

#[test]
fn write_and_verify_page_checksum() {
    let mut page = vec![0u8; PAGE_SIZE as usize];
    // Fill payload with some data
    for (i, byte) in page.iter_mut().enumerate().skip(PAGE_HEADER_SIZE) {
        *byte = (i % 256) as u8;
    }
    write_page_header(&mut page, PageType::ColumnData);

    // Verify type
    assert_eq!(page[0], PageType::ColumnData as u8);

    // Verify checksum passes
    let pt = verify_page_header(&page, 42).unwrap();
    assert_eq!(pt, PageType::ColumnData);

    // Compute checksum independently
    let expected = page_checksum(&page);
    let stored = u32::from_le_bytes(page[4..8].try_into().unwrap());
    assert_eq!(stored, expected);
}

#[test]
fn corrupted_page_detected() {
    let mut page = vec![0u8; PAGE_SIZE as usize];
    // Write valid header
    page[PAGE_HEADER_SIZE] = 0xAB; // some data
    write_page_header(&mut page, PageType::CatalogData);

    // Verify passes
    assert!(verify_page_header(&page, 1).is_ok());

    // Corrupt one byte in the payload
    page[PAGE_HEADER_SIZE + 10] ^= 0xFF;

    // Verify should fail
    let result = verify_page_header(&page, 1);
    assert!(result.is_err());
    let err = format!("{}", result.err().unwrap());
    assert!(err.contains("checksum mismatch"), "error: {}", err);
    assert!(err.contains("page 1"), "error: {}", err);
}

#[test]
fn zero_checksum_page_passes() {
    // A page with all zeros (including checksum=0) should pass verification.
    // This handles freshly allocated pages that haven't been written yet.
    let page = vec![0u8; PAGE_SIZE as usize];
    let pt = verify_page_header(&page, 0).unwrap();
    assert_eq!(pt, PageType::Free);
}
