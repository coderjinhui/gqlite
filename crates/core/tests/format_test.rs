use gqlite_core::storage::format::{
    FileHeader, FILE_HEADER_SIZE, FORMAT_VERSION, MAGIC, PAGE_SIZE,
    NODE_GROUP_SIZE, CHUNK_CAPACITY, VECTOR_CAPACITY,
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
    assert_eq!(loaded.version, FORMAT_VERSION);
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
