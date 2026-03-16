use gqlite_core::storage::format::PAGE_SIZE;
use gqlite_core::storage::pager::Pager;
use std::fs;
use std::path::Path;

fn temp_path(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join("gqlite_test");
    fs::create_dir_all(&dir).ok();
    dir.join(name)
}

fn cleanup(path: &Path) {
    fs::remove_file(path).ok();
}

#[test]
fn create_and_open() {
    let path = temp_path("test_create_open.graph");
    cleanup(&path);

    // Create
    let pager = Pager::create(&path).unwrap();
    assert_eq!(pager.page_count(), 1); // just the header page
    drop(pager);

    // Open
    let pager = Pager::open(&path).unwrap();
    assert_eq!(pager.page_count(), 1);
    assert_eq!(pager.page_size(), PAGE_SIZE);
    drop(pager);

    cleanup(&path);
}

#[test]
fn write_and_read_page() {
    let path = temp_path("test_write_read.graph");
    cleanup(&path);

    let mut pager = Pager::create(&path).unwrap();

    // Allocate a data page
    let page_id = pager.allocate_page().unwrap();
    assert_eq!(page_id, 1);
    assert_eq!(pager.page_count(), 2);

    // Write data to it
    let mut data = vec![0u8; PAGE_SIZE as usize];
    data[0] = 0xDE;
    data[1] = 0xAD;
    data[PAGE_SIZE as usize - 1] = 0xFF;
    pager.write_page(page_id, &data).unwrap();

    // Read it back
    let mut buf = vec![0u8; PAGE_SIZE as usize];
    pager.read_page(page_id, &mut buf).unwrap();
    assert_eq!(buf[0], 0xDE);
    assert_eq!(buf[1], 0xAD);
    assert_eq!(buf[PAGE_SIZE as usize - 1], 0xFF);

    drop(pager);
    cleanup(&path);
}

#[test]
fn allocate_multiple_pages() {
    let path = temp_path("test_alloc_multi.graph");
    cleanup(&path);

    let mut pager = Pager::create(&path).unwrap();
    let p1 = pager.allocate_page().unwrap();
    let p2 = pager.allocate_page().unwrap();
    let p3 = pager.allocate_page().unwrap();
    assert_eq!(p1, 1);
    assert_eq!(p2, 2);
    assert_eq!(p3, 3);
    assert_eq!(pager.page_count(), 4);

    drop(pager);
    cleanup(&path);
}

#[test]
fn persistence_across_reopen() {
    let path = temp_path("test_persist.graph");
    cleanup(&path);

    // Create and write
    {
        let mut pager = Pager::create(&path).unwrap();
        let page_id = pager.allocate_page().unwrap();
        let mut data = vec![0u8; PAGE_SIZE as usize];
        data[0] = 42;
        pager.write_page(page_id, &data).unwrap();
        pager.sync().unwrap();
    }

    // Reopen and read
    {
        let pager = Pager::open(&path).unwrap();
        assert_eq!(pager.page_count(), 2);
        let mut buf = vec![0u8; PAGE_SIZE as usize];
        pager.read_page(1, &mut buf).unwrap();
        assert_eq!(buf[0], 42);
    }

    cleanup(&path);
}

#[test]
fn read_out_of_range() {
    let path = temp_path("test_oor.graph");
    cleanup(&path);

    let pager = Pager::create(&path).unwrap();
    let mut buf = vec![0u8; PAGE_SIZE as usize];
    let result = pager.read_page(999, &mut buf);
    assert!(result.is_err());

    drop(pager);
    cleanup(&path);
}
