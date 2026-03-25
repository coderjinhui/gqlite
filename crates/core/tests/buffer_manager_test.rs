use gqlite_core::storage::buffer_manager::BufferPool;
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
fn cache_hit_avoids_disk() {
    let path = temp_path("test_bp_cache_hit.graph");
    cleanup(&path);

    let mut pager = Pager::create(&path).unwrap();
    let page_id = pager.allocate_page().unwrap();
    let mut data = vec![0u8; PAGE_SIZE as usize];
    data[0] = 0xAB;
    pager.write_page(page_id, &data).unwrap();

    let mut pool = BufferPool::with_capacity(pager, 4);

    // First read: cache miss, loads from disk
    let page = pool.read_page(page_id).unwrap();
    assert_eq!(page[0], 0xAB);
    assert_eq!(pool.cached_count(), 1);

    // Second read: cache hit
    let page = pool.read_page(page_id).unwrap();
    assert_eq!(page[0], 0xAB);
    assert_eq!(pool.cached_count(), 1); // still 1

    cleanup(&path);
}

#[test]
fn lru_eviction_when_full() {
    let path = temp_path("test_bp_eviction.graph");
    cleanup(&path);

    let mut pager = Pager::create(&path).unwrap();
    // Allocate 4 pages
    let mut page_ids = Vec::new();
    for i in 0..4u8 {
        let pid = pager.allocate_page().unwrap();
        let mut data = vec![0u8; PAGE_SIZE as usize];
        data[0] = i + 1;
        pager.write_page(pid, &data).unwrap();
        page_ids.push(pid);
    }

    // Pool capacity = 2
    let mut pool = BufferPool::with_capacity(pager, 2);

    // Load pages 0 and 1
    pool.read_page(page_ids[0]).unwrap();
    pool.read_page(page_ids[1]).unwrap();
    assert_eq!(pool.cached_count(), 2);

    // Loading page 2 should evict the LRU (page 0)
    pool.read_page(page_ids[2]).unwrap();
    assert_eq!(pool.cached_count(), 2);
    assert!(!pool.frames.contains_key(&page_ids[0]));
    assert!(pool.frames.contains_key(&page_ids[1]));
    assert!(pool.frames.contains_key(&page_ids[2]));

    cleanup(&path);
}

#[test]
fn dirty_pages_flushed_on_evict() {
    let path = temp_path("test_bp_dirty_flush.graph");
    cleanup(&path);

    let mut pager = Pager::create(&path).unwrap();
    let p1 = pager.allocate_page().unwrap();
    let p2 = pager.allocate_page().unwrap();
    let p3 = pager.allocate_page().unwrap();

    let mut pool = BufferPool::with_capacity(pager, 2);

    // Write dirty page
    let mut data = vec![0u8; PAGE_SIZE as usize];
    data[0] = 0xFF;
    pool.write_page(p1, &data).unwrap();
    assert_eq!(pool.dirty_count(), 1);

    // Fill cache
    pool.read_page(p2).unwrap();
    assert_eq!(pool.cached_count(), 2);

    // Evict p1 (LRU) by loading p3 — dirty p1 should be flushed to disk
    pool.read_page(p3).unwrap();
    assert_eq!(pool.cached_count(), 2);

    // Verify p1 was written to disk by reading directly from pager
    let mut buf = vec![0u8; PAGE_SIZE as usize];
    pool.pager().read_page(p1, &mut buf).unwrap();
    assert_eq!(buf[0], 0xFF);

    cleanup(&path);
}

#[test]
fn flush_all_writes_dirty() {
    let path = temp_path("test_bp_flush_all.graph");
    cleanup(&path);

    let mut pager = Pager::create(&path).unwrap();
    let p1 = pager.allocate_page().unwrap();

    let mut pool = BufferPool::with_capacity(pager, 4);

    let mut data = vec![0u8; PAGE_SIZE as usize];
    data[0] = 42;
    pool.write_page(p1, &data).unwrap();
    assert_eq!(pool.dirty_count(), 1);

    pool.flush_all().unwrap();
    assert_eq!(pool.dirty_count(), 0);

    // Verify on disk
    let mut buf = vec![0u8; PAGE_SIZE as usize];
    pool.pager().read_page(p1, &mut buf).unwrap();
    assert_eq!(buf[0], 42);

    cleanup(&path);
}

#[test]
fn evict_all_clears_cache() {
    let path = temp_path("test_bp_evict_all.graph");
    cleanup(&path);

    let mut pager = Pager::create(&path).unwrap();
    let p1 = pager.allocate_page().unwrap();

    let mut pool = BufferPool::with_capacity(pager, 4);
    pool.read_page(p1).unwrap();
    assert_eq!(pool.cached_count(), 1);

    pool.evict_all().unwrap();
    assert_eq!(pool.cached_count(), 0);

    cleanup(&path);
}

#[test]
fn buffer_pool_stats() {
    use std::sync::atomic::Ordering;

    let path = temp_path("bp_stats.graph");
    cleanup(&path);

    let pager = Pager::create(&path).unwrap();
    let mut pool = BufferPool::with_capacity(pager, 4);

    // Allocate and write pages
    let p1 = pool.allocate_page().unwrap();
    let p2 = pool.allocate_page().unwrap();
    let data = vec![0xABu8; PAGE_SIZE as usize];
    pool.write_page(p1, &data).unwrap();
    pool.write_page(p2, &data).unwrap();

    // First read = miss (p1 is already in cache from write, so re-read another)
    pool.evict_all().unwrap();
    let _ = pool.read_page(p1).unwrap(); // miss
    assert!(pool.stats.misses.load(Ordering::Relaxed) >= 1);

    let _ = pool.read_page(p1).unwrap(); // hit
    assert!(pool.stats.hits.load(Ordering::Relaxed) >= 1);

    // Hit rate should be > 0
    assert!(pool.stats.hit_rate() > 0.0);

    cleanup(&path);
}

#[test]
fn buffer_pool_eviction_stats() {
    use std::sync::atomic::Ordering;

    let path = temp_path("bp_evict_stats.graph");
    cleanup(&path);

    let pager = Pager::create(&path).unwrap();
    let mut pool = BufferPool::with_capacity(pager, 2);

    let p1 = pool.allocate_page().unwrap();
    let p2 = pool.allocate_page().unwrap();
    let p3 = pool.allocate_page().unwrap();
    let data = vec![0u8; PAGE_SIZE as usize];
    pool.write_page(p1, &data).unwrap();
    pool.write_page(p2, &data).unwrap();
    pool.write_page(p3, &data).unwrap();

    assert!(pool.stats.evictions.load(Ordering::Relaxed) >= 1);
    assert_eq!(pool.cached_count(), 2);

    cleanup(&path);
}
