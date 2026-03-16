//! LRU-based page buffer pool.
//!
//! Caches recently accessed pages in memory to reduce disk I/O.
//! Dirty pages are flushed back to disk when evicted or on explicit flush.

use std::collections::HashMap;

use crate::error::GqliteError;
use crate::storage::pager::{PageId, Pager};

/// Default number of page frames in the buffer pool.
const DEFAULT_POOL_SIZE: usize = 256;

/// A frame in the buffer pool holding one page's data.
struct Frame {
    page_id: PageId,
    data: Vec<u8>,
    dirty: bool,
    /// LRU counter — higher means more recently used.
    last_access: u64,
}

/// LRU page buffer pool wrapping a Pager.
pub struct BufferPool {
    pager: Pager,
    frames: HashMap<PageId, Frame>,
    max_frames: usize,
    access_counter: u64,
    page_size: usize,
}

impl BufferPool {
    /// Create a buffer pool wrapping an existing pager.
    pub fn new(pager: Pager) -> Self {
        Self::with_capacity(pager, DEFAULT_POOL_SIZE)
    }

    /// Create a buffer pool with a specific capacity (number of cached pages).
    pub fn with_capacity(pager: Pager, max_frames: usize) -> Self {
        let page_size = pager.page_size() as usize;
        Self {
            pager,
            frames: HashMap::with_capacity(max_frames),
            max_frames,
            access_counter: 0,
            page_size,
        }
    }

    /// Read a page. Returns cached data if available, otherwise loads from disk.
    pub fn read_page(&mut self, page_id: PageId) -> Result<&[u8], GqliteError> {
        self.access_counter += 1;
        let counter = self.access_counter;

        if self.frames.contains_key(&page_id) {
            // Cache hit — update LRU counter
            let frame = self.frames.get_mut(&page_id).unwrap();
            frame.last_access = counter;
            return Ok(&self.frames[&page_id].data);
        }

        // Cache miss — evict if full, then load from disk
        self.ensure_space()?;

        let mut data = vec![0u8; self.page_size];
        self.pager.read_page(page_id, &mut data)?;

        self.frames.insert(
            page_id,
            Frame {
                page_id,
                data,
                dirty: false,
                last_access: counter,
            },
        );

        Ok(&self.frames[&page_id].data)
    }

    /// Write a page. The data is buffered and marked dirty.
    pub fn write_page(&mut self, page_id: PageId, data: &[u8]) -> Result<(), GqliteError> {
        if data.len() != self.page_size {
            return Err(GqliteError::Storage(format!(
                "buffer size {} != page_size {}",
                data.len(),
                self.page_size
            )));
        }

        self.access_counter += 1;
        let counter = self.access_counter;

        if let Some(frame) = self.frames.get_mut(&page_id) {
            frame.data.copy_from_slice(data);
            frame.dirty = true;
            frame.last_access = counter;
        } else {
            self.ensure_space()?;
            self.frames.insert(
                page_id,
                Frame {
                    page_id,
                    data: data.to_vec(),
                    dirty: true,
                    last_access: counter,
                },
            );
        }

        Ok(())
    }

    /// Flush all dirty pages to disk.
    pub fn flush_all(&mut self) -> Result<(), GqliteError> {
        let dirty_ids: Vec<PageId> = self
            .frames
            .iter()
            .filter(|(_, f)| f.dirty)
            .map(|(id, _)| *id)
            .collect();

        for page_id in dirty_ids {
            let frame = self.frames.get_mut(&page_id).unwrap();
            self.pager.write_page(page_id, &frame.data)?;
            frame.dirty = false;
        }

        self.pager.sync()?;
        Ok(())
    }

    /// Flush a single page if it's dirty and cached.
    pub fn flush_page(&mut self, page_id: PageId) -> Result<(), GqliteError> {
        if let Some(frame) = self.frames.get_mut(&page_id) {
            if frame.dirty {
                self.pager.write_page(page_id, &frame.data)?;
                frame.dirty = false;
            }
        }
        Ok(())
    }

    /// Evict all pages from the cache, flushing dirty ones first.
    pub fn evict_all(&mut self) -> Result<(), GqliteError> {
        self.flush_all()?;
        self.frames.clear();
        Ok(())
    }

    /// Returns the number of pages currently cached.
    pub fn cached_count(&self) -> usize {
        self.frames.len()
    }

    /// Returns the number of dirty pages in the cache.
    pub fn dirty_count(&self) -> usize {
        self.frames.values().filter(|f| f.dirty).count()
    }

    /// Returns a reference to the underlying pager.
    pub fn pager(&self) -> &Pager {
        &self.pager
    }

    /// Returns a mutable reference to the underlying pager.
    pub fn pager_mut(&mut self) -> &mut Pager {
        &mut self.pager
    }

    /// Evict the LRU page if the pool is full.
    fn ensure_space(&mut self) -> Result<(), GqliteError> {
        if self.frames.len() < self.max_frames {
            return Ok(());
        }

        // Find the least recently used frame
        let lru_id = self
            .frames
            .iter()
            .min_by_key(|(_, f)| f.last_access)
            .map(|(id, _)| *id)
            .unwrap();

        // Flush if dirty
        let frame = self.frames.get(&lru_id).unwrap();
        if frame.dirty {
            self.pager.write_page(frame.page_id, &frame.data)?;
        }

        self.frames.remove(&lru_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::format::PAGE_SIZE;
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
}
