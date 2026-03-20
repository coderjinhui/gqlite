//! LRU-based page buffer pool.
//!
//! Caches recently accessed pages in memory to reduce disk I/O.
//! Dirty pages are flushed back to disk when evicted or on explicit flush.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::GqliteError;
use crate::storage::pager::{PageId, Pager};

/// Default number of page frames in the buffer pool.
const DEFAULT_POOL_SIZE: usize = 256;

/// Statistics for the buffer pool.
#[derive(Debug, Default)]
pub struct BufferPoolStats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub evictions: AtomicU64,
    pub flushes: AtomicU64,
}

impl BufferPoolStats {
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}

/// A frame in the buffer pool holding one page's data.
pub struct Frame {
    page_id: PageId,
    data: Vec<u8>,
    dirty: bool,
    /// LRU counter — higher means more recently used.
    last_access: u64,
}

/// LRU page buffer pool wrapping a Pager.
pub struct BufferPool {
    pager: Pager,
    pub frames: HashMap<PageId, Frame>,
    max_frames: usize,
    access_counter: u64,
    page_size: usize,
    pub stats: BufferPoolStats,
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
            stats: BufferPoolStats::default(),
        }
    }

    /// Read a page. Returns cached data if available, otherwise loads from disk.
    pub fn read_page(&mut self, page_id: PageId) -> Result<&[u8], GqliteError> {
        self.access_counter += 1;
        let counter = self.access_counter;

        if self.frames.contains_key(&page_id) {
            // Cache hit — update LRU counter
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            let frame = self.frames.get_mut(&page_id).unwrap();
            frame.last_access = counter;
            return Ok(&self.frames[&page_id].data);
        }

        // Cache miss — evict if full, then load from disk
        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        self.ensure_space()?;

        let mut data = vec![0u8; self.page_size];
        self.pager.read_page(page_id, &mut data)?;

        self.frames.insert(page_id, Frame { page_id, data, dirty: false, last_access: counter });

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
                Frame { page_id, data: data.to_vec(), dirty: true, last_access: counter },
            );
        }

        Ok(())
    }

    /// Flush all dirty pages to disk.
    pub fn flush_all(&mut self) -> Result<(), GqliteError> {
        let dirty_ids: Vec<PageId> =
            self.frames.iter().filter(|(_, f)| f.dirty).map(|(id, _)| *id).collect();

        for page_id in &dirty_ids {
            let frame = self.frames.get_mut(page_id).unwrap();
            self.pager.write_page(*page_id, &frame.data)?;
            frame.dirty = false;
        }
        self.stats.flushes.fetch_add(dirty_ids.len() as u64, Ordering::Relaxed);

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
        let lru_id =
            self.frames.iter().min_by_key(|(_, f)| f.last_access).map(|(id, _)| *id).unwrap();

        // Flush if dirty
        let frame = self.frames.get(&lru_id).unwrap();
        if frame.dirty {
            self.pager.write_page(frame.page_id, &frame.data)?;
            self.stats.flushes.fetch_add(1, Ordering::Relaxed);
        }

        self.frames.remove(&lru_id);
        self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    // ── Pager delegation ────────────────────────────────────────

    /// Allocate a new page (delegates to underlying pager).
    pub fn allocate_page(&mut self) -> Result<PageId, GqliteError> {
        self.pager.allocate_page()
    }

    /// Returns the page size in bytes.
    pub fn page_size(&self) -> u32 {
        self.pager.page_size()
    }

    /// Returns the number of pages.
    pub fn page_count(&self) -> u64 {
        self.pager.page_count()
    }

    /// Returns a reference to the file header.
    pub fn header(&self) -> &crate::storage::format::FileHeader {
        self.pager.header()
    }

    /// Returns a mutable reference to the file header.
    pub fn header_mut(&mut self) -> &mut crate::storage::format::FileHeader {
        self.pager.header_mut()
    }

    /// Write the in-memory header back to page 0.
    pub fn flush_header(&self) -> Result<(), GqliteError> {
        self.pager.flush_header()
    }

    /// Flush all OS-level buffers to disk.
    pub fn sync(&self) -> Result<(), GqliteError> {
        self.pager.sync()
    }
}
