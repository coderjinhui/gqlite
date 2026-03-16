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
