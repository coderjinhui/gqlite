use std::fs::{File, OpenOptions};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::GqliteError;

use super::format::{FileHeader, FILE_HEADER_SIZE};

/// A page identifier (0-based).
pub type PageId = u64;

/// Manages fixed-size pages within a `.graph` file.
///
/// Page 0 is reserved for the FileHeader. User data pages start from page 1.
pub struct Pager {
    file: File,
    header: FileHeader,
}

impl Pager {
    /// Create a new `.graph` file at `path`, writing the initial header.
    pub fn create(path: &Path) -> Result<Self, GqliteError> {
        let mut file = OpenOptions::new().read(true).write(true).create_new(true).open(path)?;

        let header = FileHeader::new();

        // Write header into a full page-sized buffer
        let mut page_buf = vec![0u8; header.page_size as usize];
        let mut cursor = Cursor::new(&mut page_buf[..]);
        header.write_to(&mut cursor)?;

        file.write_all(&page_buf)?;
        file.sync_all()?;

        Ok(Self { file, header })
    }

    /// Open an existing `.graph` file and validate its header.
    pub fn open(path: &Path) -> Result<Self, GqliteError> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;

        // Read and validate the header
        let mut header_buf = [0u8; FILE_HEADER_SIZE];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut header_buf)?;

        let mut cursor = Cursor::new(&header_buf[..]);
        let header = FileHeader::read_from(&mut cursor)?;

        Ok(Self { file, header })
    }

    /// Read a full page into `buf`. The buffer must be exactly `page_size` bytes.
    pub fn read_page(&self, page_id: PageId, buf: &mut [u8]) -> Result<(), GqliteError> {
        let page_size = self.header.page_size as usize;
        if buf.len() != page_size {
            return Err(GqliteError::Storage(format!(
                "buffer size {} != page_size {}",
                buf.len(),
                page_size
            )));
        }
        if page_id >= self.header.page_count {
            return Err(GqliteError::Storage(format!(
                "page_id {} out of range (page_count={})",
                page_id, self.header.page_count
            )));
        }
        let offset = page_id * page_size as u64;
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.file.read_exact_at(buf, offset)?;
        }
        #[cfg(not(unix))]
        {
            use std::io::{Read, Seek, SeekFrom};
            let file = &self.file;
            // Fallback: seek + read (not thread-safe without external lock)
            (&*file).seek(SeekFrom::Start(offset))?;
            (&*file).read_exact(buf)?;
        }
        Ok(())
    }

    /// Write a full page from `data`. The slice must be exactly `page_size` bytes.
    pub fn write_page(&self, page_id: PageId, data: &[u8]) -> Result<(), GqliteError> {
        let page_size = self.header.page_size as usize;
        if data.len() != page_size {
            return Err(GqliteError::Storage(format!(
                "data size {} != page_size {}",
                data.len(),
                page_size
            )));
        }
        if page_id >= self.header.page_count {
            return Err(GqliteError::Storage(format!(
                "page_id {} out of range (page_count={})",
                page_id, self.header.page_count
            )));
        }
        let offset = page_id * page_size as u64;
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.file.write_all_at(data, offset)?;
        }
        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom, Write};
            let file = &self.file;
            (&*file).seek(SeekFrom::Start(offset))?;
            (&*file).write_all(data)?;
        }
        Ok(())
    }

    /// Allocate a new page, extending the file. Returns the new page ID.
    pub fn allocate_page(&mut self) -> Result<PageId, GqliteError> {
        let new_page_id = self.header.page_count;
        self.header.page_count += 1;

        // Extend the file by one page (write zeros)
        let page_size = self.header.page_size as usize;
        let offset = new_page_id * page_size as u64;
        let zeros = vec![0u8; page_size];

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.file.write_all_at(&zeros, offset)?;
        }
        #[cfg(not(unix))]
        {
            self.file.seek(SeekFrom::Start(offset))?;
            self.file.write_all(&zeros)?;
        }

        // Persist updated header
        self.flush_header()?;

        Ok(new_page_id)
    }

    /// Returns the current number of pages (including the header page).
    pub fn page_count(&self) -> u64 {
        self.header.page_count
    }

    /// Returns the page size in bytes.
    pub fn page_size(&self) -> u32 {
        self.header.page_size
    }

    /// Flush all OS-level buffers to disk.
    pub fn sync(&self) -> Result<(), GqliteError> {
        self.file.sync_all()?;
        Ok(())
    }

    /// Returns a reference to the file header.
    pub fn header(&self) -> &FileHeader {
        &self.header
    }

    /// Returns a mutable reference to the file header.
    pub fn header_mut(&mut self) -> &mut FileHeader {
        &mut self.header
    }

    /// Write the in-memory header back to page 0.
    pub fn flush_header(&self) -> Result<(), GqliteError> {
        let mut page_buf = vec![0u8; self.header.page_size as usize];
        let mut cursor = Cursor::new(&mut page_buf[..]);
        self.header.write_to(&mut cursor)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.file.write_all_at(&page_buf, 0)?;
        }
        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom, Write};
            let file = &self.file;
            (&*file).seek(SeekFrom::Start(0))?;
            (&*file).write_all(&page_buf)?;
        }
        Ok(())
    }
}
