use std::io::{Read, Write};

use crate::error::GqliteError;

/// Magic bytes at the start of every `.graph` file: "GQLT" in ASCII.
pub const MAGIC: [u8; 4] = [0x47, 0x51, 0x4C, 0x54];

/// Current file format version.
pub const FORMAT_VERSION: u32 = 2;

/// Minimum supported format version for reading.
pub const MIN_FORMAT_VERSION: u32 = 1;

/// Default page size in bytes (4 KiB).
pub const PAGE_SIZE: u32 = 4096;

/// Number of rows per NodeGroup (2^17 = 131072).
pub const NODE_GROUP_SIZE: usize = 1 << 17;

/// Number of rows per chunk within a NodeGroup.
pub const CHUNK_CAPACITY: usize = 2048;

/// Capacity of a single value vector (matches chunk capacity).
pub const VECTOR_CAPACITY: usize = 2048;

/// Total serialized size of the FileHeader in bytes.
pub const FILE_HEADER_SIZE: usize = 128;

/// Page index that means "not allocated" / "invalid".
pub const INVALID_PAGE_IDX: u64 = u64::MAX;

/// Size of the per-page header (type + reserved + checksum).
pub const PAGE_HEADER_SIZE: usize = 8;

/// Usable payload bytes per page (PAGE_SIZE - PAGE_HEADER_SIZE).
pub const PAGE_PAYLOAD_SIZE: usize = PAGE_SIZE as usize - PAGE_HEADER_SIZE;

// ── Page Type ──────────────────────────────────────────────────

/// Type identifier for each page in the v2 format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    /// Free / unallocated page.
    Free = 0x00,
    /// Catalog root page.
    CatalogRoot = 0x01,
    /// Catalog data page (table definitions).
    CatalogData = 0x02,
    /// ColumnChunk fixed-length data.
    ColumnData = 0x03,
    /// String overflow data.
    StringData = 0x04,
    /// NULL bitmap data.
    NullBitmap = 0x05,
    /// CSR header (offsets + lengths).
    CsrHeader = 0x06,
    /// CSR neighbor IDs / rel IDs.
    CsrNeighbors = 0x07,
    /// Free list root page.
    FreeListRoot = 0x08,
    /// Free list data page.
    FreeListData = 0x09,
    /// Per-table metadata page.
    TableMeta = 0x0A,
    /// Relationship property data.
    RelProperties = 0x0B,
    /// MVCC version metadata.
    MvccMeta = 0x0C,
    /// Raw data page (v1 compat — no structured header).
    RawData = 0xFF,
}

impl PageType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0x00 => Self::Free,
            0x01 => Self::CatalogRoot,
            0x02 => Self::CatalogData,
            0x03 => Self::ColumnData,
            0x04 => Self::StringData,
            0x05 => Self::NullBitmap,
            0x06 => Self::CsrHeader,
            0x07 => Self::CsrNeighbors,
            0x08 => Self::FreeListRoot,
            0x09 => Self::FreeListData,
            0x0A => Self::TableMeta,
            0x0B => Self::RelProperties,
            0x0C => Self::MvccMeta,
            _ => Self::RawData,
        }
    }
}

// ── Page Checksum ──────────────────────────────────────────────

/// Compute CRC32 checksum for a page's payload (bytes after the 8-byte header).
pub fn page_checksum(page_buf: &[u8]) -> u32 {
    debug_assert!(page_buf.len() >= PAGE_HEADER_SIZE);
    crc32fast::hash(&page_buf[PAGE_HEADER_SIZE..])
}

/// Write a page header (type + checksum) into the first 8 bytes of a page buffer.
///
/// Layout: `[0] page_type (u8) | [1..4] reserved (3 bytes) | [4..8] checksum (u32 LE)`
pub fn write_page_header(page_buf: &mut [u8], page_type: PageType) {
    debug_assert!(page_buf.len() >= PAGE_HEADER_SIZE);
    page_buf[0] = page_type as u8;
    page_buf[1] = 0;
    page_buf[2] = 0;
    page_buf[3] = 0;
    let checksum = crc32fast::hash(&page_buf[PAGE_HEADER_SIZE..]);
    page_buf[4..8].copy_from_slice(&checksum.to_le_bytes());
}

/// Read and verify a page header. Returns the page type.
///
/// Returns an error if the checksum does not match.
pub fn verify_page_header(page_buf: &[u8], page_id: u64) -> Result<PageType, GqliteError> {
    if page_buf.len() < PAGE_HEADER_SIZE {
        return Err(GqliteError::Storage(format!(
            "page {} too small: {} bytes",
            page_id,
            page_buf.len()
        )));
    }
    let page_type = PageType::from_u8(page_buf[0]);
    let stored_checksum = u32::from_le_bytes(page_buf[4..8].try_into().unwrap());
    let computed_checksum = crc32fast::hash(&page_buf[PAGE_HEADER_SIZE..]);

    if stored_checksum != computed_checksum && stored_checksum != 0 {
        return Err(GqliteError::Storage(format!(
            "page {} checksum mismatch: stored={:#010x}, computed={:#010x}",
            page_id, stored_checksum, computed_checksum
        )));
    }
    Ok(page_type)
}

/// File header stored at offset 0 of a `.graph` file.
///
/// Fixed at 128 bytes. Layout:
/// ```text
/// [0..4]     magic              "GQLT"
/// [4..8]     version            u32 LE
/// [8..12]    page_size          u32 LE
/// [12..20]   page_count         u64 LE
/// [20..36]   database_id        [u8; 16] UUID
/// [36..44]   catalog_page       u64 LE
/// [44..52]   free_list_page     u64 LE
/// [52..60]   storage_page_idx   u64 LE
/// [60..68]   checkpoint_ts      u64 LE
/// [68..128]  _reserved          zeroed
/// ```
#[derive(Debug, Clone)]
pub struct FileHeader {
    pub magic: [u8; 4],
    pub version: u32,
    pub page_size: u32,
    pub page_count: u64,
    pub database_id: [u8; 16],
    pub catalog_page_idx: u64,
    pub free_list_page_idx: u64,
    pub storage_page_idx: u64,
    pub checkpoint_ts: u64,
}

impl FileHeader {
    pub fn new() -> Self {
        let db_id = uuid::Uuid::new_v4();
        Self {
            magic: MAGIC,
            version: FORMAT_VERSION,
            page_size: PAGE_SIZE,
            page_count: 1, // page 0 is the header page itself
            database_id: *db_id.as_bytes(),
            catalog_page_idx: INVALID_PAGE_IDX,
            free_list_page_idx: INVALID_PAGE_IDX,
            storage_page_idx: INVALID_PAGE_IDX,
            checkpoint_ts: 0,
        }
    }

    /// Validate header magic and version.
    pub fn validate(&self) -> Result<(), GqliteError> {
        if self.magic != MAGIC {
            return Err(GqliteError::Storage("invalid magic bytes".into()));
        }
        if self.version < MIN_FORMAT_VERSION || self.version > FORMAT_VERSION {
            return Err(GqliteError::Storage(format!(
                "unsupported format version: {} (supported: {}-{})",
                self.version, MIN_FORMAT_VERSION, FORMAT_VERSION
            )));
        }
        if self.page_size == 0 || (self.page_size & (self.page_size - 1)) != 0 {
            return Err(GqliteError::Storage("page_size must be a power of 2".into()));
        }
        Ok(())
    }

    /// Whether this file needs upgrading to the current format version.
    pub fn needs_upgrade(&self) -> bool {
        self.version < FORMAT_VERSION
    }

    /// Deserialize a FileHeader from a byte stream (little-endian).
    pub fn read_from(reader: &mut impl Read) -> Result<Self, GqliteError> {
        let mut buf = [0u8; FILE_HEADER_SIZE];
        reader.read_exact(&mut buf)?;

        let mut magic = [0u8; 4];
        magic.copy_from_slice(&buf[0..4]);

        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        let page_size = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let page_count = u64::from_le_bytes(buf[12..20].try_into().unwrap());

        let mut database_id = [0u8; 16];
        database_id.copy_from_slice(&buf[20..36]);

        let catalog_page_idx = u64::from_le_bytes(buf[36..44].try_into().unwrap());
        let free_list_page_idx = u64::from_le_bytes(buf[44..52].try_into().unwrap());
        let storage_page_idx = u64::from_le_bytes(buf[52..60].try_into().unwrap());
        let checkpoint_ts = u64::from_le_bytes(buf[60..68].try_into().unwrap());

        let header = Self {
            magic,
            version,
            page_size,
            page_count,
            database_id,
            catalog_page_idx,
            free_list_page_idx,
            storage_page_idx,
            checkpoint_ts,
        };
        header.validate()?;
        Ok(header)
    }

    /// Serialize this FileHeader to a byte stream (little-endian).
    pub fn write_to(&self, writer: &mut impl Write) -> Result<(), GqliteError> {
        let mut buf = [0u8; FILE_HEADER_SIZE];

        buf[0..4].copy_from_slice(&self.magic);
        buf[4..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..12].copy_from_slice(&self.page_size.to_le_bytes());
        buf[12..20].copy_from_slice(&self.page_count.to_le_bytes());
        buf[20..36].copy_from_slice(&self.database_id);
        buf[36..44].copy_from_slice(&self.catalog_page_idx.to_le_bytes());
        buf[44..52].copy_from_slice(&self.free_list_page_idx.to_le_bytes());
        buf[52..60].copy_from_slice(&self.storage_page_idx.to_le_bytes());
        buf[60..68].copy_from_slice(&self.checkpoint_ts.to_le_bytes());
        // [68..128] remains zeroed (_reserved)

        writer.write_all(&buf)?;
        Ok(())
    }
}

impl Default for FileHeader {
    fn default() -> Self {
        Self::new()
    }
}
