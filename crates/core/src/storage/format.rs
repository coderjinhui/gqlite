use std::io::{Read, Write};

use crate::error::GqliteError;

/// Magic bytes at the start of every `.graph` file: "GQLT" in ASCII.
pub const MAGIC: [u8; 4] = [0x47, 0x51, 0x4C, 0x54];

/// Current file format version.
pub const FORMAT_VERSION: u32 = 1;

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
        if self.version != FORMAT_VERSION {
            return Err(GqliteError::Storage(format!(
                "unsupported format version: {}",
                self.version
            )));
        }
        if self.page_size == 0 || (self.page_size & (self.page_size - 1)) != 0 {
            return Err(GqliteError::Storage("page_size must be a power of 2".into()));
        }
        Ok(())
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
