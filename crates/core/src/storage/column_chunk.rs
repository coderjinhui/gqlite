use bitvec::prelude::*;
use chrono::Datelike;
use serde::{Deserialize, Serialize};

use crate::error::GqliteError;
use crate::storage::compression;
use crate::storage::format::CHUNK_CAPACITY;
use crate::storage::pager::{PageId, Pager};
use crate::types::data_type::DataType;
use crate::types::graph::InternalId;
use crate::types::value::Value;

/// Metadata for persisting/loading a ColumnChunk from disk.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ColumnChunkMetadata {
    /// Pages storing the data buffer.
    pub data_pages: Vec<PageId>,
    /// Pages storing strings (for String columns).
    pub string_pages: Vec<PageId>,
    /// Pages storing the null bitmap.
    pub null_bitmap_pages: Vec<PageId>,
    /// Number of values stored.
    pub num_values: u64,
    /// Whether the data pages use FOR+bitpacking compression (Int64 only).
    pub compressed: bool,
    /// Byte length of the compressed data (to read back exactly the right amount).
    pub compressed_len: u64,
}

/// A column-oriented storage chunk for a single column of fixed capacity.
///
/// Stores values in a raw byte buffer (for fixed-size types) or a Vec<String>
/// (for variable-length strings). A null bitmap tracks which positions are NULL.
#[derive(Serialize, Deserialize)]
pub struct ColumnChunk {
    pub data_type: DataType,
    /// Raw bytes for fixed-size types (Bool/Int64/Double/InternalId).
    buffer: Vec<u8>,
    /// String storage (only used when data_type == String).
    strings: Option<Vec<Option<String>>>,
    /// Per-value null bitmap. `true` means NULL.
    null_mask: BitVec<u8, Lsb0>,
    num_values: u64,
    capacity: u64,
}

impl ColumnChunk {
    pub fn new(data_type: DataType, capacity: u64) -> Self {
        let byte_size = data_type.byte_size().unwrap_or(0);
        let buf_len = byte_size * capacity as usize;
        let strings = if data_type == DataType::String {
            Some(Vec::with_capacity(capacity as usize))
        } else {
            None
        };

        Self {
            data_type,
            buffer: vec![0u8; buf_len],
            strings,
            null_mask: bitvec![u8, Lsb0; 0; capacity as usize],
            num_values: 0,
            capacity,
        }
    }

    /// Create a ColumnChunk with the default chunk capacity (2048).
    pub fn with_default_capacity(data_type: DataType) -> Self {
        Self::new(data_type, CHUNK_CAPACITY as u64)
    }

    /// Returns `true` if the value at `idx` is NULL.
    pub fn is_null(&self, idx: usize) -> bool {
        idx < self.null_mask.len() && self.null_mask[idx]
    }

    /// Set the null flag for position `idx`.
    fn set_null(&mut self, idx: usize, is_null: bool) {
        if idx < self.null_mask.len() {
            self.null_mask.set(idx, is_null);
        }
    }

    /// Read a value at the given index.
    pub fn get_value(&self, idx: usize) -> Value {
        if idx >= self.num_values as usize {
            return Value::Null;
        }
        if self.is_null(idx) {
            return Value::Null;
        }

        match self.data_type {
            DataType::Bool => {
                let v = self.buffer[idx];
                Value::Bool(v != 0)
            }
            DataType::Int64 | DataType::Serial => {
                let offset = idx * 8;
                let bytes: [u8; 8] = self.buffer[offset..offset + 8].try_into().unwrap();
                Value::Int(i64::from_le_bytes(bytes))
            }
            DataType::Double => {
                let offset = idx * 8;
                let bytes: [u8; 8] = self.buffer[offset..offset + 8].try_into().unwrap();
                Value::Float(f64::from_le_bytes(bytes))
            }
            DataType::InternalId => {
                let offset = idx * 12;
                let table_id = u32::from_le_bytes(
                    self.buffer[offset..offset + 4].try_into().unwrap(),
                );
                let row_offset = u64::from_le_bytes(
                    self.buffer[offset + 4..offset + 12].try_into().unwrap(),
                );
                Value::InternalId(InternalId::new(table_id, row_offset))
            }
            DataType::String => {
                if let Some(ref strings) = self.strings {
                    match strings.get(idx) {
                        Some(Some(s)) => Value::String(s.clone()),
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            DataType::Date => {
                let offset = idx * 4;
                let bytes: [u8; 4] = self.buffer[offset..offset + 4].try_into().unwrap();
                let days = i32::from_le_bytes(bytes);
                match chrono::NaiveDate::from_num_days_from_ce_opt(days) {
                    Some(d) => Value::Date(d),
                    None => Value::Null,
                }
            }
            DataType::DateTime => {
                let offset = idx * 8;
                let bytes: [u8; 8] = self.buffer[offset..offset + 8].try_into().unwrap();
                let millis = i64::from_le_bytes(bytes);
                match chrono::DateTime::from_timestamp_millis(millis) {
                    Some(dt) => Value::DateTime(dt.naive_utc()),
                    None => Value::Null,
                }
            }
            DataType::Duration => {
                let offset = idx * 8;
                let bytes: [u8; 8] = self.buffer[offset..offset + 8].try_into().unwrap();
                Value::Duration(i64::from_le_bytes(bytes))
            }
        }
    }

    /// Write a value at the given index (must be within capacity).
    pub fn set_value(&mut self, idx: usize, value: &Value) {
        if idx >= self.capacity as usize {
            return;
        }

        if value.is_null() {
            self.set_null(idx, true);
            return;
        }

        self.set_null(idx, false);

        match (&self.data_type, value) {
            (DataType::Bool, Value::Bool(b)) => {
                self.buffer[idx] = if *b { 1 } else { 0 };
            }
            (DataType::Int64 | DataType::Serial, Value::Int(v)) => {
                let offset = idx * 8;
                self.buffer[offset..offset + 8].copy_from_slice(&v.to_le_bytes());
            }
            (DataType::Double, Value::Float(v)) => {
                let offset = idx * 8;
                self.buffer[offset..offset + 8].copy_from_slice(&v.to_le_bytes());
            }
            (DataType::InternalId, Value::InternalId(id)) => {
                let offset = idx * 12;
                self.buffer[offset..offset + 4]
                    .copy_from_slice(&id.table_id.to_le_bytes());
                self.buffer[offset + 4..offset + 12]
                    .copy_from_slice(&id.offset.to_le_bytes());
            }
            (DataType::String, Value::String(s)) => {
                if let Some(ref mut strings) = self.strings {
                    while strings.len() <= idx {
                        strings.push(None);
                    }
                    strings[idx] = Some(s.clone());
                }
            }
            (DataType::Date, Value::Date(d)) => {
                let offset = idx * 4;
                self.buffer[offset..offset + 4]
                    .copy_from_slice(&d.num_days_from_ce().to_le_bytes());
            }
            (DataType::DateTime, Value::DateTime(dt)) => {
                let offset = idx * 8;
                let millis = dt.and_utc().timestamp_millis();
                self.buffer[offset..offset + 8].copy_from_slice(&millis.to_le_bytes());
            }
            (DataType::Duration, Value::Duration(ms)) => {
                let offset = idx * 8;
                self.buffer[offset..offset + 8].copy_from_slice(&ms.to_le_bytes());
            }
            _ => {} // type mismatch — silently ignore
        }
    }

    /// Append a value to the end. Returns error if at capacity.
    pub fn append(&mut self, value: &Value) -> Result<(), GqliteError> {
        if self.num_values >= self.capacity {
            return Err(GqliteError::Storage(format!(
                "ColumnChunk at capacity ({})",
                self.capacity
            )));
        }
        let idx = self.num_values as usize;
        self.set_value(idx, value);
        self.num_values += 1;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.num_values as usize
    }

    pub fn is_empty(&self) -> bool {
        self.num_values == 0
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    pub fn is_full(&self) -> bool {
        self.num_values >= self.capacity
    }

    /// Write this ColumnChunk to the pager and return metadata.
    pub fn flush_to_disk(
        &self,
        pager: &mut Pager,
    ) -> Result<ColumnChunkMetadata, GqliteError> {
        let page_size = pager.page_size() as usize;
        let mut meta = ColumnChunkMetadata {
            num_values: self.num_values,
            ..Default::default()
        };

        // Write data buffer (for fixed-size types)
        if self.data_type != DataType::String && !self.buffer.is_empty() {
            let data_bytes = &self.buffer[..self.used_buffer_bytes()];

            // Try bit-packing compression for Int64/Serial columns
            if matches!(self.data_type, DataType::Int64 | DataType::Serial)
                && self.num_values > 0
            {
                let values = self.extract_non_null_int64_values();
                if !values.is_empty() {
                    let compressed_size = compression::compressed_size_int64(&values);
                    if compressed_size < data_bytes.len() {
                        // Compression saves space — use it
                        let compressed = compression::compress_int64(&values);
                        meta.data_pages =
                            write_bytes_to_pages(pager, &compressed, page_size)?;
                        meta.compressed = true;
                        meta.compressed_len = compressed.len() as u64;
                    } else {
                        meta.data_pages = write_bytes_to_pages(pager, data_bytes, page_size)?;
                    }
                } else {
                    meta.data_pages = write_bytes_to_pages(pager, data_bytes, page_size)?;
                }
            } else {
                meta.data_pages = write_bytes_to_pages(pager, data_bytes, page_size)?;
            }
        }

        // Write strings
        if let Some(ref strings) = self.strings {
            let encoded = encode_strings(strings, self.num_values as usize);
            meta.string_pages = write_bytes_to_pages(pager, &encoded, page_size)?;
        }

        // Write null bitmap
        let null_bytes = self.null_mask.as_raw_slice();
        let used_null_bytes = (self.num_values as usize + 7) / 8;
        if used_null_bytes > 0 {
            meta.null_bitmap_pages =
                write_bytes_to_pages(pager, &null_bytes[..used_null_bytes], page_size)?;
        }

        Ok(meta)
    }

    /// Load a ColumnChunk from disk using metadata.
    pub fn load_from_disk(
        pager: &Pager,
        meta: &ColumnChunkMetadata,
        data_type: DataType,
    ) -> Result<Self, GqliteError> {
        let page_size = pager.page_size() as usize;
        let capacity = CHUNK_CAPACITY as u64;
        let num_values = meta.num_values;

        let byte_size = data_type.byte_size().unwrap_or(0);
        let buf_len = byte_size * capacity as usize;

        let mut buffer = vec![0u8; buf_len];
        let mut strings = if data_type == DataType::String {
            Some(Vec::new())
        } else {
            None
        };

        // Load data buffer
        if data_type != DataType::String && !meta.data_pages.is_empty() {
            if meta.compressed
                && matches!(data_type, DataType::Int64 | DataType::Serial)
            {
                // Compressed Int64 — read compressed bytes and decompress
                let compressed_bytes = read_bytes_from_pages(
                    pager,
                    &meta.data_pages,
                    meta.compressed_len as usize,
                    page_size,
                )?;
                let values = compression::decompress_int64(&compressed_bytes);
                for (i, val) in values.iter().enumerate() {
                    let offset = i * 8;
                    buffer[offset..offset + 8].copy_from_slice(&val.to_le_bytes());
                }
            } else {
                let used = byte_size * num_values as usize;
                let data_bytes =
                    read_bytes_from_pages(pager, &meta.data_pages, used, page_size)?;
                buffer[..data_bytes.len()].copy_from_slice(&data_bytes);
            }
        }

        // Load strings
        if let Some(ref mut strs) = strings {
            if !meta.string_pages.is_empty() {
                // First read all string data — we don't know exact size yet, read full pages
                let full = read_full_pages(pager, &meta.string_pages, page_size)?;
                *strs = decode_strings(&full, num_values as usize);
            }
        }

        // Load null bitmap
        let used_null_bytes = (num_values as usize + 7) / 8;
        let mut null_mask = bitvec![u8, Lsb0; 0; capacity as usize];
        if !meta.null_bitmap_pages.is_empty() && used_null_bytes > 0 {
            let null_bytes =
                read_bytes_from_pages(pager, &meta.null_bitmap_pages, used_null_bytes, page_size)?;
            let raw = null_mask.as_raw_mut_slice();
            raw[..null_bytes.len()].copy_from_slice(&null_bytes);
        }

        Ok(Self {
            data_type,
            buffer,
            strings,
            null_mask,
            num_values,
            capacity,
        })
    }

    /// Number of buffer bytes actually used.
    fn used_buffer_bytes(&self) -> usize {
        let byte_size = self.data_type.byte_size().unwrap_or(0);
        byte_size * self.num_values as usize
    }

    /// Extract non-null Int64 values in order, for compression.
    /// Returns all values (including positions that are NULL — stored as 0 in buffer).
    /// We compress the full buffer values, and the null bitmap restores NULLs on load.
    fn extract_non_null_int64_values(&self) -> Vec<i64> {
        let mut values = Vec::with_capacity(self.num_values as usize);
        for i in 0..self.num_values as usize {
            let offset = i * 8;
            let bytes: [u8; 8] = self.buffer[offset..offset + 8].try_into().unwrap();
            values.push(i64::from_le_bytes(bytes));
        }
        values
    }
}

// ── Helper: paged byte I/O ─────────────────────────────────────────

fn write_bytes_to_pages(
    pager: &mut Pager,
    data: &[u8],
    page_size: usize,
) -> Result<Vec<PageId>, GqliteError> {
    let mut pages = Vec::new();
    let mut offset = 0;
    while offset < data.len() {
        let page_id = pager.allocate_page()?;
        let end = std::cmp::min(offset + page_size, data.len());
        let mut page_buf = vec![0u8; page_size];
        page_buf[..end - offset].copy_from_slice(&data[offset..end]);
        pager.write_page(page_id, &page_buf)?;
        pages.push(page_id);
        offset += page_size;
    }
    Ok(pages)
}

fn read_bytes_from_pages(
    pager: &Pager,
    page_ids: &[PageId],
    total_bytes: usize,
    page_size: usize,
) -> Result<Vec<u8>, GqliteError> {
    let mut result = Vec::with_capacity(total_bytes);
    for &page_id in page_ids {
        let mut buf = vec![0u8; page_size];
        pager.read_page(page_id, &mut buf)?;
        let remaining = total_bytes - result.len();
        let take = std::cmp::min(page_size, remaining);
        result.extend_from_slice(&buf[..take]);
        if result.len() >= total_bytes {
            break;
        }
    }
    Ok(result)
}

fn read_full_pages(
    pager: &Pager,
    page_ids: &[PageId],
    page_size: usize,
) -> Result<Vec<u8>, GqliteError> {
    let mut result = Vec::with_capacity(page_ids.len() * page_size);
    for &page_id in page_ids {
        let mut buf = vec![0u8; page_size];
        pager.read_page(page_id, &mut buf)?;
        result.extend_from_slice(&buf);
    }
    Ok(result)
}

// ── String encoding: length-prefixed format ────────────────────────
// Each string: [4 bytes LE length][UTF-8 bytes]
// NULL string: length = u32::MAX

fn encode_strings(strings: &[Option<String>], num_values: usize) -> Vec<u8> {
    let mut buf = Vec::new();
    for i in 0..num_values {
        match strings.get(i) {
            Some(Some(s)) => {
                buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
            _ => {
                buf.extend_from_slice(&u32::MAX.to_le_bytes());
            }
        }
    }
    buf
}

fn decode_strings(data: &[u8], num_values: usize) -> Vec<Option<String>> {
    let mut result = Vec::with_capacity(num_values);
    let mut offset = 0;
    for _ in 0..num_values {
        if offset + 4 > data.len() {
            result.push(None);
            continue;
        }
        let len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        offset += 4;
        if len == u32::MAX {
            result.push(None);
        } else {
            let end = offset + len as usize;
            if end <= data.len() {
                let s = String::from_utf8_lossy(&data[offset..end]).to_string();
                result.push(Some(s));
                offset = end;
            } else {
                result.push(None);
            }
        }
    }
    result
}
