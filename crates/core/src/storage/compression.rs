//! Frame-of-Reference (FOR) + Bit-Packing compression for integer columns.
//!
//! Encoding layout:
//!   [8 bytes: min value (i64 LE)]
//!   [1 byte:  bit width (0..=64)]
//!   [4 bytes: count (u32 LE)]
//!   [packed bits: ceil(count * bit_width / 8) bytes]
//!
//! All values are stored as (value - min), packed into the minimum number of bits.
//! A bit_width of 0 means all values are identical (= min).

/// Compress a slice of i64 values using FOR + bit-packing.
///
/// Returns the compressed byte buffer.
pub fn compress_int64(values: &[i64]) -> Vec<u8> {
    if values.is_empty() {
        return encode_header(0, 0, 0);
    }

    let min = *values.iter().min().unwrap();
    let max = *values.iter().max().unwrap();

    // Compute bit width needed for (max - min)
    let range = (max as u128).wrapping_sub(min as u128) as u64;
    let bit_width = if range == 0 { 0 } else { 64 - range.leading_zeros() } as u8;

    let count = values.len() as u32;
    let packed_bytes = ((count as usize) * (bit_width as usize) + 7) / 8;

    let mut buf = encode_header(min, bit_width, count);
    buf.resize(buf.len() + packed_bytes, 0);

    if bit_width > 0 {
        let data_start = 13; // 8 + 1 + 4
        let mut bit_offset = 0usize;
        for &val in values {
            let residual = (val as u128).wrapping_sub(min as u128) as u64;
            pack_bits(&mut buf[data_start..], bit_offset, residual, bit_width);
            bit_offset += bit_width as usize;
        }
    }

    buf
}

/// Decompress a FOR + bit-packed buffer back to i64 values.
///
/// Returns the decompressed values.
pub fn decompress_int64(data: &[u8]) -> Vec<i64> {
    if data.len() < 13 {
        return Vec::new();
    }

    let (min, bit_width, count) = decode_header(data);

    let mut values = Vec::with_capacity(count as usize);

    if bit_width == 0 {
        // All values are identical
        values.resize(count as usize, min);
    } else {
        let data_start = 13;
        let mut bit_offset = 0usize;
        for _ in 0..count {
            let residual = unpack_bits(&data[data_start..], bit_offset, bit_width);
            let val = (min as u128).wrapping_add(residual as u128) as i64;
            values.push(val);
            bit_offset += bit_width as usize;
        }
    }

    values
}

/// Returns the compressed size in bytes for a given set of values,
/// without actually compressing. Useful for deciding whether to compress.
pub fn compressed_size_int64(values: &[i64]) -> usize {
    if values.is_empty() {
        return 13; // header only
    }

    let min = *values.iter().min().unwrap();
    let max = *values.iter().max().unwrap();
    let range = (max as u128).wrapping_sub(min as u128) as u64;
    let bit_width = if range == 0 { 0 } else { 64 - range.leading_zeros() } as u8;
    let packed_bytes = ((values.len()) * (bit_width as usize) + 7) / 8;

    13 + packed_bytes
}

// ── Internal helpers ────────────────────────────────────────────────

fn encode_header(min: i64, bit_width: u8, count: u32) -> Vec<u8> {
    let mut buf = Vec::with_capacity(13);
    buf.extend_from_slice(&min.to_le_bytes());
    buf.push(bit_width);
    buf.extend_from_slice(&count.to_le_bytes());
    buf
}

fn decode_header(data: &[u8]) -> (i64, u8, u32) {
    let min = i64::from_le_bytes(data[0..8].try_into().unwrap());
    let bit_width = data[8];
    let count = u32::from_le_bytes(data[9..13].try_into().unwrap());
    (min, bit_width, count)
}

/// Pack `bit_width` bits of `value` starting at `bit_offset` in `buf`.
fn pack_bits(buf: &mut [u8], bit_offset: usize, value: u64, bit_width: u8) {
    let bw = bit_width as usize;
    for i in 0..bw {
        let bit = (value >> i) & 1;
        let pos = bit_offset + i;
        let byte_idx = pos / 8;
        let bit_idx = pos % 8;
        if bit == 1 {
            buf[byte_idx] |= 1 << bit_idx;
        }
    }
}

/// Unpack `bit_width` bits starting at `bit_offset` in `buf`.
fn unpack_bits(buf: &[u8], bit_offset: usize, bit_width: u8) -> u64 {
    let bw = bit_width as usize;
    let mut value = 0u64;
    for i in 0..bw {
        let pos = bit_offset + i;
        let byte_idx = pos / 8;
        let bit_idx = pos % 8;
        if byte_idx < buf.len() && (buf[byte_idx] >> bit_idx) & 1 == 1 {
            value |= 1 << i;
        }
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_empty() {
        let compressed = compress_int64(&[]);
        let decompressed = decompress_int64(&compressed);
        assert!(decompressed.is_empty());
    }

    #[test]
    fn roundtrip_single_value() {
        let values = vec![42];
        let compressed = compress_int64(&values);
        let decompressed = decompress_int64(&compressed);
        assert_eq!(decompressed, values);
    }

    #[test]
    fn roundtrip_identical_values() {
        let values = vec![100; 1000];
        let compressed = compress_int64(&values);
        // bit_width = 0, so only header (13 bytes)
        assert_eq!(compressed.len(), 13);
        let decompressed = decompress_int64(&compressed);
        assert_eq!(decompressed, values);
    }

    #[test]
    fn roundtrip_sequential() {
        let values: Vec<i64> = (100..200).collect();
        let compressed = compress_int64(&values);
        let decompressed = decompress_int64(&compressed);
        assert_eq!(decompressed, values);

        // Range 0..99, needs 7 bits. 100 values * 7 bits = 700 bits = 88 bytes + 13 header = 101
        // vs uncompressed: 800 bytes
        assert!(compressed.len() < 800);
    }

    #[test]
    fn roundtrip_negative_values() {
        let values = vec![-100, -50, 0, 50, 100];
        let compressed = compress_int64(&values);
        let decompressed = decompress_int64(&compressed);
        assert_eq!(decompressed, values);
    }

    #[test]
    fn roundtrip_large_range() {
        let values = vec![i64::MIN, 0, i64::MAX];
        let compressed = compress_int64(&values);
        let decompressed = decompress_int64(&compressed);
        assert_eq!(decompressed, values);
    }

    #[test]
    fn compression_ratio_narrow_range() {
        // 100 values in [100..200], each is i64 (8 bytes) = 800 bytes uncompressed
        let values: Vec<i64> = (100..200).collect();
        let compressed = compress_int64(&values);
        let ratio = compressed.len() as f64 / (values.len() * 8) as f64;
        // Should be well under 50% compression ratio
        assert!(ratio < 0.5, "ratio was {:.2}", ratio);
    }

    #[test]
    fn compressed_size_estimate() {
        let values: Vec<i64> = (0..100).collect();
        let estimated = compressed_size_int64(&values);
        let actual = compress_int64(&values).len();
        assert_eq!(estimated, actual);
    }
}
