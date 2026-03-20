use gqlite_core::storage::compression::{compress_int64, compressed_size_int64, decompress_int64};

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
