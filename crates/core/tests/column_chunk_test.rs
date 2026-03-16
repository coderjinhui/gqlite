use gqlite_core::storage::column_chunk::ColumnChunk;
use gqlite_core::storage::pager::Pager;
use gqlite_core::types::data_type::DataType;
use gqlite_core::types::graph::InternalId;
use gqlite_core::types::value::Value;
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
fn int64_append_and_get() {
    let mut chunk = ColumnChunk::with_default_capacity(DataType::Int64);
    for i in 0..100 {
        chunk.append(&Value::Int(i)).unwrap();
    }
    assert_eq!(chunk.len(), 100);
    for i in 0..100 {
        assert_eq!(chunk.get_value(i), Value::Int(i as i64));
    }
}

#[test]
fn bool_column() {
    let mut chunk = ColumnChunk::with_default_capacity(DataType::Bool);
    chunk.append(&Value::Bool(true)).unwrap();
    chunk.append(&Value::Bool(false)).unwrap();
    chunk.append(&Value::Bool(true)).unwrap();
    assert_eq!(chunk.get_value(0), Value::Bool(true));
    assert_eq!(chunk.get_value(1), Value::Bool(false));
    assert_eq!(chunk.get_value(2), Value::Bool(true));
}

#[test]
fn double_column() {
    let mut chunk = ColumnChunk::with_default_capacity(DataType::Double);
    chunk.append(&Value::Float(3.14)).unwrap();
    chunk.append(&Value::Float(-1.0)).unwrap();
    assert_eq!(chunk.get_value(0), Value::Float(3.14));
    assert_eq!(chunk.get_value(1), Value::Float(-1.0));
}

#[test]
fn internal_id_column() {
    let mut chunk = ColumnChunk::with_default_capacity(DataType::InternalId);
    let id = InternalId::new(5, 42);
    chunk.append(&Value::InternalId(id)).unwrap();
    assert_eq!(chunk.get_value(0), Value::InternalId(id));
}

#[test]
fn string_column() {
    let mut chunk = ColumnChunk::with_default_capacity(DataType::String);
    chunk.append(&Value::String("hello".into())).unwrap();
    chunk.append(&Value::String("world".into())).unwrap();
    assert_eq!(chunk.get_value(0), Value::String("hello".into()));
    assert_eq!(chunk.get_value(1), Value::String("world".into()));
}

#[test]
fn null_values() {
    let mut chunk = ColumnChunk::with_default_capacity(DataType::Int64);
    chunk.append(&Value::Int(1)).unwrap();
    chunk.append(&Value::Null).unwrap();
    chunk.append(&Value::Int(3)).unwrap();

    assert_eq!(chunk.get_value(0), Value::Int(1));
    assert_eq!(chunk.get_value(1), Value::Null);
    assert!(chunk.is_null(1));
    assert!(!chunk.is_null(0));
    assert_eq!(chunk.get_value(2), Value::Int(3));
}

#[test]
fn null_string() {
    let mut chunk = ColumnChunk::with_default_capacity(DataType::String);
    chunk.append(&Value::String("a".into())).unwrap();
    chunk.append(&Value::Null).unwrap();
    chunk.append(&Value::String("c".into())).unwrap();

    assert_eq!(chunk.get_value(0), Value::String("a".into()));
    assert!(chunk.is_null(1));
    assert_eq!(chunk.get_value(1), Value::Null);
    assert_eq!(chunk.get_value(2), Value::String("c".into()));
}

#[test]
fn capacity_overflow() {
    let mut chunk = ColumnChunk::new(DataType::Int64, 3);
    chunk.append(&Value::Int(1)).unwrap();
    chunk.append(&Value::Int(2)).unwrap();
    chunk.append(&Value::Int(3)).unwrap();
    assert!(chunk.is_full());
    let result = chunk.append(&Value::Int(4));
    assert!(result.is_err());
}

#[test]
fn flush_and_load_int64() {
    let path = temp_path("test_column_int64.graph");
    cleanup(&path);

    let mut pager = Pager::create(&path).unwrap();

    let mut chunk = ColumnChunk::with_default_capacity(DataType::Int64);
    for i in 0..50 {
        chunk.append(&Value::Int(i * 10)).unwrap();
    }
    chunk.append(&Value::Null).unwrap(); // add a null

    let meta = chunk.flush_to_disk(&mut pager).unwrap();
    pager.sync().unwrap();

    let loaded =
        ColumnChunk::load_from_disk(&pager, &meta, DataType::Int64).unwrap();
    assert_eq!(loaded.len(), 51);
    for i in 0..50 {
        assert_eq!(loaded.get_value(i), Value::Int(i as i64 * 10));
    }
    assert!(loaded.is_null(50));

    drop(pager);
    cleanup(&path);
}

#[test]
fn flush_and_load_string() {
    let path = temp_path("test_column_str.graph");
    cleanup(&path);

    let mut pager = Pager::create(&path).unwrap();

    let mut chunk = ColumnChunk::with_default_capacity(DataType::String);
    chunk.append(&Value::String("hello".into())).unwrap();
    chunk.append(&Value::Null).unwrap();
    chunk.append(&Value::String("world".into())).unwrap();

    let meta = chunk.flush_to_disk(&mut pager).unwrap();
    pager.sync().unwrap();

    let loaded =
        ColumnChunk::load_from_disk(&pager, &meta, DataType::String).unwrap();
    assert_eq!(loaded.len(), 3);
    assert_eq!(loaded.get_value(0), Value::String("hello".into()));
    assert!(loaded.is_null(1));
    assert_eq!(loaded.get_value(2), Value::String("world".into()));

    drop(pager);
    cleanup(&path);
}

#[test]
fn set_value_overwrite() {
    let mut chunk = ColumnChunk::with_default_capacity(DataType::Int64);
    chunk.append(&Value::Int(10)).unwrap();
    chunk.append(&Value::Int(20)).unwrap();

    // Overwrite index 0
    chunk.set_value(0, &Value::Int(99));
    assert_eq!(chunk.get_value(0), Value::Int(99));
    assert_eq!(chunk.get_value(1), Value::Int(20));

    // Set to null
    chunk.set_value(1, &Value::Null);
    assert!(chunk.is_null(1));
}
