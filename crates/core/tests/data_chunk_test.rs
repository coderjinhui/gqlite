use gqlite_core::executor::data_chunk::DataChunk;
use gqlite_core::types::data_type::DataType;
use gqlite_core::types::value::Value;

#[test]
fn create_and_set_values() {
    let types = vec![DataType::Int64, DataType::String, DataType::Bool];
    let mut chunk = DataChunk::new(&types, 4);
    assert_eq!(chunk.num_rows, 0);
    assert_eq!(chunk.capacity, 4);
    assert_eq!(chunk.num_columns(), 3);

    // Write row 0
    chunk.set_value(0, 0, &Value::Int(42));
    chunk.set_value(1, 0, &Value::String("hello".into()));
    chunk.set_value(2, 0, &Value::Bool(true));
    chunk.num_rows = 1;

    assert_eq!(chunk.get_value(0, 0), Value::Int(42));
    assert_eq!(chunk.get_value(1, 0), Value::String("hello".into()));
    assert_eq!(chunk.get_value(2, 0), Value::Bool(true));
}

#[test]
fn null_handling() {
    let types = vec![DataType::Int64, DataType::String];
    let mut chunk = DataChunk::new(&types, 4);

    chunk.set_value(0, 0, &Value::Int(1));
    chunk.set_value(1, 0, &Value::Null);
    chunk.num_rows = 1;

    assert_eq!(chunk.get_value(0, 0), Value::Int(1));
    assert_eq!(chunk.get_value(1, 0), Value::Null);
}

#[test]
fn append_rows() {
    let types = vec![DataType::Int64, DataType::String, DataType::Bool];
    let mut chunk = DataChunk::empty(&types);

    for i in 0..2048 {
        chunk.append_row(&[
            Value::Int(i),
            Value::String(format!("row_{}", i)),
            Value::Bool(i % 2 == 0),
        ]);
    }

    assert_eq!(chunk.num_rows, 2048);
    assert_eq!(chunk.get_value(0, 0), Value::Int(0));
    assert_eq!(chunk.get_value(1, 0), Value::String("row_0".into()));
    assert_eq!(chunk.get_value(0, 2047), Value::Int(2047));
    assert_eq!(chunk.get_value(2, 2047), Value::Bool(false));
}

#[test]
fn reset_and_reuse() {
    let types = vec![DataType::Int64];
    let mut chunk = DataChunk::new(&types, 4);

    chunk.set_value(0, 0, &Value::Int(99));
    chunk.num_rows = 1;
    assert_eq!(chunk.get_value(0, 0), Value::Int(99));

    chunk.reset();
    assert_eq!(chunk.num_rows, 0);

    // After reset, we can write new data
    chunk.set_value(0, 0, &Value::Int(100));
    chunk.num_rows = 1;
    assert_eq!(chunk.get_value(0, 0), Value::Int(100));
}

#[test]
fn null_then_overwrite() {
    let types = vec![DataType::Int64];
    let mut chunk = DataChunk::new(&types, 4);

    // Set NULL
    chunk.set_value(0, 0, &Value::Null);
    assert_eq!(chunk.get_value(0, 0), Value::Null);

    // Overwrite with real value
    chunk.set_value(0, 0, &Value::Int(42));
    assert_eq!(chunk.get_value(0, 0), Value::Int(42));
}

#[test]
fn schema_extraction() {
    let types = vec![DataType::Int64, DataType::String, DataType::Double];
    let chunk = DataChunk::new(&types, 4);
    assert_eq!(chunk.schema(), types);
}

#[test]
fn append_with_nulls() {
    let types = vec![DataType::Int64, DataType::String];
    let mut chunk = DataChunk::empty(&types);

    chunk.append_row(&[Value::Int(1), Value::Null]);
    chunk.append_row(&[Value::Null, Value::String("hi".into())]);

    assert_eq!(chunk.num_rows, 2);
    assert_eq!(chunk.get_value(0, 0), Value::Int(1));
    assert_eq!(chunk.get_value(1, 0), Value::Null);
    assert_eq!(chunk.get_value(0, 1), Value::Null);
    assert_eq!(chunk.get_value(1, 1), Value::String("hi".into()));
}
