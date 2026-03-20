use gqlite_core::types::data_type::DataType;
use gqlite_core::types::graph::InternalId;
use gqlite_core::types::value::Value;

#[test]
fn value_data_types() {
    assert_eq!(Value::Null.data_type(), None);
    assert_eq!(Value::Bool(true).data_type(), Some(DataType::Bool));
    assert_eq!(Value::Int(42).data_type(), Some(DataType::Int64));
    assert_eq!(Value::Float(3.15).data_type(), Some(DataType::Double));
    assert_eq!(Value::String("hello".into()).data_type(), Some(DataType::String));
    assert_eq!(Value::InternalId(InternalId::new(1, 0)).data_type(), Some(DataType::InternalId));
}

#[test]
fn value_accessors() {
    assert!(Value::Null.is_null());
    assert!(!Value::Int(1).is_null());
    assert_eq!(Value::Bool(true).as_bool(), Some(true));
    assert_eq!(Value::Int(42).as_int(), Some(42));
    assert_eq!(Value::Float(3.15).as_float(), Some(3.15));
    assert_eq!(Value::String("hi".into()).as_string(), Some("hi"));

    // wrong type returns None
    assert_eq!(Value::Int(42).as_string(), None);
    assert_eq!(Value::String("hi".into()).as_int(), None);
}

#[test]
fn value_display() {
    assert_eq!(format!("{}", Value::Null), "NULL");
    assert_eq!(format!("{}", Value::InternalId(InternalId::new(1, 42))), "1:42");
    assert_eq!(
        format!("{}", Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)])),
        "[1, 2, 3]"
    );
}

#[test]
fn value_from_impls() {
    let v: Value = 42i64.into();
    assert_eq!(v.as_int(), Some(42));

    let v: Value = "hello".into();
    assert_eq!(v.as_string(), Some("hello"));
}
