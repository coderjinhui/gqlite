use gqlite_parser::data_type::DataType;

#[test]
fn byte_sizes() {
    assert_eq!(DataType::Bool.byte_size(), Some(1));
    assert_eq!(DataType::Int64.byte_size(), Some(8));
    assert_eq!(DataType::Double.byte_size(), Some(8));
    assert_eq!(DataType::InternalId.byte_size(), Some(12));
    assert_eq!(DataType::String.byte_size(), None);
}

#[test]
fn display() {
    assert_eq!(format!("{}", DataType::Bool), "BOOL");
    assert_eq!(format!("{}", DataType::String), "STRING");
}
