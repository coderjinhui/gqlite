use gqlite_core::types::data_type::DataType;

#[test]
fn serde_roundtrip() {
    let dt = DataType::Int64;
    let encoded = bincode::serialize(&dt).unwrap();
    let decoded: DataType = bincode::deserialize(&encoded).unwrap();
    assert_eq!(dt, decoded);
}
