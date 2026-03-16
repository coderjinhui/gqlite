// Re-export DataType from the standalone parser crate.
// This keeps all `use crate::types::data_type::DataType` imports in core working.
pub use gqlite_parser::data_type::DataType;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_roundtrip() {
        let dt = DataType::Int64;
        let encoded = bincode::serialize(&dt).unwrap();
        let decoded: DataType = bincode::deserialize(&encoded).unwrap();
        assert_eq!(dt, decoded);
    }
}
