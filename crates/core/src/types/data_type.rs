// Re-export DataType from the standalone parser crate.
// This keeps all `use crate::types::data_type::DataType` imports in core working.
pub use gqlite_parser::data_type::DataType;
