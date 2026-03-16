use serde::{Deserialize, Serialize};
use std::fmt;

/// Logical data types supported by gqlite's schema system.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Bool,
    Int64,
    Double,
    String,
    InternalId,
    /// Auto-incrementing integer (stored as Int64, auto-assigned on INSERT).
    Serial,
    /// Calendar date (i32 days since CE epoch).
    Date,
    /// Date and time (i64 milliseconds since Unix epoch).
    DateTime,
    /// Duration in milliseconds (i64).
    Duration,
}

impl DataType {
    /// Returns the fixed byte size of a value of this type, or `None` for variable-length types.
    pub fn byte_size(&self) -> Option<usize> {
        match self {
            DataType::Bool => Some(1),
            DataType::Int64 | DataType::Serial => Some(8),
            DataType::Double => Some(8),
            DataType::InternalId => Some(12), // u32 + u64
            DataType::String => None,
            DataType::Date => Some(4),       // i32 days since CE
            DataType::DateTime => Some(8),   // i64 millis since epoch
            DataType::Duration => Some(8),   // i64 millis
        }
    }

    /// Returns `true` if this type has a fixed byte width.
    pub fn is_fixed_size(&self) -> bool {
        self.byte_size().is_some()
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Bool => write!(f, "BOOL"),
            DataType::Int64 => write!(f, "INT64"),
            DataType::Double => write!(f, "DOUBLE"),
            DataType::String => write!(f, "STRING"),
            DataType::InternalId => write!(f, "INTERNAL_ID"),
            DataType::Serial => write!(f, "SERIAL"),
            DataType::Date => write!(f, "DATE"),
            DataType::DateTime => write!(f, "DATETIME"),
            DataType::Duration => write!(f, "DURATION"),
        }
    }
}
