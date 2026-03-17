use std::hash::{Hash, Hasher};

use chrono;
use chrono::Datelike;
use serde::{Deserialize, Serialize};

use super::data_type::DataType;
use super::graph::InternalId;

/// A dynamically-typed property value stored on nodes and edges.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    InternalId(InternalId),
    List(Vec<Value>),
    Date(chrono::NaiveDate),
    DateTime(chrono::NaiveDateTime),
    Duration(i64), // milliseconds
}

// Manual Eq: treat f64 NaN == NaN for HashMap usage (PK index).
impl Eq for Value {}

// Manual Hash: use f64 bit pattern for hashing.
impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Bool(b) => b.hash(state),
            Value::Int(i) => i.hash(state),
            Value::Float(f) => f.to_bits().hash(state),
            Value::String(s) => s.hash(state),
            Value::Bytes(b) => b.hash(state),
            Value::InternalId(id) => id.hash(state),
            Value::List(l) => l.hash(state),
            Value::Date(d) => d.num_days_from_ce().hash(state),
            Value::DateTime(dt) => dt.and_utc().timestamp_millis().hash(state),
            Value::Duration(ms) => ms.hash(state),
        }
    }
}

impl Value {
    /// Returns the `DataType` corresponding to this value, or `None` for `Null`.
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Value::Null => None,
            Value::Bool(_) => Some(DataType::Bool),
            Value::Int(_) => Some(DataType::Int64),
            Value::Float(_) => Some(DataType::Double),
            Value::String(_) => Some(DataType::String),
            Value::Bytes(_) => Some(DataType::String), // treated as opaque string-like
            Value::InternalId(_) => Some(DataType::InternalId),
            Value::List(_) => None, // lists don't map to a single DataType
            Value::Date(_) => Some(DataType::Date),
            Value::DateTime(_) => Some(DataType::DateTime),
            Value::Duration(_) => Some(DataType::Duration),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_internal_id(&self) -> Option<&InternalId> {
        match self {
            Value::InternalId(id) => Some(id),
            _ => None,
        }
    }

    pub fn as_date(&self) -> Option<chrono::NaiveDate> {
        match self {
            Value::Date(d) => Some(*d),
            _ => None,
        }
    }

    pub fn as_datetime(&self) -> Option<chrono::NaiveDateTime> {
        match self {
            Value::DateTime(dt) => Some(*dt),
            _ => None,
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Int(i) => write!(f, "{i}"),
            Value::Float(v) => write!(f, "{v}"),
            Value::String(s) => write!(f, "{s}"),
            Value::Bytes(b) => write!(f, "<{} bytes>", b.len()),
            Value::InternalId(id) => write!(f, "{id}"),
            Value::List(items) => {
                write!(f, "[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, "]")
            }
            Value::Date(d) => write!(f, "{}", d.format("%Y-%m-%d")),
            Value::DateTime(dt) => write!(f, "{}", dt.format("%Y-%m-%dT%H:%M:%S")),
            Value::Duration(ms) => {
                let total_secs = *ms as f64 / 1000.0;
                write!(f, "PT{:.3}S", total_secs)
            }
        }
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}
