//! Vectorized execution data structures: ValueVector and DataChunk.
//!
//! These are the core runtime containers for columnar batch processing.

use bitvec::prelude::*;

use crate::types::data_type::DataType;
use crate::types::graph::InternalId;
use crate::types::value::Value;

/// Default vector capacity (rows per batch).
pub const VECTOR_CAPACITY: usize = 2048;

/// A single column of typed values in columnar layout.
#[derive(Debug, Clone)]
pub enum ValueVector {
    Bool(Vec<bool>),
    Int64(Vec<i64>),
    Double(Vec<f64>),
    String(Vec<String>),
    InternalId(Vec<InternalId>),
}

impl ValueVector {
    /// Create a new empty vector for the given data type.
    pub fn new(data_type: &DataType, capacity: usize) -> Self {
        match data_type {
            DataType::Bool => ValueVector::Bool(Vec::with_capacity(capacity)),
            DataType::Int64
            | DataType::Serial
            | DataType::Date
            | DataType::DateTime
            | DataType::Duration => ValueVector::Int64(Vec::with_capacity(capacity)),
            DataType::Double => ValueVector::Double(Vec::with_capacity(capacity)),
            DataType::String => ValueVector::String(Vec::with_capacity(capacity)),
            DataType::InternalId => ValueVector::InternalId(Vec::with_capacity(capacity)),
        }
    }

    /// Create a new vector pre-filled with default values.
    pub fn with_size(data_type: &DataType, size: usize) -> Self {
        match data_type {
            DataType::Bool => ValueVector::Bool(vec![false; size]),
            DataType::Int64
            | DataType::Serial
            | DataType::Date
            | DataType::DateTime
            | DataType::Duration => ValueVector::Int64(vec![0; size]),
            DataType::Double => ValueVector::Double(vec![0.0; size]),
            DataType::String => ValueVector::String(vec![String::new(); size]),
            DataType::InternalId => ValueVector::InternalId(vec![InternalId::new(0, 0); size]),
        }
    }

    /// Get the value at the given row index.
    pub fn get(&self, idx: usize) -> Value {
        match self {
            ValueVector::Bool(v) => Value::Bool(v[idx]),
            ValueVector::Int64(v) => Value::Int(v[idx]),
            ValueVector::Double(v) => Value::Float(v[idx]),
            ValueVector::String(v) => Value::String(v[idx].clone()),
            ValueVector::InternalId(v) => Value::InternalId(v[idx]),
        }
    }

    /// Set the value at the given row index.
    pub fn set(&mut self, idx: usize, value: &Value) {
        match (self, value) {
            (ValueVector::Bool(v), Value::Bool(b)) => v[idx] = *b,
            (ValueVector::Int64(v), Value::Int(i)) => v[idx] = *i,
            (ValueVector::Double(v), Value::Float(f)) => v[idx] = *f,
            (ValueVector::String(v), Value::String(s)) => v[idx] = s.clone(),
            (ValueVector::InternalId(v), Value::InternalId(id)) => v[idx] = *id,
            // For NULL or type mismatch, set to default
            (ValueVector::Bool(v), _) => v[idx] = false,
            (ValueVector::Int64(v), _) => v[idx] = 0,
            (ValueVector::Double(v), _) => v[idx] = 0.0,
            (ValueVector::String(v), _) => v[idx] = String::new(),
            (ValueVector::InternalId(v), _) => v[idx] = InternalId::new(0, 0),
        }
    }

    /// Append a value to the end of the vector.
    pub fn push(&mut self, value: &Value) {
        match (self, value) {
            (ValueVector::Bool(v), Value::Bool(b)) => v.push(*b),
            (ValueVector::Int64(v), Value::Int(i)) => v.push(*i),
            (ValueVector::Double(v), Value::Float(f)) => v.push(*f),
            (ValueVector::String(v), Value::String(s)) => v.push(s.clone()),
            (ValueVector::InternalId(v), Value::InternalId(id)) => v.push(*id),
            // Default for NULL or type mismatch
            (ValueVector::Bool(v), _) => v.push(false),
            (ValueVector::Int64(v), _) => v.push(0),
            (ValueVector::Double(v), _) => v.push(0.0),
            (ValueVector::String(v), _) => v.push(String::new()),
            (ValueVector::InternalId(v), _) => v.push(InternalId::new(0, 0)),
        }
    }

    /// Current number of elements.
    pub fn len(&self) -> usize {
        match self {
            ValueVector::Bool(v) => v.len(),
            ValueVector::Int64(v) => v.len(),
            ValueVector::Double(v) => v.len(),
            ValueVector::String(v) => v.len(),
            ValueVector::InternalId(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all elements but keep allocated memory.
    pub fn clear(&mut self) {
        match self {
            ValueVector::Bool(v) => v.clear(),
            ValueVector::Int64(v) => v.clear(),
            ValueVector::Double(v) => v.clear(),
            ValueVector::String(v) => v.clear(),
            ValueVector::InternalId(v) => v.clear(),
        }
    }

    /// Returns the DataType of this vector.
    pub fn data_type(&self) -> DataType {
        match self {
            ValueVector::Bool(_) => DataType::Bool,
            ValueVector::Int64(_) => DataType::Int64,
            ValueVector::Double(_) => DataType::Double,
            ValueVector::String(_) => DataType::String,
            ValueVector::InternalId(_) => DataType::InternalId,
        }
    }
}

/// A batch of columnar data — the unit of data exchange between operators.
#[derive(Debug, Clone)]
pub struct DataChunk {
    /// Column vectors.
    pub vectors: Vec<ValueVector>,
    /// Per-column null bitmaps. bit=1 means NULL.
    pub null_masks: Vec<BitVec<u8, Lsb0>>,
    /// Number of valid rows in this chunk.
    pub num_rows: usize,
    /// Maximum row capacity.
    pub capacity: usize,
}

impl DataChunk {
    /// Create a new DataChunk with the given column types and capacity.
    pub fn new(types: &[DataType], capacity: usize) -> Self {
        let vectors: Vec<ValueVector> =
            types.iter().map(|dt| ValueVector::with_size(dt, capacity)).collect();
        let null_masks: Vec<BitVec<u8, Lsb0>> = types
            .iter()
            .map(|_| {
                let mut bv = BitVec::with_capacity(capacity);
                bv.resize(capacity, false);
                bv
            })
            .collect();
        Self { vectors, null_masks, num_rows: 0, capacity }
    }

    /// Create a DataChunk with zero capacity (to be grown via append).
    pub fn empty(types: &[DataType]) -> Self {
        let vectors: Vec<ValueVector> = types.iter().map(|dt| ValueVector::new(dt, 0)).collect();
        let null_masks: Vec<BitVec<u8, Lsb0>> = types.iter().map(|_| BitVec::new()).collect();
        Self { vectors, null_masks, num_rows: 0, capacity: 0 }
    }

    /// Get the value at (col, row), respecting NULL mask.
    pub fn get_value(&self, col: usize, row: usize) -> Value {
        if self.null_masks[col][row] {
            return Value::Null;
        }
        self.vectors[col].get(row)
    }

    /// Set the value at (col, row), updating the NULL mask.
    pub fn set_value(&mut self, col: usize, row: usize, value: &Value) {
        if value.is_null() {
            self.null_masks[col].set(row, true);
        } else {
            self.null_masks[col].set(row, false);
            self.vectors[col].set(row, value);
        }
    }

    /// Append a row of values to the chunk. Returns the row index.
    pub fn append_row(&mut self, values: &[Value]) -> usize {
        let row = self.num_rows;
        if row >= self.capacity {
            // Grow capacity
            self.capacity = if self.capacity == 0 { 64 } else { self.capacity * 2 };
            for (i, vec) in self.vectors.iter_mut().enumerate() {
                let dt = vec.data_type();
                while vec.len() < self.capacity {
                    vec.push(&default_value(&dt));
                }
                self.null_masks[i].resize(self.capacity, false);
            }
        }
        // Ensure vectors have enough elements
        for (i, vec) in self.vectors.iter_mut().enumerate() {
            while vec.len() <= row {
                let dt = vec.data_type();
                vec.push(&default_value(&dt));
                self.null_masks[i].push(false);
            }
        }
        for (col, value) in values.iter().enumerate() {
            self.set_value(col, row, value);
        }
        self.num_rows += 1;
        row
    }

    /// Reset the chunk for reuse (keep allocated memory).
    pub fn reset(&mut self) {
        self.num_rows = 0;
        for mask in &mut self.null_masks {
            mask.fill(false);
        }
    }

    /// Number of columns.
    pub fn num_columns(&self) -> usize {
        self.vectors.len()
    }

    /// Get the schema (data types) of this chunk.
    pub fn schema(&self) -> Vec<DataType> {
        self.vectors.iter().map(|v| v.data_type()).collect()
    }
}

fn default_value(dt: &DataType) -> Value {
    match dt {
        DataType::Bool => Value::Bool(false),
        DataType::Int64
        | DataType::Serial
        | DataType::Date
        | DataType::DateTime
        | DataType::Duration => Value::Int(0),
        DataType::Double => Value::Float(0.0),
        DataType::String => Value::String(String::new()),
        DataType::InternalId => Value::InternalId(InternalId::new(0, 0)),
    }
}
