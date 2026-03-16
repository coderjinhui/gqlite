use crate::error::GqliteError;
use crate::storage::column_chunk::ColumnChunk;
use crate::storage::format::{CHUNK_CAPACITY, NODE_GROUP_SIZE};
use crate::types::data_type::DataType;
use crate::types::value::Value;

/// A single chunk within a NodeGroup, holding up to CHUNK_CAPACITY (2048) rows.
/// Each column has its own ColumnChunk.
pub struct ChunkedNodeGroup {
    columns: Vec<ColumnChunk>,
    num_rows: u64,
    capacity: u64,
}

impl ChunkedNodeGroup {
    pub fn new(data_types: &[DataType]) -> Self {
        let columns = data_types
            .iter()
            .map(|dt| ColumnChunk::with_default_capacity(dt.clone()))
            .collect();
        Self {
            columns,
            num_rows: 0,
            capacity: CHUNK_CAPACITY as u64,
        }
    }

    /// Append a row (one value per column). Returns the row index within this chunk.
    pub fn append_row(&mut self, values: &[Value]) -> Result<u64, GqliteError> {
        if self.num_rows >= self.capacity {
            return Err(GqliteError::Storage("ChunkedNodeGroup is full".into()));
        }
        if values.len() != self.columns.len() {
            return Err(GqliteError::Storage(format!(
                "expected {} columns, got {}",
                self.columns.len(),
                values.len()
            )));
        }
        for (col, val) in self.columns.iter_mut().zip(values.iter()) {
            col.append(val)?;
        }
        let row_idx = self.num_rows;
        self.num_rows += 1;
        Ok(row_idx)
    }

    /// Read a row by its index within this chunk.
    pub fn read_row(&self, row_idx: u64) -> Result<Vec<Value>, GqliteError> {
        if row_idx >= self.num_rows {
            return Err(GqliteError::Storage(format!(
                "row {} out of range (num_rows={})",
                row_idx, self.num_rows
            )));
        }
        let idx = row_idx as usize;
        Ok(self.columns.iter().map(|c| c.get_value(idx)).collect())
    }

    /// Update a single cell in this chunk.
    pub fn set_value(&mut self, row_idx: u64, col_idx: usize, value: &Value) {
        if (row_idx as usize) < self.columns[col_idx].len() {
            self.columns[col_idx].set_value(row_idx as usize, value);
        }
    }

    pub fn num_rows(&self) -> u64 {
        self.num_rows
    }

    pub fn is_full(&self) -> bool {
        self.num_rows >= self.capacity
    }

    pub fn columns(&self) -> &[ColumnChunk] {
        &self.columns
    }

    /// Add a new column filled with NULLs for existing rows.
    pub fn add_column(&mut self, data_type: &DataType) {
        let mut col = ColumnChunk::with_default_capacity(data_type.clone());
        // Fill with NULL for all existing rows
        for _ in 0..self.num_rows {
            col.append(&Value::Null).ok();
        }
        self.columns.push(col);
    }

    /// Remove a column by index.
    pub fn drop_column(&mut self, col_idx: usize) {
        if col_idx < self.columns.len() {
            self.columns.remove(col_idx);
        }
    }
}

/// A NodeGroup manages up to NODE_GROUP_SIZE (131072) rows, split into chunks of
/// CHUNK_CAPACITY (2048) each. This is the primary unit of storage for a node table.
pub struct NodeGroup {
    group_idx: u32,
    chunks: Vec<ChunkedNodeGroup>,
    data_types: Vec<DataType>,
    num_rows: u64,
}

impl NodeGroup {
    pub fn new(group_idx: u32, data_types: Vec<DataType>) -> Self {
        Self {
            group_idx,
            chunks: Vec::new(),
            data_types,
            num_rows: 0,
        }
    }

    /// Given a global row offset, compute (group_idx, offset_in_group).
    pub fn locate(global_offset: u64) -> (u32, u64) {
        let group_idx = (global_offset / NODE_GROUP_SIZE as u64) as u32;
        let offset_in_group = global_offset % NODE_GROUP_SIZE as u64;
        (group_idx, offset_in_group)
    }

    /// Append a row. Returns the offset within this group.
    pub fn append_row(&mut self, values: &[Value]) -> Result<u64, GqliteError> {
        if self.num_rows >= NODE_GROUP_SIZE as u64 {
            return Err(GqliteError::Storage("NodeGroup is full".into()));
        }

        // Find the current chunk or create a new one
        let need_new_chunk = self.chunks.is_empty() || self.chunks.last().unwrap().is_full();
        if need_new_chunk {
            self.chunks.push(ChunkedNodeGroup::new(&self.data_types));
        }

        let chunk = self.chunks.last_mut().unwrap();
        chunk.append_row(values)?;

        let offset = self.num_rows;
        self.num_rows += 1;
        Ok(offset)
    }

    /// Read a row by its offset within this group.
    pub fn read_row(&self, offset_in_group: u64) -> Result<Vec<Value>, GqliteError> {
        if offset_in_group >= self.num_rows {
            return Err(GqliteError::Storage(format!(
                "offset {} out of range in group {} (num_rows={})",
                offset_in_group, self.group_idx, self.num_rows
            )));
        }
        let chunk_idx = (offset_in_group / CHUNK_CAPACITY as u64) as usize;
        let row_in_chunk = offset_in_group % CHUNK_CAPACITY as u64;
        self.chunks[chunk_idx].read_row(row_in_chunk)
    }

    /// Update a single cell.
    pub fn set_value(
        &mut self,
        offset_in_group: u64,
        col_idx: usize,
        value: &Value,
    ) -> Result<(), GqliteError> {
        if offset_in_group >= self.num_rows {
            return Err(GqliteError::Storage("offset out of range".into()));
        }
        let chunk_idx = (offset_in_group / CHUNK_CAPACITY as u64) as usize;
        let row_in_chunk = offset_in_group % CHUNK_CAPACITY as u64;
        self.chunks[chunk_idx].set_value(row_in_chunk, col_idx, value);
        Ok(())
    }

    pub fn num_rows(&self) -> u64 {
        self.num_rows
    }

    pub fn is_full(&self) -> bool {
        self.num_rows >= NODE_GROUP_SIZE as u64
    }

    pub fn group_idx(&self) -> u32 {
        self.group_idx
    }

    pub fn chunks(&self) -> &[ChunkedNodeGroup] {
        &self.chunks
    }

    /// Add a new column to all existing chunks (NULL-filled).
    pub fn add_column(&mut self, data_type: &DataType) {
        self.data_types.push(data_type.clone());
        for chunk in &mut self.chunks {
            chunk.add_column(data_type);
        }
    }

    /// Remove a column from all existing chunks.
    pub fn drop_column(&mut self, col_idx: usize) {
        if col_idx < self.data_types.len() {
            self.data_types.remove(col_idx);
            for chunk in &mut self.chunks {
                chunk.drop_column(col_idx);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_data_types() -> Vec<DataType> {
        vec![DataType::Int64, DataType::String]
    }

    #[test]
    fn append_and_read() {
        let mut ng = NodeGroup::new(0, test_data_types());
        for i in 0..10 {
            let row = vec![
                Value::Int(i),
                Value::String(format!("name_{}", i)),
            ];
            let off = ng.append_row(&row).unwrap();
            assert_eq!(off, i as u64);
        }
        assert_eq!(ng.num_rows(), 10);

        for i in 0..10 {
            let row = ng.read_row(i as u64).unwrap();
            assert_eq!(row[0], Value::Int(i));
            assert_eq!(row[1], Value::String(format!("name_{}", i)));
        }
    }

    #[test]
    fn cross_chunk_boundary() {
        let mut ng = NodeGroup::new(0, vec![DataType::Int64]);
        // Write 4096 rows — crosses 2 chunks of 2048 each
        for i in 0..4096u64 {
            ng.append_row(&[Value::Int(i as i64)]).unwrap();
        }
        assert_eq!(ng.num_rows(), 4096);
        assert_eq!(ng.chunks().len(), 2);

        // Check boundary values
        assert_eq!(ng.read_row(0).unwrap()[0], Value::Int(0));
        assert_eq!(ng.read_row(2047).unwrap()[0], Value::Int(2047));
        assert_eq!(ng.read_row(2048).unwrap()[0], Value::Int(2048));
        assert_eq!(ng.read_row(4095).unwrap()[0], Value::Int(4095));
    }

    #[test]
    fn locate() {
        assert_eq!(NodeGroup::locate(0), (0, 0));
        assert_eq!(
            NodeGroup::locate(NODE_GROUP_SIZE as u64 - 1),
            (0, NODE_GROUP_SIZE as u64 - 1)
        );
        assert_eq!(NodeGroup::locate(NODE_GROUP_SIZE as u64), (1, 0));
        assert_eq!(NodeGroup::locate(NODE_GROUP_SIZE as u64 + 5), (1, 5));
    }

    #[test]
    fn update_cell() {
        let mut ng = NodeGroup::new(0, vec![DataType::Int64, DataType::String]);
        ng.append_row(&[Value::Int(1), Value::String("a".into())])
            .unwrap();
        ng.append_row(&[Value::Int(2), Value::String("b".into())])
            .unwrap();

        ng.set_value(0, 1, &Value::String("updated".into()))
            .unwrap();
        let row = ng.read_row(0).unwrap();
        assert_eq!(row[1], Value::String("updated".into()));
    }

    #[test]
    fn is_full_check() {
        // Use a small NodeGroup size concept — we can't fill 131072 in a test easily,
        // but we can verify the check
        let ng = NodeGroup::new(0, vec![DataType::Int64]);
        assert!(!ng.is_full());
    }
}
