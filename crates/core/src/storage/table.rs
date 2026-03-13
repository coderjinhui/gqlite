use std::collections::HashMap;

use crate::catalog::{NodeTableEntry, RelTableEntry};
use crate::error::GqliteError;
use crate::storage::csr::{CSRNodeGroup, PendingEdge};
use crate::storage::format::NODE_GROUP_SIZE;
use crate::storage::node_group::NodeGroup;
use crate::types::data_type::DataType;
use crate::types::graph::InternalId;
use crate::types::value::Value;

// ── NodeTable ──────────────────────────────────────────────────────

/// Manages all rows for a single node table, organized into NodeGroups.
pub struct NodeTable {
    table_id: u32,
    schema: Vec<(String, DataType)>,
    node_groups: Vec<NodeGroup>,
    next_offset: u64,
    /// Primary key → global offset mapping for uniqueness enforcement.
    pk_index: HashMap<Value, u64>,
    pk_col_idx: usize,
}

impl NodeTable {
    pub fn new(entry: &NodeTableEntry) -> Self {
        let schema: Vec<(String, DataType)> = entry
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.data_type.clone()))
            .collect();
        Self {
            table_id: entry.table_id,
            schema,
            node_groups: Vec::new(),
            next_offset: 0,
            pk_index: HashMap::new(),
            pk_col_idx: entry.primary_key_idx,
        }
    }

    /// Insert a row. Returns the InternalId assigned.
    pub fn insert(&mut self, values: &[Value]) -> Result<InternalId, GqliteError> {
        if values.len() != self.schema.len() {
            return Err(GqliteError::Storage(format!(
                "expected {} columns, got {}",
                self.schema.len(),
                values.len()
            )));
        }

        // Check primary key uniqueness
        let pk_val = &values[self.pk_col_idx];
        if !pk_val.is_null() && self.pk_index.contains_key(pk_val) {
            return Err(GqliteError::Storage(format!(
                "duplicate primary key: {}",
                pk_val
            )));
        }

        // Find or create a NodeGroup
        let data_types: Vec<DataType> = self.schema.iter().map(|(_, dt)| dt.clone()).collect();
        let need_new = self.node_groups.is_empty() || self.node_groups.last().unwrap().is_full();
        if need_new {
            let group_idx = self.node_groups.len() as u32;
            self.node_groups.push(NodeGroup::new(group_idx, data_types));
        }

        let group = self.node_groups.last_mut().unwrap();
        group.append_row(values)?;

        let offset = self.next_offset;
        self.next_offset += 1;

        // Update PK index
        if !pk_val.is_null() {
            self.pk_index.insert(pk_val.clone(), offset);
        }

        Ok(InternalId::new(self.table_id, offset))
    }

    /// Read a row by its global offset.
    pub fn read(&self, offset: u64) -> Result<Vec<Value>, GqliteError> {
        let (group_idx, offset_in_group) = NodeGroup::locate(offset);
        let group = self.node_groups.get(group_idx as usize).ok_or_else(|| {
            GqliteError::Storage(format!("group {} not found", group_idx))
        })?;
        group.read_row(offset_in_group)
    }

    /// Delete a row by setting it to all NULLs and removing from PK index.
    pub fn delete(&mut self, offset: u64) -> Result<(), GqliteError> {
        // Read current row to get PK value
        let row = self.read(offset)?;
        let pk_val = &row[self.pk_col_idx];
        if !pk_val.is_null() {
            self.pk_index.remove(pk_val);
        }

        // Null out all columns
        let (group_idx, offset_in_group) = NodeGroup::locate(offset);
        let group = self.node_groups.get_mut(group_idx as usize).ok_or_else(|| {
            GqliteError::Storage(format!("group {} not found", group_idx))
        })?;
        for col_idx in 0..self.schema.len() {
            group.set_value(offset_in_group, col_idx, &Value::Null)?;
        }
        Ok(())
    }

    /// Update a single column value.
    pub fn update(
        &mut self,
        offset: u64,
        col_idx: usize,
        value: Value,
    ) -> Result<(), GqliteError> {
        // If updating the PK column, handle index
        if col_idx == self.pk_col_idx {
            let old_row = self.read(offset)?;
            let old_pk = &old_row[self.pk_col_idx];
            if !old_pk.is_null() {
                self.pk_index.remove(old_pk);
            }
            if !value.is_null() {
                if self.pk_index.contains_key(&value) {
                    return Err(GqliteError::Storage(format!(
                        "duplicate primary key: {}",
                        value
                    )));
                }
                self.pk_index.insert(value.clone(), offset);
            }
        }

        let (group_idx, offset_in_group) = NodeGroup::locate(offset);
        let group = self.node_groups.get_mut(group_idx as usize).ok_or_else(|| {
            GqliteError::Storage(format!("group {} not found", group_idx))
        })?;
        group.set_value(offset_in_group, col_idx, &value)
    }

    /// Scan all rows. Returns an iterator of (offset, row_values).
    pub fn scan(&self) -> NodeTableIter<'_> {
        NodeTableIter {
            table: self,
            current_offset: 0,
        }
    }

    pub fn row_count(&self) -> u64 {
        self.next_offset
    }

    pub fn table_id(&self) -> u32 {
        self.table_id
    }

    pub fn schema(&self) -> &[(String, DataType)] {
        &self.schema
    }

    /// Look up a global offset by primary key value.
    pub fn lookup_pk(&self, pk: &Value) -> Option<u64> {
        self.pk_index.get(pk).copied()
    }
}

/// Iterator over all rows in a NodeTable.
pub struct NodeTableIter<'a> {
    table: &'a NodeTable,
    current_offset: u64,
}

impl<'a> Iterator for NodeTableIter<'a> {
    type Item = (u64, Vec<Value>);

    fn next(&mut self) -> Option<Self::Item> {
        while self.current_offset < self.table.next_offset {
            let offset = self.current_offset;
            self.current_offset += 1;
            if let Ok(row) = self.table.read(offset) {
                // Skip deleted rows (all NULL check on PK)
                if !row[self.table.pk_col_idx].is_null() {
                    return Some((offset, row));
                }
            }
        }
        None
    }
}

// ── RelTable ───────────────────────────────────────────────────────

/// Manages relationship storage using CSR format with bidirectional indexing.
#[allow(dead_code)]
pub struct RelTable {
    table_id: u32,
    src_table_id: u32,
    dst_table_id: u32,
    schema: Vec<(String, DataType)>,
    /// Forward CSR groups, indexed by source node group.
    fwd_groups: Vec<CSRNodeGroup>,
    /// Backward CSR groups, indexed by destination node group.
    bwd_groups: Vec<CSRNodeGroup>,
    next_rel_id: u64,
}

impl RelTable {
    pub fn new(entry: &RelTableEntry) -> Self {
        let schema: Vec<(String, DataType)> = entry
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.data_type.clone()))
            .collect();
        Self {
            table_id: entry.table_id,
            src_table_id: entry.src_table_id,
            dst_table_id: entry.dst_table_id,
            schema,
            fwd_groups: Vec::new(),
            bwd_groups: Vec::new(),
            next_rel_id: 0,
        }
    }

    /// Insert a relationship. Returns the assigned rel_id.
    pub fn insert_rel(
        &mut self,
        src: InternalId,
        dst: InternalId,
        _props: &[Value],
    ) -> Result<u64, GqliteError> {
        let rel_id = self.next_rel_id;
        self.next_rel_id += 1;

        let (src_group_idx, src_offset_in_group) = NodeGroup::locate(src.offset);
        let (dst_group_idx, dst_offset_in_group) = NodeGroup::locate(dst.offset);

        // Ensure FWD group exists
        self.ensure_fwd_group(src_group_idx as usize);
        self.fwd_groups[src_group_idx as usize].insert_edge(PendingEdge {
            src_offset: src_offset_in_group,
            dst_offset: dst.offset,
            rel_id,
            properties: vec![], // TODO: store props in property columns
        });

        // Ensure BWD group exists
        self.ensure_bwd_group(dst_group_idx as usize);
        self.bwd_groups[dst_group_idx as usize].insert_edge(PendingEdge {
            src_offset: dst_offset_in_group,
            dst_offset: src.offset,
            rel_id,
            properties: vec![],
        });

        Ok(rel_id)
    }

    /// Get outgoing relationships from a source node.
    /// Returns Vec<(dst_offset, rel_id)>.
    pub fn get_rels_from(&self, src_offset: u64) -> Vec<(u64, u64)> {
        let (group_idx, offset_in_group) = NodeGroup::locate(src_offset);
        if let Some(csr) = self.fwd_groups.get(group_idx as usize) {
            let neighbors = csr.get_neighbors(offset_in_group);
            let rel_ids = csr.get_rel_ids(offset_in_group);
            neighbors
                .iter()
                .zip(rel_ids.iter())
                .map(|(&n, &r)| (n, r))
                .collect()
        } else {
            vec![]
        }
    }

    /// Get incoming relationships to a destination node.
    /// Returns Vec<(src_offset, rel_id)>.
    pub fn get_rels_to(&self, dst_offset: u64) -> Vec<(u64, u64)> {
        let (group_idx, offset_in_group) = NodeGroup::locate(dst_offset);
        if let Some(csr) = self.bwd_groups.get(group_idx as usize) {
            let neighbors = csr.get_neighbors(offset_in_group);
            let rel_ids = csr.get_rel_ids(offset_in_group);
            neighbors
                .iter()
                .zip(rel_ids.iter())
                .map(|(&n, &r)| (n, r))
                .collect()
        } else {
            vec![]
        }
    }

    /// Compact all pending inserts into the main CSR structures.
    pub fn compact(&mut self) {
        for csr in &mut self.fwd_groups {
            csr.compact();
        }
        for csr in &mut self.bwd_groups {
            csr.compact();
        }
    }

    pub fn rel_count(&self) -> u64 {
        self.next_rel_id
    }

    pub fn table_id(&self) -> u32 {
        self.table_id
    }

    pub fn src_table_id(&self) -> u32 {
        self.src_table_id
    }

    pub fn dst_table_id(&self) -> u32 {
        self.dst_table_id
    }

    /// Return all edges as (src_InternalId, dst_InternalId) pairs.
    /// Uses the forward CSR to enumerate all relationships.
    pub fn all_edges(&self) -> Vec<(InternalId, InternalId)> {
        let mut edges = Vec::new();
        for csr in &self.fwd_groups {
            for (src_offset, dst_offset, _rel_id) in csr.all_edges() {
                let src = InternalId::new(self.src_table_id, src_offset);
                let dst = InternalId::new(self.dst_table_id, dst_offset);
                edges.push((src, dst));
            }
        }
        edges
    }

    fn ensure_fwd_group(&mut self, group_idx: usize) {
        while self.fwd_groups.len() <= group_idx {
            let idx = self.fwd_groups.len() as u32;
            self.fwd_groups
                .push(CSRNodeGroup::new(idx, NODE_GROUP_SIZE));
        }
    }

    fn ensure_bwd_group(&mut self, group_idx: usize) {
        while self.bwd_groups.len() <= group_idx {
            let idx = self.bwd_groups.len() as u32;
            self.bwd_groups
                .push(CSRNodeGroup::new(idx, NODE_GROUP_SIZE));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ColumnDef;

    fn person_entry() -> NodeTableEntry {
        NodeTableEntry {
            table_id: 0,
            name: "Person".into(),
            columns: vec![
                ColumnDef {
                    column_id: 0,
                    name: "id".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnDef {
                    column_id: 1,
                    name: "name".into(),
                    data_type: DataType::String,
                    nullable: true,
                },
                ColumnDef {
                    column_id: 2,
                    name: "age".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                },
            ],
            primary_key_idx: 0,
            row_count: 0,
        }
    }

    fn knows_entry() -> RelTableEntry {
        RelTableEntry {
            table_id: 1,
            name: "KNOWS".into(),
            src_table_id: 0,
            dst_table_id: 0,
            columns: vec![],
            row_count: 0,
        }
    }

    // ── NodeTable tests ──

    #[test]
    fn insert_and_read() {
        let mut table = NodeTable::new(&person_entry());
        let id = table
            .insert(&[
                Value::Int(1),
                Value::String("Alice".into()),
                Value::Int(30),
            ])
            .unwrap();
        assert_eq!(id, InternalId::new(0, 0));

        let row = table.read(0).unwrap();
        assert_eq!(row[0], Value::Int(1));
        assert_eq!(row[1], Value::String("Alice".into()));
        assert_eq!(row[2], Value::Int(30));
    }

    #[test]
    fn duplicate_pk() {
        let mut table = NodeTable::new(&person_entry());
        table
            .insert(&[Value::Int(1), Value::String("A".into()), Value::Int(20)])
            .unwrap();
        let result = table.insert(&[Value::Int(1), Value::String("B".into()), Value::Int(25)]);
        assert!(result.is_err());
    }

    #[test]
    fn scan_all() {
        let mut table = NodeTable::new(&person_entry());
        for i in 0..5 {
            table
                .insert(&[
                    Value::Int(i),
                    Value::String(format!("p{}", i)),
                    Value::Int(20 + i),
                ])
                .unwrap();
        }

        let rows: Vec<_> = table.scan().collect();
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0].0, 0);
        assert_eq!(rows[4].0, 4);
    }

    #[test]
    fn delete_row() {
        let mut table = NodeTable::new(&person_entry());
        table
            .insert(&[Value::Int(1), Value::String("A".into()), Value::Int(20)])
            .unwrap();
        table
            .insert(&[Value::Int(2), Value::String("B".into()), Value::Int(25)])
            .unwrap();

        table.delete(0).unwrap();

        // PK 1 should be gone from index
        assert!(table.lookup_pk(&Value::Int(1)).is_none());
        assert_eq!(table.lookup_pk(&Value::Int(2)), Some(1));

        // Scan should skip deleted rows
        let rows: Vec<_> = table.scan().collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1[0], Value::Int(2));
    }

    #[test]
    fn update_column() {
        let mut table = NodeTable::new(&person_entry());
        table
            .insert(&[Value::Int(1), Value::String("A".into()), Value::Int(20)])
            .unwrap();

        table.update(0, 1, Value::String("Updated".into())).unwrap();
        let row = table.read(0).unwrap();
        assert_eq!(row[1], Value::String("Updated".into()));
    }

    #[test]
    fn pk_lookup() {
        let mut table = NodeTable::new(&person_entry());
        table
            .insert(&[Value::Int(42), Value::String("X".into()), Value::Int(99)])
            .unwrap();
        assert_eq!(table.lookup_pk(&Value::Int(42)), Some(0));
        assert_eq!(table.lookup_pk(&Value::Int(99)), None);
    }

    // ── RelTable tests ──

    #[test]
    fn insert_rel_and_query() {
        let mut rel = RelTable::new(&knows_entry());

        let src = InternalId::new(0, 0);
        let dst = InternalId::new(0, 1);
        let rel_id = rel.insert_rel(src, dst, &[]).unwrap();
        assert_eq!(rel_id, 0);

        // Must compact to populate main CSR
        rel.compact();

        let fwd = rel.get_rels_from(0);
        assert_eq!(fwd.len(), 1);
        assert_eq!(fwd[0], (1, 0));

        let bwd = rel.get_rels_to(1);
        assert_eq!(bwd.len(), 1);
        assert_eq!(bwd[0], (0, 0));
    }

    #[test]
    fn multiple_edges() {
        let mut rel = RelTable::new(&knows_entry());

        // 0→1, 0→2, 1→2
        rel.insert_rel(InternalId::new(0, 0), InternalId::new(0, 1), &[])
            .unwrap();
        rel.insert_rel(InternalId::new(0, 0), InternalId::new(0, 2), &[])
            .unwrap();
        rel.insert_rel(InternalId::new(0, 1), InternalId::new(0, 2), &[])
            .unwrap();

        rel.compact();

        let from_0 = rel.get_rels_from(0);
        assert_eq!(from_0.len(), 2);

        let from_1 = rel.get_rels_from(1);
        assert_eq!(from_1.len(), 1);

        let to_2 = rel.get_rels_to(2);
        assert_eq!(to_2.len(), 2);

        assert_eq!(rel.rel_count(), 3);
    }

    #[test]
    fn empty_query() {
        let rel = RelTable::new(&knows_entry());
        assert!(rel.get_rels_from(0).is_empty());
        assert!(rel.get_rels_to(0).is_empty());
    }
}
