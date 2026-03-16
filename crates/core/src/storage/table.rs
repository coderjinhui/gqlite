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
    /// MVCC: transaction ID that created each row (indexed by offset).
    create_ts: Vec<u64>,
    /// MVCC: transaction ID that deleted each row (0 = not deleted).
    delete_ts: Vec<u64>,
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
            create_ts: Vec::new(),
            delete_ts: Vec::new(),
        }
    }

    /// Insert a row. Returns the InternalId assigned.
    ///
    /// `txn_id` is the MVCC create timestamp; pass 0 for legacy/non-MVCC callers.
    pub fn insert(&mut self, values: &[Value], txn_id: u64) -> Result<InternalId, GqliteError> {
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

        // MVCC version metadata
        self.create_ts.push(txn_id);
        self.delete_ts.push(0);

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

    /// Delete a row by setting delete_ts (MVCC soft-delete) and removing from PK index.
    pub fn delete(&mut self, offset: u64, txn_id: u64) -> Result<(), GqliteError> {
        // Read current row to get PK value
        let row = self.read(offset)?;
        let pk_val = &row[self.pk_col_idx];
        if !pk_val.is_null() {
            self.pk_index.remove(pk_val);
        }

        // Set MVCC delete timestamp
        if (offset as usize) < self.delete_ts.len() {
            self.delete_ts[offset as usize] = txn_id;
        }

        // Null out all columns (physical delete for backward compatibility)
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
    /// Uses legacy visibility: skips rows where PK is null (physically deleted).
    pub fn scan(&self) -> NodeTableIter<'_> {
        NodeTableIter {
            table: self,
            current_offset: 0,
            start_ts: None,
        }
    }

    /// Scan all rows visible to a given snapshot timestamp (MVCC).
    ///
    /// A row is visible if:
    /// - `create_ts <= start_ts` (committed before this read started)
    /// - `delete_ts == 0 || delete_ts > start_ts` (not yet deleted, or deleted after snapshot)
    pub fn scan_mvcc(&self, start_ts: u64) -> NodeTableIter<'_> {
        NodeTableIter {
            table: self,
            current_offset: 0,
            start_ts: Some(start_ts),
        }
    }

    /// Check if a row at the given offset is visible to the given snapshot.
    pub fn is_visible(&self, offset: u64, start_ts: u64) -> bool {
        let idx = offset as usize;
        if idx >= self.create_ts.len() {
            return false;
        }
        let ct = self.create_ts[idx];
        let dt = self.delete_ts[idx];
        ct <= start_ts && (dt == 0 || dt > start_ts)
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

    /// Add a new column to the table (NULL-filled for existing rows).
    pub fn add_column(&mut self, data_type: &DataType) {
        self.schema.push(("".to_string(), data_type.clone()));
        for group in &mut self.node_groups {
            group.add_column(data_type);
        }
    }

    /// Drop a column by index from the table.
    pub fn drop_column(&mut self, col_idx: usize) {
        if col_idx < self.schema.len() {
            self.schema.remove(col_idx);
            for group in &mut self.node_groups {
                group.drop_column(col_idx);
            }
            // Adjust pk_col_idx if needed
            if col_idx < self.pk_col_idx {
                self.pk_col_idx -= 1;
            }
        }
    }

    /// Garbage-collect old versions that are no longer visible to any active transaction.
    ///
    /// `safe_ts` is the minimum snapshot timestamp of all active read transactions.
    /// Rows with `delete_ts != 0 && delete_ts <= safe_ts` can be safely purged
    /// since no active reader can see them.
    ///
    /// Returns the number of rows purged.
    pub fn gc(&mut self, safe_ts: u64) -> u64 {
        let mut purged = 0u64;
        for offset in 0..self.next_offset {
            let idx = offset as usize;
            if idx >= self.delete_ts.len() {
                break;
            }
            let dt = self.delete_ts[idx];
            if dt != 0 && dt <= safe_ts {
                // This row was deleted at or before safe_ts — no reader can see it.
                // Already physically deleted (columns nulled) in delete(), just
                // mark as fully purged by zeroing create_ts too.
                self.create_ts[idx] = 0;
                self.delete_ts[idx] = 0;
                purged += 1;
            }
        }
        purged
    }
}

/// Iterator over all rows in a NodeTable.
pub struct NodeTableIter<'a> {
    table: &'a NodeTable,
    current_offset: u64,
    /// If Some, use MVCC visibility; if None, use legacy PK-null check.
    start_ts: Option<u64>,
}

impl<'a> Iterator for NodeTableIter<'a> {
    type Item = (u64, Vec<Value>);

    fn next(&mut self) -> Option<Self::Item> {
        while self.current_offset < self.table.next_offset {
            let offset = self.current_offset;
            self.current_offset += 1;

            match self.start_ts {
                Some(ts) => {
                    // MVCC visibility check
                    if !self.table.is_visible(offset, ts) {
                        continue;
                    }
                    if let Ok(row) = self.table.read(offset) {
                        return Some((offset, row));
                    }
                }
                None => {
                    // Legacy: skip deleted rows (all NULL check on PK)
                    if let Ok(row) = self.table.read(offset) {
                        if !row[self.table.pk_col_idx].is_null() {
                            return Some((offset, row));
                        }
                    }
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
            next_serial: 0,
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
            ], 1)
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
            .insert(&[Value::Int(1), Value::String("A".into()), Value::Int(20)], 1)
            .unwrap();
        let result = table.insert(&[Value::Int(1), Value::String("B".into()), Value::Int(25)], 1);
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
                ], 1)
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
            .insert(&[Value::Int(1), Value::String("A".into()), Value::Int(20)], 1)
            .unwrap();
        table
            .insert(&[Value::Int(2), Value::String("B".into()), Value::Int(25)], 1)
            .unwrap();

        table.delete(0, 2).unwrap();

        // PK 1 should be gone from index
        assert!(table.lookup_pk(&Value::Int(1)).is_none());
        assert_eq!(table.lookup_pk(&Value::Int(2)), Some(1));

        // Scan should skip deleted rows (legacy scan uses PK-null check)
        let rows: Vec<_> = table.scan().collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1[0], Value::Int(2));
    }

    #[test]
    fn update_column() {
        let mut table = NodeTable::new(&person_entry());
        table
            .insert(&[Value::Int(1), Value::String("A".into()), Value::Int(20)], 1)
            .unwrap();

        table.update(0, 1, Value::String("Updated".into())).unwrap();
        let row = table.read(0).unwrap();
        assert_eq!(row[1], Value::String("Updated".into()));
    }

    #[test]
    fn pk_lookup() {
        let mut table = NodeTable::new(&person_entry());
        table
            .insert(&[Value::Int(42), Value::String("X".into()), Value::Int(99)], 1)
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

    // ── MVCC tests ──

    #[test]
    fn mvcc_scan_visibility() {
        let mut table = NodeTable::new(&person_entry());
        // txn 1 inserts Alice
        table.insert(&[Value::Int(1), Value::String("Alice".into()), Value::Int(30)], 1).unwrap();
        // txn 3 inserts Bob (not yet committed from snapshot 2's perspective)
        table.insert(&[Value::Int(2), Value::String("Bob".into()), Value::Int(25)], 3).unwrap();

        // Snapshot at ts=2: should see Alice (create_ts=1 <= 2) but NOT Bob (create_ts=3 > 2)
        let rows: Vec<_> = table.scan_mvcc(2).collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1[1], Value::String("Alice".into()));

        // Snapshot at ts=3: should see both
        let rows: Vec<_> = table.scan_mvcc(3).collect();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn mvcc_delete_visibility() {
        let mut table = NodeTable::new(&person_entry());
        // txn 1 inserts Alice and Bob
        table.insert(&[Value::Int(1), Value::String("Alice".into()), Value::Int(30)], 1).unwrap();
        table.insert(&[Value::Int(2), Value::String("Bob".into()), Value::Int(25)], 1).unwrap();

        // txn 3 deletes Alice
        table.delete(0, 3).unwrap();

        // Snapshot at ts=2: sees both (delete at ts=3 > 2, so Alice still visible)
        let rows: Vec<_> = table.scan_mvcc(2).collect();
        assert_eq!(rows.len(), 2);

        // Snapshot at ts=3: delete_ts=3 is NOT > 3, so Alice is invisible
        let rows: Vec<_> = table.scan_mvcc(3).collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1[1], Value::String("Bob".into()));
    }

    #[test]
    fn mvcc_gc_purges_old_versions() {
        let mut table = NodeTable::new(&person_entry());
        table.insert(&[Value::Int(1), Value::String("Alice".into()), Value::Int(30)], 1).unwrap();
        table.insert(&[Value::Int(2), Value::String("Bob".into()), Value::Int(25)], 1).unwrap();

        // Delete Alice at txn 2
        table.delete(0, 2).unwrap();

        // GC with safe_ts=1: delete at ts=2 > 1, not safe to purge
        let purged = table.gc(1);
        assert_eq!(purged, 0);

        // GC with safe_ts=3: delete at ts=2 <= 3, safe to purge
        let purged = table.gc(3);
        assert_eq!(purged, 1);
    }

    #[test]
    fn mvcc_is_visible() {
        let mut table = NodeTable::new(&person_entry());
        table.insert(&[Value::Int(1), Value::String("A".into()), Value::Int(20)], 5).unwrap();

        assert!(!table.is_visible(0, 4)); // create_ts=5 > 4
        assert!(table.is_visible(0, 5));  // create_ts=5 <= 5
        assert!(table.is_visible(0, 10)); // create_ts=5 <= 10

        table.delete(0, 8).unwrap();
        assert!(table.is_visible(0, 7));   // delete_ts=8 > 7
        assert!(!table.is_visible(0, 8));  // delete_ts=8 is NOT > 8
        assert!(!table.is_visible(0, 10)); // delete_ts=8 <= 10
    }
}
