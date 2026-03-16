use std::collections::HashMap;

use serde::{Deserialize, Serialize};

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
#[derive(Serialize, Deserialize)]
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
#[derive(Serialize, Deserialize)]
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
    /// Per-relationship property storage: rel_id -> property values (aligned with schema).
    #[serde(default)]
    rel_properties: HashMap<u64, Vec<Value>>,
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
            rel_properties: HashMap::new(),
        }
    }

    /// Insert a relationship. Returns the assigned rel_id.
    pub fn insert_rel(
        &mut self,
        src: InternalId,
        dst: InternalId,
        props: &[Value],
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
            properties: vec![], // CSR doesn't use properties directly
        });

        // Ensure BWD group exists
        self.ensure_bwd_group(dst_group_idx as usize);
        self.bwd_groups[dst_group_idx as usize].insert_edge(PendingEdge {
            src_offset: dst_offset_in_group,
            dst_offset: src.offset,
            rel_id,
            properties: vec![],
        });

        // Store rel properties if any were provided
        if !props.is_empty() {
            self.rel_properties.insert(rel_id, props.to_vec());
        }

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

    /// Get the schema (column name, type) for relationship properties.
    pub fn schema(&self) -> &[(String, DataType)] {
        &self.schema
    }

    /// Read a property value for a given rel_id by column name.
    /// Returns None if the rel_id has no stored properties or the column is not found.
    pub fn get_rel_property(&self, rel_id: u64, col_name: &str) -> Option<Value> {
        let col_idx = self.schema.iter().position(|(name, _)| name == col_name)?;
        let props = self.rel_properties.get(&rel_id)?;
        props.get(col_idx).cloned()
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
