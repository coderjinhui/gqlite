//! Write-set based transaction buffering.
//!
//! All write operations during a transaction are buffered in a `WriteSet`.
//! On commit, the write set is flushed to WAL and then applied to storage.
//! On rollback (or drop), the write set is simply discarded.

use crate::catalog::{Catalog, ColumnDef};
use crate::error::GqliteError;
use crate::storage::table::{NodeTable, RelTable};
use crate::transaction::wal::{WalPayload, WalRecord, WalWriter};
use crate::types::data_type::DataType;
use crate::types::graph::InternalId;
use crate::types::value::Value;
use crate::Storage;
use std::sync::RwLock;

use parking_lot::Mutex;

/// A buffered DDL operation.
#[derive(Debug, Clone)]
pub enum DdlOp {
    CreateNodeTable {
        name: String,
        columns: Vec<(String, DataType)>,
        primary_key: String,
    },
    CreateRelTable {
        name: String,
        from_table: String,
        to_table: String,
        columns: Vec<(String, DataType)>,
    },
    DropTable {
        name: String,
    },
    AlterTableAddColumn {
        table_name: String,
        col_name: String,
        data_type: DataType,
    },
    AlterTableDropColumn {
        table_name: String,
        col_name: String,
    },
    AlterTableRenameTable {
        old_name: String,
        new_name: String,
    },
    AlterTableRenameColumn {
        table_name: String,
        old_col: String,
        new_col: String,
    },
}

/// Buffers all write operations during a transaction.
///
/// On commit, operations are flushed to WAL and applied to storage atomically.
/// On rollback (or drop without commit), all changes are discarded.
#[derive(Debug)]
pub struct WriteSet {
    /// DDL operations in execution order.
    pub ddl_ops: Vec<DdlOp>,
    /// Node insertions: (table_name, table_id, values).
    pub node_inserts: Vec<(String, u32, Vec<Value>)>,
    /// Node updates: (table_id, node_offset, col_idx, new_value).
    pub node_updates: Vec<(u32, u64, usize, Value)>,
    /// Node deletions: (table_id, node_offset).
    pub node_deletes: Vec<(u32, u64)>,
    /// Relationship insertions: (rel_table_name, rel_table_id, src, dst, properties).
    pub rel_inserts: Vec<(String, u32, InternalId, InternalId, Vec<Value>)>,
}

impl WriteSet {
    /// Create a new empty write set.
    pub fn new() -> Self {
        Self {
            ddl_ops: Vec::new(),
            node_inserts: Vec::new(),
            node_updates: Vec::new(),
            node_deletes: Vec::new(),
            rel_inserts: Vec::new(),
        }
    }

    /// Whether this write set has no buffered operations.
    pub fn is_empty(&self) -> bool {
        self.ddl_ops.is_empty()
            && self.node_inserts.is_empty()
            && self.node_updates.is_empty()
            && self.node_deletes.is_empty()
            && self.rel_inserts.is_empty()
    }

    /// Buffer a node insertion.
    pub fn add_insert_node(&mut self, table_name: String, table_id: u32, values: Vec<Value>) {
        self.node_inserts.push((table_name, table_id, values));
    }

    /// Buffer a node property update.
    pub fn add_update(&mut self, table_id: u32, offset: u64, col_idx: usize, value: Value) {
        self.node_updates.push((table_id, offset, col_idx, value));
    }

    /// Buffer a node deletion.
    pub fn add_delete(&mut self, table_id: u32, offset: u64) {
        self.node_deletes.push((table_id, offset));
    }

    /// Buffer a relationship insertion.
    pub fn add_insert_rel(
        &mut self,
        rel_table_name: String,
        rel_table_id: u32,
        src: InternalId,
        dst: InternalId,
        properties: Vec<Value>,
    ) {
        self.rel_inserts.push((rel_table_name, rel_table_id, src, dst, properties));
    }

    /// Buffer a DDL operation.
    pub fn add_ddl(&mut self, op: DdlOp) {
        self.ddl_ops.push(op);
    }

    /// Commit the write set: flush all operations to WAL, then apply to storage.
    ///
    /// This is the atomic boundary — if this method succeeds, the transaction is
    /// committed. If it fails at any point, the caller should rollback.
    pub fn commit(
        &self,
        txn_id: u64,
        wal: &Mutex<Option<WalWriter>>,
        catalog: &RwLock<Catalog>,
        storage: &RwLock<Storage>,
    ) -> Result<(), GqliteError> {
        // Phase 1: Write all WAL records
        self.write_wal(txn_id, wal)?;

        // Phase 2: Apply to catalog and storage
        self.apply(txn_id, catalog, storage)?;

        Ok(())
    }

    /// Write all buffered operations to WAL, including TxnCommit.
    fn write_wal(&self, txn_id: u64, wal: &Mutex<Option<WalWriter>>) -> Result<(), GqliteError> {
        let mut wal_guard = wal.lock();
        let wal = match wal_guard.as_mut() {
            Some(w) => w,
            None => return Ok(()), // in-memory database
        };

        // DDL operations
        for op in &self.ddl_ops {
            let payload = match op {
                DdlOp::CreateNodeTable { name, columns, primary_key } => {
                    WalPayload::CreateNodeTable {
                        name: name.clone(),
                        columns: columns.clone(),
                        primary_key: primary_key.clone(),
                    }
                }
                DdlOp::CreateRelTable { name, from_table, to_table, columns } => {
                    WalPayload::CreateRelTable {
                        name: name.clone(),
                        from_table: from_table.clone(),
                        to_table: to_table.clone(),
                        columns: columns.clone(),
                    }
                }
                DdlOp::DropTable { name } => WalPayload::DropTable { name: name.clone() },
                DdlOp::AlterTableAddColumn { table_name, col_name, data_type } => {
                    WalPayload::AlterTableAddColumn {
                        table_name: table_name.clone(),
                        col_name: col_name.clone(),
                        data_type: data_type.clone(),
                    }
                }
                DdlOp::AlterTableDropColumn { table_name, col_name } => {
                    WalPayload::AlterTableDropColumn {
                        table_name: table_name.clone(),
                        col_name: col_name.clone(),
                    }
                }
                DdlOp::AlterTableRenameTable { old_name, new_name } => {
                    WalPayload::AlterTableRenameTable {
                        old_name: old_name.clone(),
                        new_name: new_name.clone(),
                    }
                }
                DdlOp::AlterTableRenameColumn { table_name, old_col, new_col } => {
                    WalPayload::AlterTableRenameColumn {
                        table_name: table_name.clone(),
                        old_col: old_col.clone(),
                        new_col: new_col.clone(),
                    }
                }
            };
            wal.append(&WalRecord { txn_id, payload })?;
        }

        // Node insertions
        for (table_name, table_id, values) in &self.node_inserts {
            wal.append(&WalRecord {
                txn_id,
                payload: WalPayload::InsertNode {
                    table_name: table_name.clone(),
                    table_id: *table_id,
                    values: values.clone(),
                },
            })?;
        }

        // Node updates
        for (table_id, node_offset, col_idx, new_value) in &self.node_updates {
            wal.append(&WalRecord {
                txn_id,
                payload: WalPayload::UpdateProperty {
                    table_id: *table_id,
                    node_offset: *node_offset,
                    col_idx: *col_idx,
                    new_value: new_value.clone(),
                },
            })?;
        }

        // Node deletions
        for (table_id, node_offset) in &self.node_deletes {
            wal.append(&WalRecord {
                txn_id,
                payload: WalPayload::DeleteNode { table_id: *table_id, node_offset: *node_offset },
            })?;
        }

        // Relationship insertions
        for (rel_table_name, rel_table_id, src, dst, properties) in &self.rel_inserts {
            wal.append(&WalRecord {
                txn_id,
                payload: WalPayload::InsertRel {
                    rel_table_name: rel_table_name.clone(),
                    rel_table_id: *rel_table_id,
                    src: *src,
                    dst: *dst,
                    properties: properties.clone(),
                },
            })?;
        }

        // TxnCommit marker
        wal.append(&WalRecord { txn_id, payload: WalPayload::TxnCommit })?;

        Ok(())
    }

    /// Apply all buffered operations to catalog and storage.
    fn apply(
        &self,
        txn_id: u64,
        catalog_lock: &RwLock<Catalog>,
        storage_lock: &RwLock<Storage>,
    ) -> Result<(), GqliteError> {
        // Apply DDL operations
        if !self.ddl_ops.is_empty() {
            let mut catalog = catalog_lock.write().unwrap();
            let mut storage = storage_lock.write().unwrap();
            for op in &self.ddl_ops {
                apply_ddl_op(op, &mut catalog, &mut storage)?;
            }
        }

        // Apply DML operations
        if !self.node_inserts.is_empty()
            || !self.node_updates.is_empty()
            || !self.node_deletes.is_empty()
            || !self.rel_inserts.is_empty()
        {
            let mut storage = storage_lock.write().unwrap();

            // Inserts
            for (_table_name, table_id, values) in &self.node_inserts {
                if let Some(node_table) = storage.node_tables.get_mut(table_id) {
                    node_table.insert(values, txn_id)?;
                }
            }

            // Updates
            for (table_id, offset, col_idx, value) in &self.node_updates {
                if let Some(node_table) = storage.node_tables.get_mut(table_id) {
                    node_table.update(*offset, *col_idx, value.clone(), txn_id)?;
                }
            }

            // Deletes
            for (table_id, offset) in &self.node_deletes {
                if let Some(node_table) = storage.node_tables.get_mut(table_id) {
                    node_table.delete(*offset, txn_id)?;
                }
            }

            // Relationship inserts
            let mut rel_tables_to_compact: Vec<u32> = Vec::new();
            for (_name, rel_table_id, src, dst, props) in &self.rel_inserts {
                if let Some(rel_table) = storage.rel_tables.get_mut(rel_table_id) {
                    rel_table.insert_rel(*src, *dst, props)?;
                    if !rel_tables_to_compact.contains(rel_table_id) {
                        rel_tables_to_compact.push(*rel_table_id);
                    }
                }
            }
            for rel_table_id in &rel_tables_to_compact {
                if let Some(rel_table) = storage.rel_tables.get_mut(rel_table_id) {
                    rel_table.compact();
                }
            }
        }

        Ok(())
    }
}

impl Default for WriteSet {
    fn default() -> Self {
        Self::new()
    }
}

/// Apply a single DDL operation to catalog and storage.
/// Mirrors the exact logic in `Engine::exec_create_node_table` etc.
fn apply_ddl_op(
    op: &DdlOp,
    catalog: &mut Catalog,
    storage: &mut Storage,
) -> Result<(), GqliteError> {
    match op {
        DdlOp::CreateNodeTable { name, columns, primary_key } => {
            let col_defs: Vec<ColumnDef> = columns
                .iter()
                .enumerate()
                .map(|(i, (cname, dtype))| ColumnDef {
                    column_id: i as u32,
                    name: cname.clone(),
                    data_type: dtype.clone(),
                    nullable: cname != primary_key,
                })
                .collect();
            let table_id = catalog.create_node_table(name, col_defs, primary_key)?;
            let entry = catalog.get_node_table(name).unwrap().clone();
            storage.node_tables.insert(table_id, NodeTable::new(&entry));
            Ok(())
        }
        DdlOp::CreateRelTable { name, from_table, to_table, columns } => {
            let col_defs: Vec<ColumnDef> = columns
                .iter()
                .enumerate()
                .map(|(i, (cname, dtype))| ColumnDef {
                    column_id: i as u32,
                    name: cname.clone(),
                    data_type: dtype.clone(),
                    nullable: true,
                })
                .collect();
            let table_id = catalog.create_rel_table(name, from_table, to_table, col_defs)?;
            let entry = catalog.get_rel_table(name).unwrap().clone();
            storage.rel_tables.insert(table_id, RelTable::new(&entry));
            Ok(())
        }
        DdlOp::DropTable { name } => {
            let table_id = catalog
                .get_node_table(name)
                .map(|e| e.table_id)
                .or_else(|| catalog.get_rel_table(name).map(|e| e.table_id));
            catalog.drop_table(name)?;
            if let Some(id) = table_id {
                storage.node_tables.remove(&id);
                storage.rel_tables.remove(&id);
            }
            Ok(())
        }
        DdlOp::AlterTableAddColumn { table_name, col_name, data_type } => {
            let is_node = catalog.get_node_table(table_name).is_some();
            let col_id = if is_node {
                catalog.get_node_table(table_name).unwrap().columns.len() as u32
            } else {
                catalog.get_rel_table(table_name).unwrap().columns.len() as u32
            };
            let col_def = ColumnDef {
                column_id: col_id,
                name: col_name.clone(),
                data_type: data_type.clone(),
                nullable: true,
            };
            if is_node {
                catalog.add_column_to_node_table(table_name, col_def)?;
                let table_id = catalog.get_node_table(table_name).unwrap().table_id;
                if let Some(node_table) = storage.node_tables.get_mut(&table_id) {
                    node_table.add_column(data_type);
                }
            } else {
                catalog.add_column_to_rel_table(table_name, col_def)?;
            }
            Ok(())
        }
        DdlOp::AlterTableDropColumn { table_name, col_name } => {
            let is_node = catalog.get_node_table(table_name).is_some();
            if is_node {
                let col_idx = catalog
                    .get_node_table(table_name)
                    .unwrap()
                    .columns
                    .iter()
                    .position(|c| c.name == *col_name);
                catalog.drop_column_from_node_table(table_name, col_name)?;
                if let Some(idx) = col_idx {
                    let table_id = catalog.get_node_table(table_name).unwrap().table_id;
                    if let Some(node_table) = storage.node_tables.get_mut(&table_id) {
                        node_table.drop_column(idx);
                    }
                }
            } else {
                catalog.drop_column_from_rel_table(table_name, col_name)?;
            }
            Ok(())
        }
        DdlOp::AlterTableRenameTable { old_name, new_name } => {
            catalog.rename_table(old_name, new_name)?;
            Ok(())
        }
        DdlOp::AlterTableRenameColumn { table_name, old_col, new_col } => {
            let is_node = catalog.get_node_table(table_name).is_some();
            if is_node {
                catalog.rename_column_in_node_table(table_name, old_col, new_col)?;
            } else {
                catalog.rename_column_in_rel_table(table_name, old_col, new_col)?;
            }
            Ok(())
        }
    }
}
