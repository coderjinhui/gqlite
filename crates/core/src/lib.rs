//! gqlite-core — the core engine for gqlite, a lightweight embeddable graph database.
//!
//! This crate provides the storage engine, GQL parser, query planner,
//! executor, and catalog management for `.graph` files.

pub mod binder;
pub mod catalog;
pub mod error;
pub mod executor;
pub mod functions;
pub mod parser;
pub mod planner;
pub mod storage;
pub mod transaction;
pub mod types;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use parking_lot::Mutex;

use error::GqliteError;
use types::data_type::DataType;
use types::value::Value;

use binder::Binder;
use catalog::Catalog;
use executor::engine::Engine;
use parser::parser::Parser;
use planner::logical::Planner;
use planner::physical;
use storage::table::{NodeTable, RelTable};
use transaction::TransactionManager;
use transaction::wal::{WalPayload, WalRecord, WalWriter};

// ── Storage ─────────────────────────────────────────────────────

/// Manages in-memory storage for all node and relationship tables.
pub struct Storage {
    pub node_tables: HashMap<u32, NodeTable>,
    pub rel_tables: HashMap<u32, RelTable>,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            node_tables: HashMap::new(),
            rel_tables: HashMap::new(),
        }
    }
}

impl Default for Storage {
    fn default() -> Self {
        Self::new()
    }
}

// ── DatabaseInner ───────────────────────────────────────────────

pub(crate) struct DatabaseInner {
    pub(crate) path: PathBuf,
    pub(crate) catalog: RwLock<Catalog>,
    pub(crate) storage: RwLock<Storage>,
    pub(crate) txn_manager: TransactionManager,
    /// WAL writer — None for in-memory databases.
    pub(crate) wal: Mutex<Option<WalWriter>>,
}

// ── Database ────────────────────────────────────────────────────

/// Primary handle to a gqlite database backed by a `.graph` file.
///
/// Database is `Clone + Send + Sync` via `Arc<DatabaseInner>`.
#[derive(Clone)]
pub struct Database {
    inner: Arc<DatabaseInner>,
}

impl Database {
    /// Open (or create) a database at the given path.
    ///
    /// If a WAL file (`.graph.wal`) exists, committed transactions are replayed
    /// to rebuild state (crash recovery).
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, GqliteError> {
        let path = path.as_ref().to_path_buf();
        let wal_path = transaction::wal::wal_path_for(&path);

        let mut catalog = Catalog::new();
        let mut storage = Storage::new();

        // Recovery: replay WAL if it exists
        if wal_path.exists() {
            let mut reader = transaction::wal::WalReader::open(&wal_path)?;
            let records = reader.read_all()?;
            if !records.is_empty() {
                transaction::wal::replay_wal(&records, &mut catalog, &mut storage)?;
            }
        }

        // Open (or create) the WAL for subsequent writes
        let wal = if wal_path.exists() {
            WalWriter::open_append(&wal_path)?
        } else {
            WalWriter::create(&wal_path)?
        };

        Ok(Self {
            inner: Arc::new(DatabaseInner {
                path,
                catalog: RwLock::new(catalog),
                storage: RwLock::new(storage),
                txn_manager: TransactionManager::new(),
                wal: Mutex::new(Some(wal)),
            }),
        })
    }

    /// Create a new in-memory database (no file backing, no WAL).
    pub fn in_memory() -> Self {
        Self {
            inner: Arc::new(DatabaseInner {
                path: PathBuf::from(":memory:"),
                catalog: RwLock::new(Catalog::new()),
                storage: RwLock::new(Storage::new()),
                txn_manager: TransactionManager::new(),
                wal: Mutex::new(None),
            }),
        }
    }

    /// Create a connection to this database.
    pub fn connect(&self) -> Connection {
        Connection {
            db: self.inner.clone(),
        }
    }

    /// Convenience: execute a GQL statement.
    pub fn execute(&self, gql: &str) -> Result<QueryResult, GqliteError> {
        let conn = self.connect();
        conn.execute(gql)
    }

    /// Convenience: execute a read-only query.
    pub fn query(&self, gql: &str) -> Result<QueryResult, GqliteError> {
        self.execute(gql)
    }

    /// Return the file path of this database.
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// List all node table names.
    pub fn node_table_names(&self) -> Vec<String> {
        let catalog = self.inner.catalog.read().unwrap();
        catalog.node_tables().iter().map(|t| t.name.clone()).collect()
    }

    /// List all relationship table names.
    pub fn rel_table_names(&self) -> Vec<String> {
        let catalog = self.inner.catalog.read().unwrap();
        catalog.rel_tables().iter().map(|t| t.name.clone()).collect()
    }

    /// Get schema info for a table by name.
    /// Returns column (name, type) pairs, or None if not found.
    pub fn table_schema(&self, name: &str) -> Option<Vec<(String, DataType)>> {
        let catalog = self.inner.catalog.read().unwrap();
        if let Some(entry) = catalog.get_node_table(name) {
            return Some(
                entry.columns.iter().map(|c| (c.name.clone(), c.data_type.clone())).collect(),
            );
        }
        if let Some(entry) = catalog.get_rel_table(name) {
            return Some(
                entry.columns.iter().map(|c| (c.name.clone(), c.data_type.clone())).collect(),
            );
        }
        None
    }

    /// Checkpoint: flush the current WAL state and clear it.
    ///
    /// After checkpoint, the WAL only contains the complete set of operations
    /// needed to rebuild the database from scratch (a fresh snapshot).
    /// This avoids an ever-growing WAL file.
    pub fn checkpoint(&self) -> Result<(), GqliteError> {
        let mut wal_guard = self.inner.wal.lock();
        let wal = match wal_guard.as_mut() {
            Some(w) => w,
            None => return Ok(()), // in-memory — nothing to do
        };

        // Build a fresh WAL that represents the current state
        wal.clear()?;

        let catalog = self.inner.catalog.read().unwrap();
        let storage = self.inner.storage.read().unwrap();
        let txn_id = self.inner.txn_manager.last_committed_id() + 1;

        // Write DDL for all node tables
        for entry in catalog.node_tables() {
            let columns: Vec<(String, DataType)> = entry
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.data_type.clone()))
                .collect();
            let pk_name = entry.columns[entry.primary_key_idx].name.clone();
            wal.append(&WalRecord {
                txn_id,
                payload: WalPayload::CreateNodeTable {
                    name: entry.name.clone(),
                    columns,
                    primary_key: pk_name,
                },
            })?;
        }

        // Write DDL for all rel tables
        for entry in catalog.rel_tables() {
            let columns: Vec<(String, DataType)> = entry
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.data_type.clone()))
                .collect();
            // Look up source/destination table names
            let from_name = catalog
                .get_node_table_by_id(entry.src_table_id)
                .map(|e| e.name.clone())
                .unwrap_or_default();
            let to_name = catalog
                .get_node_table_by_id(entry.dst_table_id)
                .map(|e| e.name.clone())
                .unwrap_or_default();
            wal.append(&WalRecord {
                txn_id,
                payload: WalPayload::CreateRelTable {
                    name: entry.name.clone(),
                    from_table: from_name,
                    to_table: to_name,
                    columns,
                },
            })?;
        }

        // Write all node data
        for (&table_id, node_table) in &storage.node_tables {
            let entry = catalog.get_node_table_by_id(table_id);
            let table_name = entry.map(|e| e.name.clone()).unwrap_or_default();
            for (_offset, values) in node_table.scan() {
                wal.append(&WalRecord {
                    txn_id,
                    payload: WalPayload::InsertNode {
                        table_name: table_name.clone(),
                        table_id,
                        values,
                    },
                })?;
            }
        }

        // Write all rel data
        for (&table_id, rel_table) in &storage.rel_tables {
            let entry = catalog.get_rel_table_by_id(table_id);
            let rel_name = entry.map(|e| e.name.clone()).unwrap_or_default();
            for (src, dst) in rel_table.all_edges() {
                wal.append(&WalRecord {
                    txn_id,
                    payload: WalPayload::InsertRel {
                        rel_table_name: rel_name.clone(),
                        rel_table_id: table_id,
                        src,
                        dst,
                        properties: vec![],
                    },
                })?;
            }
        }

        // Commit marker
        wal.append(&WalRecord {
            txn_id,
            payload: WalPayload::TxnCommit,
        })?;

        Ok(())
    }
}

// ── Connection ──────────────────────────────────────────────────

/// A connection to a gqlite database. Each thread should use its own Connection.
pub struct Connection {
    db: Arc<DatabaseInner>,
}

impl Connection {
    /// Execute a GQL statement and return results.
    ///
    /// Automatically wraps execution in a transaction:
    /// - Read-only plans use a read-only transaction.
    /// - Mutating plans (DML/DDL) acquire the write lock and write WAL records.
    pub fn execute(&self, gql: &str) -> Result<QueryResult, GqliteError> {
        // 1. Parse
        let stmt = Parser::parse_query(gql)?;

        // 2. Bind
        let catalog = self.db.catalog.read().unwrap();
        let mut binder = Binder::new(&catalog);
        let bound = binder.bind(&stmt)?;

        // 3. Plan (logical)
        let planner = Planner::new(&catalog);
        let logical = planner.plan(&bound)?;

        // 4. Physical plan
        let physical = physical::to_physical(&logical);
        drop(catalog); // release read lock before execution

        // 5. Auto-transaction + Execute
        let engine = Engine::new();
        if physical.is_read_only() {
            let mut txn = self.db.txn_manager.begin_read_only();
            let result = engine.execute_plan(&physical, &self.db, txn.id);
            match &result {
                Ok(_) => self.db.txn_manager.commit(&mut txn),
                Err(_) => self.db.txn_manager.rollback(&mut txn),
            }
            result
        } else {
            let (mut txn, _write_guard) = self.db.txn_manager.begin_read_write()?;
            let txn_id = txn.id;
            let result = engine.execute_plan(&physical, &self.db, txn_id);
            match &result {
                Ok(_) => {
                    // Write TxnCommit to WAL
                    let mut wal_guard = self.db.wal.lock();
                    if let Some(wal) = wal_guard.as_mut() {
                        wal.append(&WalRecord {
                            txn_id,
                            payload: WalPayload::TxnCommit,
                        })?;
                    }
                    self.db.txn_manager.commit(&mut txn);
                }
                Err(_) => {
                    self.db.txn_manager.rollback(&mut txn);
                }
            }
            result
        }
    }

    /// Execute a query and return results (alias for execute).
    pub fn query(&self, gql: &str) -> Result<QueryResult, GqliteError> {
        self.execute(gql)
    }
}

// ── QueryResult ─────────────────────────────────────────────────

/// Metadata for a result column.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: DataType,
}

/// A single result row.
#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    /// Get value at column index.
    pub fn get(&self, idx: usize) -> &Value {
        &self.values[idx]
    }

    /// Get value as string if it is a string.
    pub fn get_string(&self, idx: usize) -> Option<&str> {
        self.values[idx].as_string()
    }

    /// Get value as i64 if it is an integer.
    pub fn get_int(&self, idx: usize) -> Option<i64> {
        self.values[idx].as_int()
    }

    /// Get value as f64 if it is a float.
    pub fn get_float(&self, idx: usize) -> Option<f64> {
        self.values[idx].as_float()
    }

    /// Get value as bool if it is a boolean.
    pub fn get_bool(&self, idx: usize) -> Option<bool> {
        self.values[idx].as_bool()
    }

    /// Number of columns in this row.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl std::fmt::Display for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cols: Vec<String> = self.values.iter().map(|v| v.to_string()).collect();
        write!(f, "{}", cols.join(" | "))
    }
}

/// Result of a query execution.
#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<ColumnInfo>,
    rows: Vec<Row>,
    cursor: usize,
}

impl QueryResult {
    /// Create a new QueryResult.
    pub fn new(columns: Vec<ColumnInfo>, rows: Vec<Row>) -> Self {
        Self {
            columns,
            rows,
            cursor: 0,
        }
    }

    /// Create an empty result (for DDL operations).
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            cursor: 0,
        }
    }

    /// Number of rows.
    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    /// Whether the result set is empty.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Get all rows as a slice.
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    /// Get column names.
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

impl Iterator for QueryResult {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor < self.rows.len() {
            let row = self.rows[self.cursor].clone();
            self.cursor += 1;
            Some(row)
        } else {
            None
        }
    }
}
