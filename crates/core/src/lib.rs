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
pub mod procedure;
pub mod storage;
pub mod testing;
pub mod transaction;
pub mod types;

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use fs2::FileExt;
use log::{debug, info, warn};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

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
use transaction::wal::{WalPayload, WalRecord, WalWriter};
use transaction::TransactionManager;

// ── Storage ─────────────────────────────────────────────────────

/// Manages in-memory storage for all node and relationship tables.
#[derive(Serialize, Deserialize)]
pub struct Storage {
    pub node_tables: HashMap<u32, NodeTable>,
    pub rel_tables: HashMap<u32, RelTable>,
}

impl Storage {
    pub fn new() -> Self {
        Self { node_tables: HashMap::new(), rel_tables: HashMap::new() }
    }

    /// Serialize the storage to bytes using bincode.
    pub fn to_bytes(&self) -> Result<Vec<u8>, GqliteError> {
        bincode::serialize(self)
            .map_err(|e| GqliteError::Storage(format!("storage serialize error: {}", e)))
    }

    /// Deserialize storage from bincode bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, GqliteError> {
        bincode::deserialize(data)
            .map_err(|e| GqliteError::Storage(format!("storage deserialize error: {}", e)))
    }

    /// Persist the storage into the pager starting at `start_page`.
    ///
    /// Format: first 8 bytes = total length (u64 LE), then the bincode payload.
    /// If the data exceeds one page, it spans consecutive pages.
    /// v2 format: each page has an 8-byte header (page_type + checksum).
    pub fn save_to(
        &self,
        pager: &mut storage::pager::Pager,
        start_page: storage::pager::PageId,
    ) -> Result<(), GqliteError> {
        use storage::format::{write_page_header, PageType, PAGE_HEADER_SIZE};

        let payload = self.to_bytes()?;
        let total_len = payload.len() as u64;
        let page_size = pager.page_size() as usize;
        let is_v2 = pager.header().version >= 2;
        let usable_per_page = if is_v2 { page_size - PAGE_HEADER_SIZE } else { page_size };

        // Build the full byte stream: 8-byte length prefix + payload
        let mut stream = Vec::with_capacity(8 + payload.len());
        stream.extend_from_slice(&total_len.to_le_bytes());
        stream.extend_from_slice(&payload);

        // Calculate how many pages we need
        let pages_needed = stream.len().div_ceil(usable_per_page);

        // Ensure we have enough pages allocated
        while pager.page_count() < start_page + pages_needed as u64 {
            pager.allocate_page()?;
        }

        // Write page by page
        for i in 0..pages_needed {
            let page_id = start_page + i as u64;
            let start = i * usable_per_page;
            let end = std::cmp::min(start + usable_per_page, stream.len());

            let mut page_buf = vec![0u8; page_size];
            if is_v2 {
                page_buf[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + (end - start)]
                    .copy_from_slice(&stream[start..end]);
                write_page_header(&mut page_buf, PageType::ColumnData);
            } else {
                page_buf[..end - start].copy_from_slice(&stream[start..end]);
            }
            pager.write_page(page_id, &page_buf)?;
        }

        Ok(())
    }

    /// Load the storage from the pager starting at `start_page`.
    pub fn load_from(
        pager: &storage::pager::Pager,
        start_page: storage::pager::PageId,
    ) -> Result<Self, GqliteError> {
        use storage::format::{verify_page_header, PAGE_HEADER_SIZE};

        let page_size = pager.page_size() as usize;
        let is_v2 = pager.header().version >= 2;
        let header_offset = if is_v2 { PAGE_HEADER_SIZE } else { 0 };
        let usable_per_page = page_size - header_offset;

        // Read first page to get the total length
        let mut first_page = vec![0u8; page_size];
        pager.read_page(start_page, &mut first_page)?;

        if is_v2 {
            verify_page_header(&first_page, start_page)?;
        }

        let total_len =
            u64::from_le_bytes(first_page[header_offset..header_offset + 8].try_into().unwrap())
                as usize;
        let total_with_prefix = 8 + total_len;
        let pages_needed = total_with_prefix.div_ceil(usable_per_page);

        // Accumulate all payload bytes
        let first_take = std::cmp::min(usable_per_page, total_with_prefix);
        let mut stream = Vec::with_capacity(total_with_prefix);
        stream.extend_from_slice(&first_page[header_offset..header_offset + first_take]);

        for i in 1..pages_needed {
            let page_id = start_page + i as u64;
            let mut buf = vec![0u8; page_size];
            pager.read_page(page_id, &mut buf)?;
            if is_v2 {
                verify_page_header(&buf, page_id)?;
            }
            let remaining = total_with_prefix - stream.len();
            let take = std::cmp::min(usable_per_page, remaining);
            stream.extend_from_slice(&buf[header_offset..header_offset + take]);
        }

        // Skip the 8-byte length prefix
        let payload = &stream[8..8 + total_len];
        Self::from_bytes(payload)
    }
}

impl Default for Storage {
    fn default() -> Self {
        Self::new()
    }
}

// ── DatabaseConfig ──────────────────────────────────────────────

/// Configuration options for a gqlite database.
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Buffer pool size in bytes. Default: 256 MB.
    pub buffer_pool_size: usize,
    /// Whether to open in read-only mode (writes are rejected). Default: false.
    pub read_only: bool,
    /// Whether to enable auto-checkpoint after write transactions. Default: true.
    pub auto_checkpoint: bool,
    /// Number of WAL records before triggering auto-checkpoint. Default: 10_000.
    pub checkpoint_threshold: u64,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            buffer_pool_size: 256 * 1024 * 1024, // 256 MB
            read_only: false,
            auto_checkpoint: true,
            checkpoint_threshold: 10_000,
        }
    }
}

// ── DatabaseInner ───────────────────────────────────────────────

pub struct DatabaseInner {
    pub path: PathBuf,
    pub config: DatabaseConfig,
    pub catalog: RwLock<Catalog>,
    pub storage: RwLock<Storage>,
    pub txn_manager: TransactionManager,
    /// WAL writer — None for in-memory databases.
    pub wal: Mutex<Option<WalWriter>>,
    /// File lock — held for the lifetime of the database to prevent concurrent access.
    /// None for in-memory databases.
    _lock_file: Option<File>,
}

// ── Database ────────────────────────────────────────────────────

/// Primary handle to a gqlite database backed by a `.graph` file.
///
/// Database is `Clone + Send + Sync` via `Arc<DatabaseInner>`.
#[derive(Clone)]
pub struct Database {
    pub inner: Arc<DatabaseInner>,
}

impl Database {
    /// Open (or create) a database at the given path with default configuration.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, GqliteError> {
        Self::open_with_config(path, DatabaseConfig::default())
    }

    /// Open (or create) a database at the given path with custom configuration.
    pub fn open_with_config<P: AsRef<Path>>(
        path: P,
        config: DatabaseConfig,
    ) -> Result<Self, GqliteError> {
        let path = path.as_ref().to_path_buf();
        let wal_path = transaction::wal::wal_path_for(&path);

        // Acquire file lock to prevent concurrent access from other processes.
        let lock_path = path.with_extension("graph.lock");
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| {
                GqliteError::Storage(format!(
                    "failed to open lock file '{}': {}",
                    lock_path.display(),
                    e
                ))
            })?;

        if config.read_only {
            lock_file.try_lock_shared().map_err(|_| {
                GqliteError::Storage(format!(
                    "database '{}' is locked by another process for exclusive access",
                    path.display()
                ))
            })?;
        } else {
            lock_file.try_lock_exclusive().map_err(|_| {
                GqliteError::Storage(format!(
                    "database '{}' is already opened by another process",
                    path.display()
                ))
            })?;
        }

        let mut catalog = Catalog::new();
        let mut storage = Storage::new();
        let mut max_committed_id = 0u64;
        let mut checkpoint_ts = 0u64;

        // Phase 1: Load from .graph main file if it exists and is valid
        if path.exists() {
            info!("opening database at '{}'", path.display());
            match storage::pager::Pager::open(&path) {
                Ok(pager) => {
                    let header = pager.header();
                    if header.checkpoint_ts > 0 {
                        // Try to load Catalog + Storage from main file
                        match Catalog::load_from(&pager, header.catalog_page_idx) {
                            Ok(loaded_catalog) => {
                                match Storage::load_from(&pager, header.storage_page_idx) {
                                    Ok(loaded_storage) => {
                                        catalog = loaded_catalog;
                                        storage = loaded_storage;
                                        checkpoint_ts = header.checkpoint_ts;
                                        max_committed_id = checkpoint_ts;
                                        info!(
                                            "loaded checkpoint ts={} version={}",
                                            checkpoint_ts, header.version
                                        );
                                    }
                                    Err(_) => {
                                        // Fallback: ignore main file, rely on WAL
                                        catalog = Catalog::new();
                                        storage = Storage::new();
                                    }
                                }
                            }
                            Err(_) => {
                                // Fallback: ignore main file, rely on WAL
                            }
                        }
                    }
                }
                Err(_) => {
                    // .graph file exists but can't be opened — ignore, rely on WAL
                }
            }
        }

        // Phase 2: Replay WAL (incremental if we loaded from main file)
        let mut wal_record_count = 0u64;
        if wal_path.exists() {
            let mut reader = transaction::wal::WalReader::open(&wal_path)?;
            let records = reader.read_all()?;
            wal_record_count = records.len() as u64;
            if !records.is_empty() {
                let wal_max = if checkpoint_ts > 0 {
                    transaction::wal::replay_wal_incremental(
                        &records,
                        &mut catalog,
                        &mut storage,
                        checkpoint_ts,
                    )?
                } else {
                    transaction::wal::replay_wal(&records, &mut catalog, &mut storage)?
                };
                if wal_max > max_committed_id {
                    max_committed_id = wal_max;
                }
                info!(
                    "replayed {} WAL records, max_committed_id={}",
                    wal_record_count, max_committed_id
                );
            }
        }

        // Open (or create) the WAL for subsequent writes
        let wal = if config.read_only {
            None
        } else if wal_path.exists() {
            let mut w = WalWriter::open_append(&wal_path)?;
            w.set_record_count(wal_record_count);
            Some(w)
        } else {
            Some(WalWriter::create(&wal_path)?)
        };

        Ok(Self {
            inner: Arc::new(DatabaseInner {
                path,
                config,
                catalog: RwLock::new(catalog),
                storage: RwLock::new(storage),
                txn_manager: TransactionManager::with_recovered_state(max_committed_id),
                wal: Mutex::new(wal),
                _lock_file: Some(lock_file),
            }),
        })
    }

    /// Create a new in-memory database (no file backing, no WAL).
    pub fn in_memory() -> Self {
        Self {
            inner: Arc::new(DatabaseInner {
                path: PathBuf::from(":memory:"),
                config: DatabaseConfig::default(),
                catalog: RwLock::new(Catalog::new()),
                storage: RwLock::new(Storage::new()),
                txn_manager: TransactionManager::new(),
                wal: Mutex::new(None),
                _lock_file: None,
            }),
        }
    }

    /// Create a connection to this database.
    pub fn connect(&self) -> Connection {
        Connection { db: self.inner.clone() }
    }

    /// Convenience: execute a GQL statement.
    pub fn execute(&self, gql: &str) -> Result<QueryResult, GqliteError> {
        let conn = self.connect();
        conn.execute(gql)
    }

    /// Execute a script containing multiple semicolon-separated GQL statements.
    /// Returns the result of the last statement, or an empty result if no statements.
    /// Stops on the first error.
    pub fn execute_script(&self, script: &str) -> Result<QueryResult, GqliteError> {
        let conn = self.connect();
        conn.execute_script(script)
    }

    /// Convenience: execute a read-only query.
    pub fn query(&self, gql: &str) -> Result<QueryResult, GqliteError> {
        self.execute(gql)
    }

    /// Return the file path of this database.
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// Return the database configuration.
    pub fn config(&self) -> &DatabaseConfig {
        &self.inner.config
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

    /// Checkpoint: serialize Catalog + Storage to `.graph` main file, then clear WAL.
    ///
    /// Crash-safe: writes to `.graph.tmp` first, then atomically renames.
    pub fn checkpoint(&self) -> Result<(), GqliteError> {
        checkpoint_impl(&self.inner)
    }

    /// Dump the entire database as a Cypher script string.
    ///
    /// The output contains:
    /// 1. CREATE NODE TABLE / CREATE REL TABLE statements (schema)
    /// 2. CREATE (...) statements for all nodes
    /// 3. MATCH ... CREATE relationship statements for all edges
    ///
    /// The script can be executed against an empty database to restore the data.
    pub fn dump(&self) -> Result<String, GqliteError> {
        let catalog = self.inner.catalog.read().unwrap();
        let storage = self.inner.storage.read().unwrap();
        let mut out = String::new();

        // 1. Schema: Node tables
        for entry in catalog.node_tables() {
            out.push_str("CREATE NODE TABLE ");
            out.push_str(&entry.name);
            out.push('(');
            for (i, col) in entry.columns.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                out.push_str(&col.name);
                out.push(' ');
                out.push_str(&format!("{}", col.data_type));
            }
            out.push_str(", PRIMARY KEY(");
            out.push_str(&entry.columns[entry.primary_key_idx].name);
            out.push_str("));\n");
        }

        // 2. Schema: Rel tables
        for entry in catalog.rel_tables() {
            let src_name = catalog
                .get_node_table_by_id(entry.src_table_id)
                .map(|e| e.name.as_str())
                .unwrap_or("?");
            let dst_name = catalog
                .get_node_table_by_id(entry.dst_table_id)
                .map(|e| e.name.as_str())
                .unwrap_or("?");
            out.push_str("CREATE REL TABLE ");
            out.push_str(&entry.name);
            out.push_str("(FROM ");
            out.push_str(src_name);
            out.push_str(" TO ");
            out.push_str(dst_name);
            for col in &entry.columns {
                out.push_str(", ");
                out.push_str(&col.name);
                out.push(' ');
                out.push_str(&format!("{}", col.data_type));
            }
            out.push_str(");\n");
        }

        // 3. Data: Nodes
        for entry in catalog.node_tables() {
            if let Some(nt) = storage.node_tables.get(&entry.table_id) {
                for (_offset, row) in nt.scan() {
                    out.push_str("CREATE (n:");
                    out.push_str(&entry.name);
                    out.push_str(" {");
                    let mut first = true;
                    for (i, col) in entry.columns.iter().enumerate() {
                        if i < row.len() && !row[i].is_null() {
                            if !first {
                                out.push_str(", ");
                            }
                            first = false;
                            out.push_str(&col.name);
                            out.push_str(": ");
                            out.push_str(&value_to_cypher(&row[i]));
                        }
                    }
                    out.push_str("});\n");
                }
            }
        }

        // 4. Data: Relationships
        for rel_entry in catalog.rel_tables() {
            let src_entry = catalog.get_node_table_by_id(rel_entry.src_table_id);
            let dst_entry = catalog.get_node_table_by_id(rel_entry.dst_table_id);
            if src_entry.is_none() || dst_entry.is_none() {
                continue;
            }
            let src_entry = src_entry.unwrap();
            let dst_entry = dst_entry.unwrap();
            let src_pk_col = &src_entry.columns[src_entry.primary_key_idx].name;
            let dst_pk_col = &dst_entry.columns[dst_entry.primary_key_idx].name;

            if let Some(rt) = storage.rel_tables.get(&rel_entry.table_id) {
                let src_nt = storage.node_tables.get(&rel_entry.src_table_id);
                let dst_nt = storage.node_tables.get(&rel_entry.dst_table_id);
                if src_nt.is_none() || dst_nt.is_none() {
                    continue;
                }
                let src_nt = src_nt.unwrap();
                let dst_nt = dst_nt.unwrap();

                for (src_id, dst_id) in rt.all_edges() {
                    // Look up PK values for src and dst
                    let src_row = src_nt.read(src_id.offset);
                    let dst_row = dst_nt.read(dst_id.offset);
                    if src_row.is_err() || dst_row.is_err() {
                        continue;
                    }
                    let src_row = src_row.unwrap();
                    let dst_row = dst_row.unwrap();
                    let src_pk = &src_row[src_entry.primary_key_idx];
                    let dst_pk = &dst_row[dst_entry.primary_key_idx];

                    out.push_str("MATCH (a:");
                    out.push_str(&src_entry.name);
                    out.push_str("), (b:");
                    out.push_str(&dst_entry.name);
                    out.push_str(") WHERE a.");
                    out.push_str(src_pk_col);
                    out.push_str(" = ");
                    out.push_str(&value_to_cypher(src_pk));
                    out.push_str(" AND b.");
                    out.push_str(dst_pk_col);
                    out.push_str(" = ");
                    out.push_str(&value_to_cypher(dst_pk));
                    out.push_str(" CREATE (a)-[r:");
                    out.push_str(&rel_entry.name);
                    out.push_str("]->(b);\n");
                }
            }
        }

        Ok(out)
    }

    /// Check database integrity.
    ///
    /// Returns a list of issues found. An empty list means the database is healthy.
    /// Each issue is a human-readable string describing the problem.
    pub fn check(&self) -> Vec<String> {
        let mut issues = Vec::new();
        let catalog = self.inner.catalog.read().unwrap();
        let storage = self.inner.storage.read().unwrap();

        // 1. Check Catalog consistency
        for entry in catalog.node_tables() {
            if !storage.node_tables.contains_key(&entry.table_id) {
                issues.push(format!(
                    "ERROR: node table '{}' (id={}) exists in catalog but not in storage",
                    entry.name, entry.table_id
                ));
            }
        }
        for entry in catalog.rel_tables() {
            if !storage.rel_tables.contains_key(&entry.table_id) {
                issues.push(format!(
                    "ERROR: rel table '{}' (id={}) exists in catalog but not in storage",
                    entry.name, entry.table_id
                ));
            }
            // Check src/dst table references
            if catalog.get_node_table_by_id(entry.src_table_id).is_none() {
                issues.push(format!(
                    "ERROR: rel table '{}' references non-existent src table id={}",
                    entry.name, entry.src_table_id
                ));
            }
            if catalog.get_node_table_by_id(entry.dst_table_id).is_none() {
                issues.push(format!(
                    "ERROR: rel table '{}' references non-existent dst table id={}",
                    entry.name, entry.dst_table_id
                ));
            }
        }

        // 2. Check storage tables have catalog entries
        for table_id in storage.node_tables.keys() {
            if catalog.get_node_table_by_id(*table_id).is_none() {
                issues.push(format!(
                    "WARNING: storage has node table id={} with no catalog entry",
                    table_id
                ));
            }
        }

        // 3. Check MVCC metadata consistency
        for entry in catalog.node_tables() {
            if let Some(nt) = storage.node_tables.get(&entry.table_id) {
                let row_count = nt.row_count();
                let ts_count = nt.create_ts_len();
                if row_count != ts_count as u64 {
                    issues.push(format!(
                        "WARNING: table '{}' has {} rows but {} MVCC timestamps",
                        entry.name, row_count, ts_count
                    ));
                }
            }
        }

        issues
    }
}

// ── Checkpoint implementation ──────────────────────────────────

/// Convert a Value to a Cypher literal string.
fn value_to_cypher(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => format!("{}", b),
        Value::Int(i) => format!("{}", i),
        Value::Float(f) => format!("{}", f),
        Value::String(s) => {
            // Escape single quotes
            let escaped = s.replace('\\', "\\\\").replace('\'', "\\'");
            format!("'{}'", escaped)
        }
        Value::Date(d) => format!("date('{}')", d.format("%Y-%m-%d")),
        Value::DateTime(dt) => format!("datetime('{}')", dt.format("%Y-%m-%dT%H:%M:%S")),
        Value::Duration(ms) => format!("duration('PT{}S')", ms / 1000),
        Value::List(items) => {
            let inner: Vec<String> = items.iter().map(value_to_cypher).collect();
            format!("[{}]", inner.join(", "))
        }
        Value::InternalId(id) => format!("{}", id),
        Value::Bytes(_) => "'<bytes>'".to_string(),
    }
}

/// Standalone checkpoint logic, callable from both Database::checkpoint and auto-checkpoint.
fn checkpoint_impl(inner: &DatabaseInner) -> Result<(), GqliteError> {
    let start = std::time::Instant::now();
    let mut wal_guard = inner.wal.lock();
    let wal = match wal_guard.as_mut() {
        Some(w) => w,
        None => return Ok(()),
    };

    let catalog = inner.catalog.read().unwrap();
    let storage_guard = inner.storage.read().unwrap();
    let checkpoint_ts = inner.txn_manager.last_committed_id();

    let db_path = &inner.path;
    let tmp_path = db_path.with_extension("graph.tmp");

    let _ = std::fs::remove_file(&tmp_path);

    let mut pager = storage::pager::Pager::create(&tmp_path)?;

    let catalog_start: storage::pager::PageId = 1;
    catalog.save_to(&mut pager, catalog_start)?;

    let catalog_bytes = catalog.to_bytes()?;
    let page_size = pager.page_size() as usize;
    let is_v2 = pager.header().version >= 2;
    let usable_per_page =
        if is_v2 { page_size - storage::format::PAGE_HEADER_SIZE } else { page_size };
    let catalog_stream_len = 8 + catalog_bytes.len();
    let catalog_pages = catalog_stream_len.div_ceil(usable_per_page);
    let storage_start = catalog_start + catalog_pages as u64;

    storage_guard.save_to(&mut pager, storage_start)?;

    {
        let header = pager.header_mut();
        header.catalog_page_idx = catalog_start;
        header.storage_page_idx = storage_start;
        header.checkpoint_ts = checkpoint_ts;
    }
    pager.flush_header()?;
    pager.sync()?;
    drop(pager);

    std::fs::rename(&tmp_path, db_path)?;
    wal.clear()?;

    info!("checkpoint completed ts={} elapsed={:?}", checkpoint_ts, start.elapsed());
    Ok(())
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
        self.execute_with_params(gql, HashMap::new())
    }

    /// Execute a script containing multiple semicolon-separated GQL statements.
    /// Returns the result of the last statement, or an empty result if no statements.
    /// Stops on the first error.
    ///
    /// Supports explicit transactions: `BEGIN; ...; COMMIT;` groups multiple
    /// statements into a single atomic transaction. `ROLLBACK` discards all
    /// changes since the last `BEGIN`.
    pub fn execute_script(&self, script: &str) -> Result<QueryResult, GqliteError> {
        use crate::parser::ast::Statement as AstStatement;

        let stmts = Parser::parse_all(script)?;
        let mut last_result = QueryResult::empty();
        let mut in_transaction = false;
        let mut txn_stmts: Vec<&AstStatement> = Vec::new();

        for stmt in &stmts {
            match stmt {
                AstStatement::Begin => {
                    if in_transaction {
                        return Err(GqliteError::Execution("nested BEGIN is not allowed".into()));
                    }
                    in_transaction = true;
                    txn_stmts.clear();
                }
                AstStatement::Commit => {
                    if !in_transaction {
                        return Err(GqliteError::Execution(
                            "COMMIT without active transaction".into(),
                        ));
                    }
                    // Execute all buffered statements in a single transaction
                    last_result = self.execute_transaction_block(&txn_stmts)?;
                    in_transaction = false;
                    txn_stmts.clear();
                }
                AstStatement::Rollback => {
                    if !in_transaction {
                        return Err(GqliteError::Execution(
                            "ROLLBACK without active transaction".into(),
                        ));
                    }
                    // Discard all buffered statements
                    in_transaction = false;
                    txn_stmts.clear();
                    last_result = QueryResult::empty();
                }
                _ => {
                    if in_transaction {
                        txn_stmts.push(stmt);
                    } else {
                        last_result = self.execute_statement(stmt, HashMap::new())?;
                    }
                }
            }
        }

        if in_transaction {
            return Err(GqliteError::Execution(
                "unterminated transaction: missing COMMIT or ROLLBACK".into(),
            ));
        }

        Ok(last_result)
    }

    /// Execute multiple statements as a single atomic transaction.
    ///
    /// All statements are executed in a single write transaction. WAL records
    /// are buffered and flushed atomically on commit. If any statement fails,
    /// no WAL records are written (but in-memory storage may have been modified).
    fn execute_transaction_block(
        &self,
        stmts: &[&crate::parser::ast::Statement],
    ) -> Result<QueryResult, GqliteError> {
        use crate::parser::ast::Statement as AstStatement;

        if stmts.is_empty() {
            return Ok(QueryResult::empty());
        }

        // Acquire write transaction
        let (mut txn, _write_guard) = self.db.txn_manager.begin_read_write()?;
        let txn_id = txn.id;
        let mut engine = Engine::with_snapshot(txn.start_ts, HashMap::new());
        engine.set_db(self.db.clone());
        let mut last_result = QueryResult::empty();

        for stmt in stmts {
            // Handle CALL within transaction
            if let AstStatement::Call { procedure, args, yields } = stmt {
                match self.execute_call(procedure, args, yields, &HashMap::new()) {
                    Ok(r) => {
                        last_result = r;
                        continue;
                    }
                    Err(e) => {
                        self.db.txn_manager.rollback(&mut txn);
                        return Err(e);
                    }
                }
            }

            // Bind + plan
            let catalog = self.db.catalog.read().unwrap();
            let mut binder = Binder::new(&catalog);
            let bound = match binder.bind(stmt) {
                Ok(b) => b,
                Err(e) => {
                    drop(catalog);
                    self.db.txn_manager.rollback(&mut txn);
                    return Err(e);
                }
            };
            let planner = Planner::new(&catalog);
            let logical = match planner.plan(&bound) {
                Ok(l) => l,
                Err(e) => {
                    drop(catalog);
                    self.db.txn_manager.rollback(&mut txn);
                    return Err(e);
                }
            };
            let logical = planner::optimizer::optimize(logical);
            let physical = physical::to_physical(&logical);
            drop(catalog);

            if !physical.is_read_only() && self.db.config.read_only {
                self.db.txn_manager.rollback(&mut txn);
                return Err(GqliteError::Execution("database is opened in read-only mode".into()));
            }

            // Execute (both read and write ops go through the engine)
            match engine.execute_plan(&physical, &self.db, txn_id) {
                Ok(r) => {
                    last_result = r;
                }
                Err(e) => {
                    self.db.txn_manager.rollback(&mut txn);
                    return Err(e);
                }
            }
        }

        // Flush all buffered WAL records + TxnCommit
        let mut should_checkpoint = false;
        {
            let mut wal_guard = self.db.wal.lock();
            if let Some(wal) = wal_guard.as_mut() {
                for record in &engine.wal_buffer {
                    wal.append(record)?;
                }
                wal.append(&WalRecord { txn_id, payload: WalPayload::TxnCommit })?;
                should_checkpoint = self.db.config.auto_checkpoint
                    && wal.record_count() >= self.db.config.checkpoint_threshold;
            }
        }
        self.db.txn_manager.commit(&mut txn);
        drop(_write_guard);
        if should_checkpoint {
            let _ = checkpoint_impl(&self.db);
        }

        Ok(last_result)
    }

    /// Execute a GQL statement with parameter bindings.
    pub fn execute_with_params(
        &self,
        gql: &str,
        params: HashMap<String, Value>,
    ) -> Result<QueryResult, GqliteError> {
        let stmt = Parser::parse_query(gql)?;
        self.execute_statement(&stmt, params)
    }

    /// Execute a pre-parsed statement with parameter bindings.
    fn execute_statement(
        &self,
        stmt: &crate::parser::ast::Statement,
        params: HashMap<String, Value>,
    ) -> Result<QueryResult, GqliteError> {
        use crate::parser::ast::Statement as AstStatement;

        // Handle CALL procedure directly (bypasses binder/planner)
        if let AstStatement::Call { procedure, args, yields } = &stmt {
            return self.execute_call(procedure, args, yields, &params);
        }

        // Handle EXPLAIN — show execution plan without running the query
        if let AstStatement::Explain(inner) = &stmt {
            let catalog = self.db.catalog.read().unwrap();
            let mut binder = Binder::new(&catalog);
            let bound = binder.bind(inner)?;
            let planner_inst = Planner::new(&catalog);
            let logical = planner_inst.plan(&bound)?;
            let logical = planner::optimizer::optimize(logical);
            let physical = physical::to_physical(&logical);
            drop(catalog);

            let plan_text = physical.explain_text(0);
            // Return the plan as a single-column result
            let col = ColumnInfo { name: "plan".to_string(), data_type: DataType::String };
            let rows: Vec<Row> = plan_text
                .lines()
                .filter(|l| !l.is_empty())
                .map(|l| Row { values: vec![Value::String(l.to_string())] })
                .collect();
            return Ok(QueryResult::new(vec![col], rows));
        }

        // Handle transaction control statements.
        // These are supported within execute_script (multi-statement mode).
        // Single-statement execute() does not support explicit transactions.
        match &stmt {
            AstStatement::Begin => {
                return Err(GqliteError::Execution(
                    "BEGIN is only supported in execute_script() with multi-statement mode \
                     (e.g., \"BEGIN; ...; COMMIT;\")"
                        .into(),
                ));
            }
            AstStatement::Commit => {
                return Err(GqliteError::Execution("COMMIT without active transaction".into()));
            }
            AstStatement::Rollback => {
                return Err(GqliteError::Execution("ROLLBACK without active transaction".into()));
            }
            _ => {}
        }

        // 2. Bind
        let catalog = self.db.catalog.read().unwrap();
        let mut binder = Binder::new(&catalog);
        let bound = binder.bind(stmt)?;

        // 3. Plan (logical) + optimize
        let planner = Planner::new(&catalog);
        let logical = planner.plan(&bound)?;
        let logical = planner::optimizer::optimize(logical);

        // 4. Physical plan
        let physical = physical::to_physical(&logical);
        drop(catalog); // release read lock before execution

        // 4b. Reject writes in read-only mode
        if !physical.is_read_only() && self.db.config.read_only {
            return Err(GqliteError::Execution("database is opened in read-only mode".into()));
        }

        // 5. Auto-transaction + Execute
        if physical.is_read_only() {
            let mut txn = self.db.txn_manager.begin_read_only();
            let mut engine = Engine::with_snapshot(txn.start_ts, params);
            engine.set_db(self.db.clone());
            let start = std::time::Instant::now();
            let result = engine.execute_plan_parallel(&physical, &self.db, txn.id);
            let elapsed = start.elapsed();
            match &result {
                Ok(r) => {
                    self.db.txn_manager.commit(&mut txn);
                    debug!("query txn={} rows={} elapsed={:?}", txn.id, r.num_rows(), elapsed);
                }
                Err(e) => {
                    self.db.txn_manager.rollback(&mut txn);
                    debug!("query txn={} failed: {} elapsed={:?}", txn.id, e, elapsed);
                }
            }
            result
        } else {
            let (mut txn, _write_guard) = self.db.txn_manager.begin_read_write()?;
            let txn_id = txn.id;
            let mut engine = Engine::with_snapshot(txn.start_ts, params);
            engine.set_db(self.db.clone());
            let start = std::time::Instant::now();
            let result = engine.execute_plan(&physical, &self.db, txn_id);
            let elapsed = start.elapsed();
            let mut should_checkpoint = false;
            match &result {
                Ok(_) => {
                    // Flush buffered WAL records + TxnCommit atomically
                    let mut wal_guard = self.db.wal.lock();
                    if let Some(wal) = wal_guard.as_mut() {
                        for record in &engine.wal_buffer {
                            wal.append(record)?;
                        }
                        wal.append(&WalRecord { txn_id, payload: WalPayload::TxnCommit })?;
                        should_checkpoint = self.db.config.auto_checkpoint
                            && wal.record_count() >= self.db.config.checkpoint_threshold;
                    }
                    self.db.txn_manager.commit(&mut txn);
                    debug!(
                        "write txn={} committed wal_records={} elapsed={:?}",
                        txn_id,
                        engine.wal_buffer.len(),
                        elapsed
                    );
                }
                Err(e) => {
                    // Discard buffered WAL records — no WAL writes for failed transactions
                    self.db.txn_manager.rollback(&mut txn);
                    warn!("write txn={} rolled back: {} elapsed={:?}", txn_id, e, elapsed);
                }
            }
            drop(_write_guard);
            if should_checkpoint {
                let _ = checkpoint_impl(&self.db);
            }
            result
        }
    }

    /// Prepare a GQL statement for later execution with parameter bindings.
    pub fn prepare(&self, gql: &str) -> Result<PreparedStatement, GqliteError> {
        let stmt = Parser::parse_query(gql)?;

        let catalog = self.db.catalog.read().unwrap();
        let mut binder = Binder::new(&catalog);
        let bound = binder.bind(&stmt)?;

        let planner = Planner::new(&catalog);
        let logical = planner.plan(&bound)?;
        let logical = planner::optimizer::optimize(logical);
        let physical = physical::to_physical(&logical);

        Ok(PreparedStatement { db: self.db.clone(), plan: physical })
    }

    /// Execute a query and return results (alias for execute).
    pub fn query(&self, gql: &str) -> Result<QueryResult, GqliteError> {
        self.execute(gql)
    }

    /// Execute a CALL procedure statement directly.
    fn execute_call(
        &self,
        procedure_name: &str,
        arg_exprs: &[crate::parser::ast::Expr],
        yields: &[String],
        params: &HashMap<String, Value>,
    ) -> Result<QueryResult, GqliteError> {
        // Resolve the procedure from the registry
        let registry = procedure::registry::ProcedureRegistry::new();
        let proc = registry.get(procedure_name).ok_or_else(|| {
            GqliteError::Execution(format!("unknown procedure '{}'", procedure_name))
        })?;

        // Evaluate argument expressions to values
        let args: Vec<Value> = arg_exprs
            .iter()
            .map(|expr| self.eval_call_arg(expr, params))
            .collect::<Result<Vec<_>, _>>()?;

        // Execute the procedure
        let all_columns = proc.output_columns();
        let all_rows = proc.execute(&args, &self.db)?;

        // Filter columns by YIELD list (if specified)
        if yields.is_empty() {
            // Return all columns
            let columns: Vec<ColumnInfo> = all_columns
                .iter()
                .map(|name| ColumnInfo { name: name.clone(), data_type: DataType::String })
                .collect();
            let rows: Vec<Row> = all_rows.into_iter().map(|r| Row { values: r }).collect();
            Ok(QueryResult::new(columns, rows))
        } else {
            // Map YIELD column names to indices in the procedure output
            let yield_indices: Vec<usize> = yields
                .iter()
                .map(|y| {
                    all_columns.iter().position(|c| c == y).ok_or_else(|| {
                        GqliteError::Execution(format!(
                            "procedure '{}' does not output column '{}'",
                            procedure_name, y
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            let columns: Vec<ColumnInfo> = yields
                .iter()
                .map(|name| ColumnInfo { name: name.clone(), data_type: DataType::String })
                .collect();

            let rows: Vec<Row> = all_rows
                .into_iter()
                .map(|row| {
                    let values: Vec<Value> =
                        yield_indices.iter().map(|&idx| row[idx].clone()).collect();
                    Row { values }
                })
                .collect();

            Ok(QueryResult::new(columns, rows))
        }
    }

    /// Evaluate a simple expression for CALL arguments.
    /// Supports literals and parameter references.
    fn eval_call_arg(
        &self,
        expr: &crate::parser::ast::Expr,
        params: &HashMap<String, Value>,
    ) -> Result<Value, GqliteError> {
        use crate::parser::ast::Expr;
        match expr {
            Expr::IntLit(v) => Ok(Value::Int(*v)),
            Expr::FloatLit(v) => Ok(Value::Float(*v)),
            Expr::StringLit(s) => Ok(Value::String(s.clone())),
            Expr::BoolLit(b) => Ok(Value::Bool(*b)),
            Expr::NullLit => Ok(Value::Null),
            Expr::Param(name) => params
                .get(name)
                .cloned()
                .ok_or_else(|| GqliteError::Execution(format!("parameter '{}' not found", name))),
            _ => Err(GqliteError::Execution("unsupported expression in CALL arguments".into())),
        }
    }
}

// ── PreparedStatement ────────────────────────────────────────────

/// A pre-compiled GQL statement that can be executed multiple times with
/// different parameter bindings.
pub struct PreparedStatement {
    db: Arc<DatabaseInner>,
    plan: planner::physical::PhysicalPlan,
}

impl PreparedStatement {
    /// Execute the prepared statement with parameter bindings.
    ///
    /// Parameters are passed as a map of `$name` → Value.
    pub fn execute(&self, params: HashMap<String, Value>) -> Result<QueryResult, GqliteError> {
        let txn_manager = &self.db.txn_manager;

        if self.plan.is_read_only() {
            let mut txn = txn_manager.begin_read_only();
            let mut engine = Engine::with_snapshot(txn.start_ts, params);
            engine.set_db(self.db.clone());
            let result = engine.execute_plan_parallel(&self.plan, &self.db, txn.id);
            match &result {
                Ok(_) => txn_manager.commit(&mut txn),
                Err(_) => txn_manager.rollback(&mut txn),
            }
            result
        } else {
            let (mut txn, _write_guard) = txn_manager.begin_read_write()?;
            let txn_id = txn.id;
            let mut engine = Engine::with_snapshot(txn.start_ts, params);
            engine.set_db(self.db.clone());
            let result = engine.execute_plan(&self.plan, &self.db, txn_id);
            let mut should_checkpoint = false;
            match &result {
                Ok(_) => {
                    let mut wal_guard = self.db.wal.lock();
                    if let Some(wal) = wal_guard.as_mut() {
                        wal.append(&WalRecord { txn_id, payload: WalPayload::TxnCommit })?;
                        should_checkpoint = self.db.config.auto_checkpoint
                            && wal.record_count() >= self.db.config.checkpoint_threshold;
                    }
                    txn_manager.commit(&mut txn);
                }
                Err(_) => {
                    txn_manager.rollback(&mut txn);
                }
            }
            drop(_write_guard);
            if should_checkpoint {
                let _ = checkpoint_impl(&self.db);
            }
            result
        }
    }

    /// Whether the prepared statement is read-only.
    pub fn is_read_only(&self) -> bool {
        self.plan.is_read_only()
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
        Self { columns, rows, cursor: 0 }
    }

    /// Create an empty result (for DDL operations).
    pub fn empty() -> Self {
        Self { columns: Vec::new(), rows: Vec::new(), cursor: 0 }
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
