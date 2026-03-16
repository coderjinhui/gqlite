//! Write-Ahead Log (WAL) for crash recovery and durability.
//!
//! WAL file format (`.graph.wal`):
//! ```text
//! [WAL Header — 8 bytes]
//!   magic:   "GWAL" (4 bytes)
//!   version: u32 LE
//! [Record 0]
//!   record_type: u8
//!   txn_id:      u64 LE
//!   data_len:    u32 LE
//!   data:        [u8; data_len]   (bincode-encoded WalPayload)
//!   checksum:    u32 LE           (CRC32 over type+txn_id+data_len+data)
//! [Record 1]
//!   ...
//! ```

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::GqliteError;
use crate::types::data_type::DataType;
use crate::types::graph::InternalId;
use crate::types::value::Value;

// ── Constants ────────────────────────────────────────────────────

const WAL_MAGIC: [u8; 4] = [b'G', b'W', b'A', b'L'];
const WAL_VERSION: u32 = 1;
pub const WAL_HEADER_SIZE: usize = 8;

// ── Record types ─────────────────────────────────────────────────

/// The payload of a single WAL record, encoding one logical operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalPayload {
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
    InsertNode {
        table_name: String,
        table_id: u32,
        values: Vec<Value>,
    },
    InsertRel {
        rel_table_name: String,
        rel_table_id: u32,
        src: InternalId,
        dst: InternalId,
        properties: Vec<Value>,
    },
    UpdateProperty {
        table_id: u32,
        node_offset: u64,
        col_idx: usize,
        new_value: Value,
    },
    DeleteNode {
        table_id: u32,
        node_offset: u64,
    },
    /// Marks the end of a committed transaction.
    TxnCommit,
}

/// A single WAL record with metadata.
#[derive(Debug, Clone)]
pub struct WalRecord {
    pub txn_id: u64,
    pub payload: WalPayload,
}

// ── WalWriter ────────────────────────────────────────────────────

/// Appends WAL records to a `.graph.wal` file.
pub struct WalWriter {
    writer: BufWriter<File>,
    path: PathBuf,
}

impl WalWriter {
    /// Create a new WAL file, overwriting if it exists.
    pub fn create(path: &Path) -> Result<Self, GqliteError> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(path)?;
        let mut writer = BufWriter::new(file);

        // Write header
        writer.write_all(&WAL_MAGIC)?;
        writer.write_all(&WAL_VERSION.to_le_bytes())?;
        writer.flush()?;

        Ok(Self {
            writer,
            path: path.to_path_buf(),
        })
    }

    /// Open an existing WAL file for appending.
    pub fn open_append(path: &Path) -> Result<Self, GqliteError> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut writer = BufWriter::new(file);
        writer.seek(SeekFrom::End(0))?;
        Ok(Self {
            writer,
            path: path.to_path_buf(),
        })
    }

    /// Append a record and fsync.
    pub fn append(&mut self, record: &WalRecord) -> Result<(), GqliteError> {
        let data = bincode::serialize(&record.payload)
            .map_err(|e| GqliteError::Storage(format!("WAL serialize error: {e}")))?;

        // Build the raw record: type_tag(1) + txn_id(8) + data_len(4) + data
        let mut buf = Vec::with_capacity(1 + 8 + 4 + data.len());
        buf.push(payload_type_tag(&record.payload));
        buf.extend_from_slice(&record.txn_id.to_le_bytes());
        buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&data);

        let checksum = crc32fast::hash(&buf);

        self.writer.write_all(&buf)?;
        self.writer.write_all(&checksum.to_le_bytes())?;
        self.writer.flush()?;

        // fsync to guarantee durability
        self.writer.get_ref().sync_data()?;

        Ok(())
    }

    /// Return the WAL file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Truncate the WAL file, keeping only the header (effectively clearing it).
    pub fn clear(&mut self) -> Result<(), GqliteError> {
        let file = self.writer.get_mut();
        file.set_len(WAL_HEADER_SIZE as u64)?;
        file.seek(SeekFrom::Start(WAL_HEADER_SIZE as u64))?;
        file.sync_all()?;
        Ok(())
    }
}

/// Map payload variant to a numeric tag (for the raw record format).
fn payload_type_tag(p: &WalPayload) -> u8 {
    match p {
        WalPayload::CreateNodeTable { .. } => 1,
        WalPayload::CreateRelTable { .. } => 2,
        WalPayload::DropTable { .. } => 3,
        WalPayload::InsertNode { .. } => 4,
        WalPayload::InsertRel { .. } => 5,
        WalPayload::UpdateProperty { .. } => 6,
        WalPayload::DeleteNode { .. } => 7,
        WalPayload::AlterTableAddColumn { .. } => 11,
        WalPayload::AlterTableDropColumn { .. } => 12,
        WalPayload::AlterTableRenameTable { .. } => 13,
        WalPayload::AlterTableRenameColumn { .. } => 14,
        WalPayload::TxnCommit => 10,
    }
}

// ── WalReader ────────────────────────────────────────────────────

/// Reads WAL records sequentially, validating CRC32 checksums.
pub struct WalReader {
    reader: BufReader<File>,
}

impl WalReader {
    /// Open a WAL file for reading.
    pub fn open(path: &Path) -> Result<Self, GqliteError> {
        let file = OpenOptions::new().read(true).open(path)?;
        let mut reader = BufReader::new(file);

        // Validate header
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if magic != WAL_MAGIC {
            return Err(GqliteError::Storage("invalid WAL magic".into()));
        }
        let mut ver_buf = [0u8; 4];
        reader.read_exact(&mut ver_buf)?;
        let _version = u32::from_le_bytes(ver_buf);

        Ok(Self { reader })
    }

    /// Read all records from the WAL. Stops at EOF or first corrupted record.
    pub fn read_all(&mut self) -> Result<Vec<WalRecord>, GqliteError> {
        let mut records = Vec::new();
        loop {
            match self.read_one() {
                Ok(Some(rec)) => records.push(rec),
                Ok(None) => break,           // EOF
                Err(_) => break,             // corrupted — stop reading
            }
        }
        Ok(records)
    }

    /// Try to read one record. Returns Ok(None) on clean EOF.
    fn read_one(&mut self) -> Result<Option<WalRecord>, GqliteError> {
        // Read type tag (1 byte)
        let mut tag_buf = [0u8; 1];
        match self.reader.read_exact(&mut tag_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }

        // Read txn_id (8 bytes)
        let mut txn_buf = [0u8; 8];
        self.reader.read_exact(&mut txn_buf)?;
        let txn_id = u64::from_le_bytes(txn_buf);

        // Read data_len (4 bytes)
        let mut len_buf = [0u8; 4];
        self.reader.read_exact(&mut len_buf)?;
        let data_len = u32::from_le_bytes(len_buf) as usize;

        // Read data
        let mut data = vec![0u8; data_len];
        self.reader.read_exact(&mut data)?;

        // Read checksum (4 bytes)
        let mut cksum_buf = [0u8; 4];
        self.reader.read_exact(&mut cksum_buf)?;
        let stored_checksum = u32::from_le_bytes(cksum_buf);

        // Verify CRC32
        let mut verify_buf = Vec::with_capacity(1 + 8 + 4 + data_len);
        verify_buf.push(tag_buf[0]);
        verify_buf.extend_from_slice(&txn_buf);
        verify_buf.extend_from_slice(&len_buf);
        verify_buf.extend_from_slice(&data);
        let computed = crc32fast::hash(&verify_buf);

        if computed != stored_checksum {
            return Err(GqliteError::Storage(format!(
                "WAL CRC32 mismatch: expected {stored_checksum:#x}, got {computed:#x}"
            )));
        }

        // Deserialize payload
        let payload: WalPayload = bincode::deserialize(&data)
            .map_err(|e| GqliteError::Storage(format!("WAL deserialize error: {e}")))?;

        Ok(Some(WalRecord { txn_id, payload }))
    }
}

// ── Recovery ─────────────────────────────────────────────────────

use crate::catalog::{ColumnDef, Catalog};
use crate::storage::table::{NodeTable, RelTable};
use crate::Storage;

/// Replay committed WAL records against a Catalog + Storage, rebuilding state.
///
/// Only records belonging to transactions that have a matching `TxnCommit`
/// entry are replayed. Uncommitted transactions are discarded.
pub fn replay_wal(
    records: &[WalRecord],
    catalog: &mut Catalog,
    storage: &mut Storage,
) -> Result<u64, GqliteError> {
    // 1. Find committed transaction IDs
    let committed: std::collections::HashSet<u64> = records
        .iter()
        .filter(|r| matches!(r.payload, WalPayload::TxnCommit))
        .map(|r| r.txn_id)
        .collect();

    let max_committed = committed.iter().copied().max().unwrap_or(0);

    // 2. Replay committed records in order
    for record in records {
        if !committed.contains(&record.txn_id) {
            continue; // skip uncommitted
        }
        replay_single_record(record, catalog, storage)?;
    }

    Ok(max_committed)
}

/// Replay only WAL records with `txn_id > checkpoint_ts` (incremental recovery).
///
/// Used when recovering from a `.graph` main file + WAL: the main file already
/// contains state up to `checkpoint_ts`, so only newer records are needed.
pub fn replay_wal_incremental(
    records: &[WalRecord],
    catalog: &mut Catalog,
    storage: &mut Storage,
    checkpoint_ts: u64,
) -> Result<u64, GqliteError> {
    // Find committed transaction IDs with txn_id > checkpoint_ts
    let committed: std::collections::HashSet<u64> = records
        .iter()
        .filter(|r| matches!(r.payload, WalPayload::TxnCommit) && r.txn_id > checkpoint_ts)
        .map(|r| r.txn_id)
        .collect();

    let max_committed = committed.iter().copied().max().unwrap_or(0);

    for record in records {
        if record.txn_id <= checkpoint_ts {
            continue; // already in main file
        }
        if !committed.contains(&record.txn_id) {
            continue; // uncommitted
        }
        replay_single_record(record, catalog, storage)?;
    }

    Ok(max_committed)
}

/// Replay a single WAL record against catalog + storage.
fn replay_single_record(
    record: &WalRecord,
    catalog: &mut Catalog,
    storage: &mut Storage,
) -> Result<(), GqliteError> {
    match &record.payload {
        WalPayload::CreateNodeTable {
            name,
            columns,
            primary_key,
        } => {
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
        }

        WalPayload::CreateRelTable {
            name,
            from_table,
            to_table,
            columns,
        } => {
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
            let table_id =
                catalog.create_rel_table(name, from_table, to_table, col_defs)?;
            let entry = catalog.get_rel_table(name).unwrap().clone();
            storage.rel_tables.insert(table_id, RelTable::new(&entry));
        }

        WalPayload::DropTable { name } => {
            let table_id = catalog
                .get_node_table(name)
                .map(|e| e.table_id)
                .or_else(|| catalog.get_rel_table(name).map(|e| e.table_id));
            catalog.drop_table(name)?;
            if let Some(id) = table_id {
                storage.node_tables.remove(&id);
                storage.rel_tables.remove(&id);
            }
        }

        WalPayload::InsertNode {
            table_id, values, ..
        } => {
            if let Some(nt) = storage.node_tables.get_mut(table_id) {
                nt.insert(values, record.txn_id)?;
            }
        }

        WalPayload::InsertRel {
            rel_table_id,
            src,
            dst,
            properties,
            ..
        } => {
            if let Some(rt) = storage.rel_tables.get_mut(rel_table_id) {
                rt.insert_rel(*src, *dst, properties)?;
                rt.compact();
            }
        }

        WalPayload::UpdateProperty {
            table_id,
            node_offset,
            col_idx,
            new_value,
        } => {
            if let Some(nt) = storage.node_tables.get_mut(table_id) {
                nt.update(*node_offset, *col_idx, new_value.clone())?;
            }
        }

        WalPayload::DeleteNode {
            table_id,
            node_offset,
        } => {
            if let Some(nt) = storage.node_tables.get_mut(table_id) {
                nt.delete(*node_offset, record.txn_id)?;
            }
        }

        WalPayload::AlterTableAddColumn {
            table_name,
            col_name,
            data_type,
        } => {
            let col_id = catalog
                .get_node_table(table_name)
                .map(|e| e.columns.len() as u32)
                .or_else(|| catalog.get_rel_table(table_name).map(|e| e.columns.len() as u32))
                .unwrap_or(0);
            let col_def = ColumnDef {
                column_id: col_id,
                name: col_name.clone(),
                data_type: data_type.clone(),
                nullable: true,
            };
            let is_node = catalog.get_node_table(table_name).is_some();
            if is_node {
                catalog.add_column_to_node_table(table_name, col_def)?;
                let table_id = catalog.get_node_table(table_name).unwrap().table_id;
                if let Some(nt) = storage.node_tables.get_mut(&table_id) {
                    nt.add_column(data_type);
                }
            } else {
                catalog.add_column_to_rel_table(table_name, col_def)?;
            }
        }

        WalPayload::AlterTableDropColumn {
            table_name,
            col_name,
        } => {
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
                    if let Some(nt) = storage.node_tables.get_mut(&table_id) {
                        nt.drop_column(idx);
                    }
                }
            } else {
                catalog.drop_column_from_rel_table(table_name, col_name)?;
            }
        }

        WalPayload::AlterTableRenameTable {
            old_name,
            new_name,
        } => {
            catalog.rename_table(old_name, new_name)?;
        }

        WalPayload::AlterTableRenameColumn {
            table_name,
            old_col,
            new_col,
        } => {
            let is_node = catalog.get_node_table(table_name).is_some();
            if is_node {
                catalog.rename_column_in_node_table(table_name, old_col, new_col)?;
            } else {
                catalog.rename_column_in_rel_table(table_name, old_col, new_col)?;
            }
        }

        WalPayload::TxnCommit => {
            // No-op during replay — just a marker
        }
    }

    Ok(())
}

/// Return the WAL path for a given database path.
pub fn wal_path_for(db_path: &Path) -> PathBuf {
    let mut p = db_path.as_os_str().to_owned();
    p.push(".wal");
    PathBuf::from(p)
}

// ── Tests ────────────────────────────────────────────────────────

