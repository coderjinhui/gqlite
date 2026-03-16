use std::fs;
use std::path::{Path, PathBuf};

use gqlite_core::catalog::Catalog;
use gqlite_core::Storage;
use gqlite_core::transaction::wal::{
    replay_wal, wal_path_for, WalPayload, WalReader, WalRecord, WalWriter, WAL_HEADER_SIZE,
};
use gqlite_core::types::data_type::DataType;
use gqlite_core::types::value::Value;

fn temp_wal(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("gqlite_wal_test");
    fs::create_dir_all(&dir).ok();
    dir.join(name)
}

fn cleanup(p: &Path) {
    fs::remove_file(p).ok();
}

#[test]
fn write_and_read_records() {
    let path = temp_wal("test_wr.wal");
    cleanup(&path);

    // Write
    {
        let mut w = WalWriter::create(&path).unwrap();
        w.append(&WalRecord {
            txn_id: 1,
            payload: WalPayload::CreateNodeTable {
                name: "Person".into(),
                columns: vec![
                    ("id".into(), DataType::Int64),
                    ("name".into(), DataType::String),
                ],
                primary_key: "id".into(),
            },
        })
        .unwrap();
        w.append(&WalRecord {
            txn_id: 1,
            payload: WalPayload::InsertNode {
                table_name: "Person".into(),
                table_id: 0,
                values: vec![Value::Int(1), Value::String("Alice".into())],
            },
        })
        .unwrap();
        w.append(&WalRecord {
            txn_id: 1,
            payload: WalPayload::TxnCommit,
        })
        .unwrap();
    }

    // Read
    let mut reader = WalReader::open(&path).unwrap();
    let records = reader.read_all().unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].txn_id, 1);
    assert!(matches!(
        records[0].payload,
        WalPayload::CreateNodeTable { .. }
    ));
    assert!(matches!(records[2].payload, WalPayload::TxnCommit));

    cleanup(&path);
}

#[test]
fn crc32_detects_corruption() {
    let path = temp_wal("test_crc.wal");
    cleanup(&path);

    {
        let mut w = WalWriter::create(&path).unwrap();
        w.append(&WalRecord {
            txn_id: 1,
            payload: WalPayload::TxnCommit,
        })
        .unwrap();
    }

    // Corrupt the file: flip a byte in the data area
    {
        let mut data = fs::read(&path).unwrap();
        // The record starts at offset 8 (after header)
        // Corrupt the txn_id area
        if data.len() > 10 {
            data[9] ^= 0xFF;
        }
        fs::write(&path, data).unwrap();
    }

    let mut reader = WalReader::open(&path).unwrap();
    let records = reader.read_all().unwrap();
    // Should stop at corrupted record
    assert_eq!(records.len(), 0);

    cleanup(&path);
}

#[test]
fn replay_committed_only() {
    let path = temp_wal("test_replay.wal");
    cleanup(&path);

    {
        let mut w = WalWriter::create(&path).unwrap();
        // Txn 1: committed
        w.append(&WalRecord {
            txn_id: 1,
            payload: WalPayload::CreateNodeTable {
                name: "Person".into(),
                columns: vec![("id".into(), DataType::Int64)],
                primary_key: "id".into(),
            },
        })
        .unwrap();
        w.append(&WalRecord {
            txn_id: 1,
            payload: WalPayload::TxnCommit,
        })
        .unwrap();

        // Txn 2: NOT committed (simulating crash)
        w.append(&WalRecord {
            txn_id: 2,
            payload: WalPayload::CreateNodeTable {
                name: "Orphan".into(),
                columns: vec![("id".into(), DataType::Int64)],
                primary_key: "id".into(),
            },
        })
        .unwrap();
        // No commit for txn 2
    }

    let mut reader = WalReader::open(&path).unwrap();
    let records = reader.read_all().unwrap();

    let mut catalog = Catalog::new();
    let mut storage = Storage::new();
    replay_wal(&records, &mut catalog, &mut storage).unwrap();

    // Only "Person" should exist, not "Orphan"
    assert!(catalog.get_node_table("Person").is_some());
    assert!(catalog.get_node_table("Orphan").is_none());
    assert_eq!(storage.node_tables.len(), 1);

    cleanup(&path);
}

#[test]
fn replay_insert_and_delete() {
    let path = temp_wal("test_replay_crud.wal");
    cleanup(&path);

    {
        let mut w = WalWriter::create(&path).unwrap();
        // Create table
        w.append(&WalRecord {
            txn_id: 1,
            payload: WalPayload::CreateNodeTable {
                name: "Person".into(),
                columns: vec![
                    ("id".into(), DataType::Int64),
                    ("name".into(), DataType::String),
                ],
                primary_key: "id".into(),
            },
        })
        .unwrap();
        w.append(&WalRecord {
            txn_id: 1,
            payload: WalPayload::TxnCommit,
        })
        .unwrap();

        // Insert two rows
        w.append(&WalRecord {
            txn_id: 2,
            payload: WalPayload::InsertNode {
                table_name: "Person".into(),
                table_id: 0,
                values: vec![Value::Int(1), Value::String("Alice".into())],
            },
        })
        .unwrap();
        w.append(&WalRecord {
            txn_id: 2,
            payload: WalPayload::InsertNode {
                table_name: "Person".into(),
                table_id: 0,
                values: vec![Value::Int(2), Value::String("Bob".into())],
            },
        })
        .unwrap();
        w.append(&WalRecord {
            txn_id: 2,
            payload: WalPayload::TxnCommit,
        })
        .unwrap();

        // Delete first row
        w.append(&WalRecord {
            txn_id: 3,
            payload: WalPayload::DeleteNode {
                table_id: 0,
                node_offset: 0,
            },
        })
        .unwrap();
        w.append(&WalRecord {
            txn_id: 3,
            payload: WalPayload::TxnCommit,
        })
        .unwrap();
    }

    let mut reader = WalReader::open(&path).unwrap();
    let records = reader.read_all().unwrap();

    let mut catalog = Catalog::new();
    let mut storage = Storage::new();
    replay_wal(&records, &mut catalog, &mut storage).unwrap();

    let nt = storage.node_tables.get(&0).unwrap();
    // Only Bob should remain
    let rows: Vec<_> = nt.scan().collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1[1], Value::String("Bob".into()));

    cleanup(&path);
}

#[test]
fn clear_wal() {
    let path = temp_wal("test_clear.wal");
    cleanup(&path);

    let mut w = WalWriter::create(&path).unwrap();
    w.append(&WalRecord {
        txn_id: 1,
        payload: WalPayload::TxnCommit,
    })
    .unwrap();

    // File should be larger than header
    let size_before = fs::metadata(&path).unwrap().len();
    assert!(size_before > WAL_HEADER_SIZE as u64);

    w.clear().unwrap();

    let size_after = fs::metadata(&path).unwrap().len();
    assert_eq!(size_after, WAL_HEADER_SIZE as u64);

    // Reading should return zero records
    let mut reader = WalReader::open(&path).unwrap();
    let records = reader.read_all().unwrap();
    assert_eq!(records.len(), 0);

    cleanup(&path);
}

#[test]
fn wal_path_for_works() {
    let p = wal_path_for(Path::new("/tmp/test.graph"));
    assert_eq!(p, PathBuf::from("/tmp/test.graph.wal"));
}
