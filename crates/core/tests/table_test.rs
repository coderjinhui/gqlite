use gqlite_core::catalog::{ColumnDef, NodeTableEntry, RelTableEntry};
use gqlite_core::storage::table::{NodeTable, RelTable};
use gqlite_core::types::data_type::DataType;
use gqlite_core::types::graph::InternalId;
use gqlite_core::types::value::Value;

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
    let id =
        table.insert(&[Value::Int(1), Value::String("Alice".into()), Value::Int(30)], 1).unwrap();
    assert_eq!(id, InternalId::new(0, 0));

    let row = table.read(0).unwrap();
    assert_eq!(row[0], Value::Int(1));
    assert_eq!(row[1], Value::String("Alice".into()));
    assert_eq!(row[2], Value::Int(30));
}

#[test]
fn duplicate_pk() {
    let mut table = NodeTable::new(&person_entry());
    table.insert(&[Value::Int(1), Value::String("A".into()), Value::Int(20)], 1).unwrap();
    let result = table.insert(&[Value::Int(1), Value::String("B".into()), Value::Int(25)], 1);
    assert!(result.is_err());
}

#[test]
fn scan_all() {
    let mut table = NodeTable::new(&person_entry());
    for i in 0..5 {
        table
            .insert(&[Value::Int(i), Value::String(format!("p{}", i)), Value::Int(20 + i)], 1)
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
    table.insert(&[Value::Int(1), Value::String("A".into()), Value::Int(20)], 1).unwrap();
    table.insert(&[Value::Int(2), Value::String("B".into()), Value::Int(25)], 1).unwrap();

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
    table.insert(&[Value::Int(1), Value::String("A".into()), Value::Int(20)], 1).unwrap();

    table.update(0, 1, Value::String("Updated".into()), 2).unwrap();
    let row = table.read(0).unwrap();
    assert_eq!(row[1], Value::String("Updated".into()));
}

#[test]
fn pk_lookup() {
    let mut table = NodeTable::new(&person_entry());
    table.insert(&[Value::Int(42), Value::String("X".into()), Value::Int(99)], 1).unwrap();
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
    rel.insert_rel(InternalId::new(0, 0), InternalId::new(0, 1), &[]).unwrap();
    rel.insert_rel(InternalId::new(0, 0), InternalId::new(0, 2), &[]).unwrap();
    rel.insert_rel(InternalId::new(0, 1), InternalId::new(0, 2), &[]).unwrap();

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
    assert!(table.is_visible(0, 5)); // create_ts=5 <= 5
    assert!(table.is_visible(0, 10)); // create_ts=5 <= 10

    table.delete(0, 8).unwrap();
    assert!(table.is_visible(0, 7)); // delete_ts=8 > 7
    assert!(!table.is_visible(0, 8)); // delete_ts=8 is NOT > 8
    assert!(!table.is_visible(0, 10)); // delete_ts=8 <= 10
}
