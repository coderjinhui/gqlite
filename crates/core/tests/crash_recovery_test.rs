use gqlite_core::Database;
use std::path::PathBuf;

fn temp_db_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("gqlite_crash_recovery_test");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(format!("{}.graph", name))
}

fn cleanup(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
    let wal = path.with_extension("graph.wal");
    let _ = std::fs::remove_file(&wal);
    let lock = path.with_extension("graph.lock");
    let _ = std::fs::remove_file(&lock);
    let tmp = path.with_extension("graph.tmp");
    let _ = std::fs::remove_file(&tmp);
}

// ============================================================
// 1. Committed data survives without checkpoint (WAL recovery)
// ============================================================

#[test]
fn committed_data_recovered_from_wal() {
    let path = temp_db_path("crash_committed_wal");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE A(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE (n:A {id: 1, name: 'Alice'})").unwrap();
        db.execute("CREATE (n:A {id: 2, name: 'Bob'})").unwrap();
        // No explicit checkpoint — rely on WAL
    }

    {
        let db = Database::open(&path).unwrap();
        let r = db.query("MATCH (n:A) RETURN n.id ORDER BY n.id").unwrap();
        assert_eq!(r.num_rows(), 2);
        assert_eq!(r.rows()[0].get_int(0), Some(1));
        assert_eq!(r.rows()[1].get_int(0), Some(2));
    }

    cleanup(&path);
}

// ============================================================
// 2. Committed data survives after checkpoint + additional WAL
// ============================================================

#[test]
fn data_recovered_after_checkpoint_plus_wal() {
    let path = temp_db_path("crash_checkpoint_wal");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE (n:A {id: 1})").unwrap();
        db.execute("CREATE (n:A {id: 2})").unwrap();
        db.checkpoint().unwrap();

        // Additional writes after checkpoint — stored only in WAL
        db.execute("CREATE (n:A {id: 3})").unwrap();
        db.execute("CREATE (n:A {id: 4})").unwrap();
    }

    {
        let db = Database::open(&path).unwrap();
        let r = db.query("MATCH (n:A) RETURN n.id ORDER BY n.id").unwrap();
        assert_eq!(r.num_rows(), 4);
    }

    cleanup(&path);
}

// ============================================================
// 3. UPDATE survives recovery
// ============================================================

#[test]
fn update_survives_recovery() {
    let path = temp_db_path("crash_update");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE A(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE (n:A {id: 1, name: 'Alice'})").unwrap();
        db.execute("MATCH (n:A) WHERE n.id = 1 SET n.name = 'Alicia'").unwrap();
    }

    {
        let db = Database::open(&path).unwrap();
        let r = db.query("MATCH (n:A) WHERE n.id = 1 RETURN n.name").unwrap();
        assert_eq!(r.rows()[0].get_string(0), Some("Alicia"));
    }

    cleanup(&path);
}

// ============================================================
// 4. DELETE survives recovery
// ============================================================

#[test]
fn delete_survives_recovery() {
    let path = temp_db_path("crash_delete");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE (n:A {id: 1})").unwrap();
        db.execute("CREATE (n:A {id: 2})").unwrap();
        db.execute("MATCH (n:A) WHERE n.id = 1 DELETE n").unwrap();
    }

    {
        let db = Database::open(&path).unwrap();
        let r = db.query("MATCH (n:A) RETURN n.id").unwrap();
        assert_eq!(r.num_rows(), 1);
        assert_eq!(r.rows()[0].get_int(0), Some(2));
    }

    cleanup(&path);
}

// ============================================================
// 5. DDL survives recovery
// ============================================================

#[test]
fn ddl_survives_recovery() {
    let path = temp_db_path("crash_ddl");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE NODE TABLE B(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE REL TABLE R(FROM A TO B)").unwrap();
    }

    {
        let db = Database::open(&path).unwrap();
        // Tables should exist
        db.execute("CREATE (n:A {id: 1})").unwrap();
        db.execute("CREATE (n:B {id: 1, name: 'test'})").unwrap();

        let r = db.query("MATCH (n:A) RETURN n.id").unwrap();
        assert_eq!(r.num_rows(), 1);
    }

    cleanup(&path);
}

// ============================================================
// 6. Multiple checkpoint cycles
// ============================================================

#[test]
fn multiple_checkpoint_cycles() {
    let path = temp_db_path("crash_multi_ckpt");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE (n:A {id: 1})").unwrap();
        db.checkpoint().unwrap();

        db.execute("CREATE (n:A {id: 2})").unwrap();
        db.checkpoint().unwrap();

        db.execute("CREATE (n:A {id: 3})").unwrap();
        // No checkpoint for last write
    }

    {
        let db = Database::open(&path).unwrap();
        let r = db.query("MATCH (n:A) RETURN n.id ORDER BY n.id").unwrap();
        assert_eq!(r.num_rows(), 3);
    }

    cleanup(&path);
}

// ============================================================
// 7. WAL truncation after checkpoint
// ============================================================

#[test]
fn wal_truncated_after_checkpoint() {
    let path = temp_db_path("crash_wal_truncate");
    cleanup(&path);

    let wal_path = path.with_extension("graph.wal");

    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
        for i in 0..100 {
            db.execute(&format!("CREATE (n:A {{id: {}}})", i)).unwrap();
        }

        // WAL should be non-empty
        assert!(wal_path.exists());
        let wal_size_before = std::fs::metadata(&wal_path).unwrap().len();
        assert!(wal_size_before > 0);

        // Checkpoint should truncate WAL
        db.checkpoint().unwrap();
    }

    // After checkpoint, WAL is cleared (small header only)
    if wal_path.exists() {
        let wal_size_after = std::fs::metadata(&wal_path).unwrap().len();
        // WAL should be much smaller after checkpoint (just header)
        assert!(
            wal_size_after < 100,
            "WAL should be truncated after checkpoint, got {} bytes",
            wal_size_after
        );
    }

    // Data should still be accessible
    {
        let db = Database::open(&path).unwrap();
        let r = db.query("MATCH (n:A) RETURN n.id").unwrap();
        assert_eq!(r.num_rows(), 100);
    }

    cleanup(&path);
}

// ============================================================
// 8. Empty database recovery
// ============================================================

#[test]
fn empty_database_recovery() {
    let path = temp_db_path("crash_empty");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
        // No data, just schema
    }

    {
        let db = Database::open(&path).unwrap();
        let r = db.query("MATCH (n:A) RETURN n").unwrap();
        assert_eq!(r.num_rows(), 0);

        // Should still be able to insert
        db.execute("CREATE (n:A {id: 1})").unwrap();
        let r = db.query("MATCH (n:A) RETURN n.id").unwrap();
        assert_eq!(r.num_rows(), 1);
    }

    cleanup(&path);
}

// ============================================================
// 9. ALTER TABLE survives recovery
// ============================================================

#[test]
fn alter_table_survives_recovery() {
    let path = temp_db_path("crash_alter");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE (n:A {id: 1})").unwrap();
        db.execute("ALTER TABLE A ADD COLUMN name STRING").unwrap();
        db.execute("MATCH (n:A) WHERE n.id = 1 SET n.name = 'Alice'").unwrap();
    }

    {
        let db = Database::open(&path).unwrap();
        let r = db.query("MATCH (n:A) WHERE n.id = 1 RETURN n.name").unwrap();
        assert_eq!(r.rows()[0].get_string(0), Some("Alice"));
    }

    cleanup(&path);
}

// ============================================================
// 10. Explicit transaction (BEGIN/COMMIT) recovery
// ============================================================

#[test]
fn explicit_transaction_survives_recovery() {
    let path = temp_db_path("crash_explicit_txn");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
        db.execute_script("BEGIN; CREATE (n:A {id: 1}); CREATE (n:A {id: 2}); COMMIT;").unwrap();
    }

    {
        let db = Database::open(&path).unwrap();
        let r = db.query("MATCH (n:A) RETURN n.id ORDER BY n.id").unwrap();
        assert_eq!(r.num_rows(), 2);
    }

    cleanup(&path);
}
