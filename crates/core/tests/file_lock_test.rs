use gqlite_core::{Database, DatabaseConfig};
use std::path::PathBuf;

fn temp_db_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("gqlite_file_lock_test");
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

/// A second write-mode open on the same database should fail.
#[test]
fn second_writer_fails() {
    let path = temp_db_path("lock_second_writer");
    cleanup(&path);

    let db1 = Database::open(&path).unwrap();
    db1.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();

    // Second writer should fail
    let result = Database::open(&path);
    assert!(result.is_err(), "second writer should be rejected");
    let err_msg = format!("{}", result.err().unwrap());
    assert!(err_msg.contains("already opened"), "error should mention lock: {}", err_msg);

    // First db still works
    db1.execute("CREATE (n:A {id: 1})").unwrap();

    cleanup(&path);
}

/// After the first writer is dropped, a new writer can open.
#[test]
fn lock_released_on_drop() {
    let path = temp_db_path("lock_released");
    cleanup(&path);

    // Open and close
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE (n:A {id: 1})").unwrap();
    }

    // Should succeed after first db dropped
    let db2 = Database::open(&path).unwrap();
    let r = db2.query("MATCH (n:A) RETURN n.id").unwrap();
    assert_eq!(r.num_rows(), 1);

    cleanup(&path);
}

/// Multiple read-only opens should succeed concurrently.
#[test]
fn multiple_readers_allowed() {
    let path = temp_db_path("lock_multi_reader");
    cleanup(&path);

    // Create database with data
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE (n:A {id: 1})").unwrap();
    }

    let ro_config = DatabaseConfig { read_only: true, ..Default::default() };

    // Open multiple readers
    let reader1 = Database::open_with_config(&path, ro_config.clone()).unwrap();
    let reader2 = Database::open_with_config(&path, ro_config.clone()).unwrap();

    // Both should be able to query
    let r1 = reader1.query("MATCH (n:A) RETURN n.id").unwrap();
    let r2 = reader2.query("MATCH (n:A) RETURN n.id").unwrap();
    assert_eq!(r1.num_rows(), 1);
    assert_eq!(r2.num_rows(), 1);

    cleanup(&path);
}

/// A writer should block when readers hold shared lock.
#[test]
fn writer_blocked_by_reader() {
    let path = temp_db_path("lock_writer_blocked");
    cleanup(&path);

    // Create database
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
    }

    let ro_config = DatabaseConfig { read_only: true, ..Default::default() };

    // Open a reader
    let _reader = Database::open_with_config(&path, ro_config).unwrap();

    // Writer should fail while reader holds shared lock
    let result = Database::open(&path);
    assert!(result.is_err(), "writer should be blocked by reader");

    cleanup(&path);
}

/// In-memory databases don't use file locks.
#[test]
fn in_memory_no_lock() {
    let db1 = Database::in_memory();
    let db2 = Database::in_memory();

    db1.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
    db2.execute("CREATE NODE TABLE B(id INT64, PRIMARY KEY(id))").unwrap();

    // Both work independently
    db1.execute("CREATE (n:A {id: 1})").unwrap();
    db2.execute("CREATE (n:B {id: 1})").unwrap();
}
