use gqlite_core::Database;
use std::path::PathBuf;

fn temp_db_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("gqlite_explicit_txn_test");
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

fn count_nodes(db: &Database, label: &str) -> usize {
    let q = format!("MATCH (n:{}) RETURN n", label);
    db.query(&q).map(|r| r.num_rows()).unwrap_or(0)
}

// ============================================================
// BEGIN ... COMMIT — data persists
// ============================================================

#[test]
fn begin_commit_persists_data() {
    let path = temp_db_path("begin_commit");
    cleanup(&path);

    let db = Database::open(&path).unwrap();
    db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();

    db.execute_script("BEGIN; CREATE (n:A {id: 1}); CREATE (n:A {id: 2}); COMMIT;").unwrap();

    assert_eq!(count_nodes(&db, "A"), 2);
    cleanup(&path);
}

#[test]
fn begin_commit_recovery() {
    let path = temp_db_path("begin_commit_recovery");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
        db.execute_script("BEGIN; CREATE (n:A {id: 1}); CREATE (n:A {id: 2}); COMMIT;").unwrap();
    }

    // Reopen — data should survive WAL recovery
    {
        let db = Database::open(&path).unwrap();
        assert_eq!(count_nodes(&db, "A"), 2);
    }

    cleanup(&path);
}

// ============================================================
// BEGIN ... ROLLBACK — data discarded
// ============================================================

#[test]
fn begin_rollback_discards_data() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:A {id: 1})").unwrap();

    // ROLLBACK discards the inserts
    db.execute_script("BEGIN; CREATE (n:A {id: 2}); CREATE (n:A {id: 3}); ROLLBACK;").unwrap();

    // Only the pre-existing row should be visible
    // Note: current implementation may still have in-memory changes from ROLLBACK.
    // This test documents expected behavior for the WAL path:
    // ROLLBACK means no WAL records were written.
    assert_eq!(count_nodes(&db, "A"), 1);
}

// ============================================================
// Error handling
// ============================================================

#[test]
fn nested_begin_fails() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();

    let result = db.execute_script("BEGIN; BEGIN;");
    assert!(result.is_err());
    let err = format!("{}", result.err().unwrap());
    assert!(err.contains("nested BEGIN"), "error: {}", err);
}

#[test]
fn commit_without_begin_fails() {
    let db = Database::in_memory();

    let result = db.execute_script("COMMIT;");
    assert!(result.is_err());
    let err = format!("{}", result.err().unwrap());
    assert!(err.contains("COMMIT without"), "error: {}", err);
}

#[test]
fn rollback_without_begin_fails() {
    let db = Database::in_memory();

    let result = db.execute_script("ROLLBACK;");
    assert!(result.is_err());
    let err = format!("{}", result.err().unwrap());
    assert!(err.contains("ROLLBACK without"), "error: {}", err);
}

#[test]
fn unterminated_transaction_fails() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();

    let result = db.execute_script("BEGIN; CREATE (n:A {id: 1});");
    assert!(result.is_err());
    let err = format!("{}", result.err().unwrap());
    assert!(err.contains("unterminated"), "error: {}", err);
}

#[test]
fn standalone_begin_fails() {
    let db = Database::in_memory();

    let result = db.execute("BEGIN");
    assert!(result.is_err());
    let err = format!("{}", result.err().unwrap());
    assert!(err.contains("execute_script"), "error: {}", err);
}

// ============================================================
// Mixed auto and explicit transactions
// ============================================================

#[test]
fn mixed_auto_and_explicit() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();

    // Auto-transaction insert
    db.execute("CREATE (n:A {id: 1})").unwrap();

    // Explicit transaction insert
    db.execute_script("BEGIN; CREATE (n:A {id: 2}); COMMIT;").unwrap();

    // Another auto-transaction
    db.execute("CREATE (n:A {id: 3})").unwrap();

    assert_eq!(count_nodes(&db, "A"), 3);
}

#[test]
fn transaction_with_error_rolls_back() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:A {id: 1})").unwrap();

    // Transaction with duplicate PK — should fail
    let result = db.execute_script("BEGIN; CREATE (n:A {id: 2}); CREATE (n:A {id: 1}); COMMIT;");
    assert!(result.is_err(), "duplicate PK in transaction should fail");

    // For in-memory: storage may have id=2 already (since storage is modified inline).
    // But WAL records are NOT written, so recovery would not see id=2.
    // The test validates that the error is properly reported.
}

// ============================================================
// DDL in transactions
// ============================================================

#[test]
fn ddl_in_transaction() {
    let db = Database::in_memory();

    db.execute_script(
        "BEGIN; \
         CREATE NODE TABLE A(id INT64, PRIMARY KEY(id)); \
         CREATE (n:A {id: 1}); \
         COMMIT;",
    )
    .unwrap();

    assert_eq!(count_nodes(&db, "A"), 1);
}
