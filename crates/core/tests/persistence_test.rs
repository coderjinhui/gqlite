use gqlite_core::Database;
use std::path::PathBuf;

fn temp_db_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("gqlite_tests");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(format!("{}.graph", name))
}

fn cleanup(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
    let wal = path.with_extension("graph.wal");
    let _ = std::fs::remove_file(&wal);
    // Also clean up .graph.tmp in case of residual from crash tests
    let tmp = path.with_extension("graph.tmp");
    let _ = std::fs::remove_file(&tmp);
}

#[test]
fn persistence_roundtrip() {
    let path = temp_db_path("persist_roundtrip");
    cleanup(&path);

    // Phase 1: Create schema and insert data
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE (p:Person {id: 1, name: 'Alice'})").unwrap();
        db.execute("CREATE (p:Person {id: 2, name: 'Bob'})").unwrap();
        // db goes out of scope — WAL is flushed
    }

    // Phase 2: Reopen and verify data survived
    {
        let db = Database::open(&path).unwrap();
        let result = db.query("MATCH (p:Person) RETURN p.id, p.name").unwrap();
        assert_eq!(result.num_rows(), 2);

        let rows = result.rows();
        let names: Vec<&str> = rows.iter().filter_map(|r| r.get_string(1)).collect();
        assert!(names.contains(&"Alice"));
        assert!(names.contains(&"Bob"));
    }

    cleanup(&path);
}

#[test]
fn persistence_with_relationships() {
    let path = temp_db_path("persist_rels");
    cleanup(&path);

    // Phase 1: Create schema, nodes, and relationships
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE REL TABLE KNOWS(FROM Person TO Person)").unwrap();
        db.execute("CREATE (p:Person {id: 1, name: 'Alice'})").unwrap();
        db.execute("CREATE (p:Person {id: 2, name: 'Bob'})").unwrap();
        db.execute("CREATE (p:Person {id: 3, name: 'Charlie'})").unwrap();
        db.execute(
            "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:KNOWS]->(b)",
        )
        .unwrap();
        db.execute(
            "MATCH (a:Person), (b:Person) WHERE a.id = 2 AND b.id = 3 CREATE (a)-[:KNOWS]->(b)",
        )
        .unwrap();
    }

    // Phase 2: Reopen and verify
    {
        let db = Database::open(&path).unwrap();

        // Nodes survived
        let result = db.query("MATCH (p:Person) RETURN p.id").unwrap();
        assert_eq!(result.num_rows(), 3);

        // Relationships survived
        let result =
            db.query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name").unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    cleanup(&path);
}

#[test]
fn persistence_after_checkpoint() {
    let path = temp_db_path("persist_checkpoint");
    cleanup(&path);

    // Phase 1: Create data and checkpoint
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE (p:Person {id: 1, name: 'Alice'})").unwrap();
        db.execute("CREATE (p:Person {id: 2, name: 'Bob'})").unwrap();

        // Checkpoint rewrites WAL as compact snapshot
        db.checkpoint().unwrap();
    }

    // Phase 2: Reopen from checkpointed WAL
    {
        let db = Database::open(&path).unwrap();
        let result = db.query("MATCH (p:Person) RETURN p.id, p.name").unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    cleanup(&path);
}

#[test]
fn persistence_multiple_sessions() {
    let path = temp_db_path("persist_multi");
    cleanup(&path);

    // Session 1: Create schema
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE (p:Person {id: 1, name: 'Alice'})").unwrap();
    }

    // Session 2: Add more data
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE (p:Person {id: 2, name: 'Bob'})").unwrap();
    }

    // Session 3: Verify all data
    {
        let db = Database::open(&path).unwrap();
        let result = db.query("MATCH (p:Person) RETURN p.id").unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    cleanup(&path);
}

#[test]
fn in_memory_database_no_file() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (p:Person {id: 1, name: 'Alice'})").unwrap();

    let result = db.query("MATCH (p:Person) RETURN p.name").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_string(0), Some("Alice"));
}

#[test]
fn checkpoint_creates_main_file() {
    let path = temp_db_path("ckpt_creates_main");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE (p:Person {id: 1, name: 'Alice'})").unwrap();
        db.checkpoint().unwrap();

        // .graph main file should exist after checkpoint
        assert!(path.exists(), ".graph file should exist after checkpoint");
    }

    cleanup(&path);
}

#[test]
fn recovery_from_main_file_only() {
    let path = temp_db_path("ckpt_main_only");
    cleanup(&path);

    // Phase 1: Create data and checkpoint
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE (p:Person {id: 1, name: 'Alice'})").unwrap();
        db.execute("CREATE (p:Person {id: 2, name: 'Bob'})").unwrap();
        db.checkpoint().unwrap();
    }

    // Delete the WAL — recovery should work from .graph alone
    let wal_path = path.with_extension("graph.wal");
    let _ = std::fs::remove_file(&wal_path);

    // Phase 2: Reopen from .graph only
    {
        let db = Database::open(&path).unwrap();
        let result = db.query("MATCH (p:Person) RETURN p.id, p.name").unwrap();
        assert_eq!(result.num_rows(), 2);

        let rows = result.rows();
        let names: Vec<&str> = rows.iter().filter_map(|r| r.get_string(1)).collect();
        assert!(names.contains(&"Alice"));
        assert!(names.contains(&"Bob"));
    }

    cleanup(&path);
}

#[test]
fn recovery_main_file_plus_incremental_wal() {
    let path = temp_db_path("ckpt_incremental");
    cleanup(&path);

    // Phase 1: Create initial data and checkpoint
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE (p:Person {id: 1, name: 'Alice'})").unwrap();
        db.checkpoint().unwrap();

        // Write more data after checkpoint (goes to WAL only)
        db.execute("CREATE (p:Person {id: 2, name: 'Bob'})").unwrap();
        db.execute("CREATE (p:Person {id: 3, name: 'Charlie'})").unwrap();
    }

    // Phase 2: Reopen — should recover from .graph + incremental WAL
    {
        let db = Database::open(&path).unwrap();
        let result = db.query("MATCH (p:Person) RETURN p.id, p.name").unwrap();
        assert_eq!(result.num_rows(), 3);

        let rows = result.rows();
        let names: Vec<&str> = rows.iter().filter_map(|r| r.get_string(1)).collect();
        assert!(names.contains(&"Alice"));
        assert!(names.contains(&"Bob"));
        assert!(names.contains(&"Charlie"));
    }

    cleanup(&path);
}

#[test]
fn multiple_checkpoints() {
    let path = temp_db_path("ckpt_multiple");
    cleanup(&path);

    // Round 1: Create + checkpoint
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE (p:Person {id: 1, name: 'Alice'})").unwrap();
        db.checkpoint().unwrap();
    }

    // Round 2: Add more data + checkpoint again
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE (p:Person {id: 2, name: 'Bob'})").unwrap();
        db.checkpoint().unwrap();
    }

    // Round 3: Add even more + checkpoint
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE (p:Person {id: 3, name: 'Charlie'})").unwrap();
        db.checkpoint().unwrap();
    }

    // Verify all data survived multiple checkpoints
    {
        let db = Database::open(&path).unwrap();
        let result = db.query("MATCH (p:Person) RETURN p.id, p.name").unwrap();
        assert_eq!(result.num_rows(), 3);

        let rows = result.rows();
        let names: Vec<&str> = rows.iter().filter_map(|r| r.get_string(1)).collect();
        assert!(names.contains(&"Alice"));
        assert!(names.contains(&"Bob"));
        assert!(names.contains(&"Charlie"));
    }

    cleanup(&path);
}

#[test]
fn checkpoint_with_relationships() {
    let path = temp_db_path("ckpt_rels");
    cleanup(&path);

    // Phase 1: Create nodes + relationships, then checkpoint
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE REL TABLE KNOWS(FROM Person TO Person)").unwrap();
        db.execute("CREATE (p:Person {id: 1, name: 'Alice'})").unwrap();
        db.execute("CREATE (p:Person {id: 2, name: 'Bob'})").unwrap();
        db.execute("CREATE (p:Person {id: 3, name: 'Charlie'})").unwrap();
        db.execute(
            "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:KNOWS]->(b)",
        )
        .unwrap();
        db.execute(
            "MATCH (a:Person), (b:Person) WHERE a.id = 2 AND b.id = 3 CREATE (a)-[:KNOWS]->(b)",
        )
        .unwrap();
        db.checkpoint().unwrap();
    }

    // Delete WAL to force recovery from .graph only
    let wal_path = path.with_extension("graph.wal");
    let _ = std::fs::remove_file(&wal_path);

    // Phase 2: Reopen and verify nodes + relationships
    {
        let db = Database::open(&path).unwrap();

        let result = db.query("MATCH (p:Person) RETURN p.id").unwrap();
        assert_eq!(result.num_rows(), 3);

        let result =
            db.query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name").unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    cleanup(&path);
}

#[test]
fn auto_checkpoint_triggers() {
    use gqlite_core::DatabaseConfig;

    let path = temp_db_path("auto_ckpt");
    cleanup(&path);

    // Use a very low threshold to trigger auto-checkpoint
    let config = DatabaseConfig { checkpoint_threshold: 5, ..DatabaseConfig::default() };

    {
        let db = Database::open_with_config(&path, config).unwrap();
        db.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
        // Insert enough rows to exceed threshold (each INSERT = multiple WAL records)
        for i in 1..=10 {
            db.execute(&format!("CREATE (p:Person {{id: {}, name: 'P{}'}})", i, i)).unwrap();
        }
        // Auto-checkpoint should have created .graph file
        assert!(path.exists(), ".graph should exist after auto-checkpoint");
    }

    // Delete WAL, verify data recovers from .graph
    let wal_path = path.with_extension("graph.wal");
    let _ = std::fs::remove_file(&wal_path);

    {
        let db = Database::open(&path).unwrap();
        let result = db.query("MATCH (p:Person) RETURN p.id").unwrap();
        assert!(result.num_rows() > 0, "data should survive via .graph after auto-checkpoint");
    }

    cleanup(&path);
}
