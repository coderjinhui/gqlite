use gqlite_core::Database;

#[test]
fn dump_empty_database() {
    let db = Database::in_memory();
    let dump = db.dump().unwrap();
    assert!(dump.is_empty());
}

#[test]
fn dump_schema_only() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))").unwrap();

    let dump = db.dump().unwrap();
    assert!(dump.contains("CREATE NODE TABLE Person"));
    assert!(dump.contains("PRIMARY KEY(id)"));
    assert!(dump.contains("INT64"));
    assert!(dump.contains("STRING"));
}

#[test]
fn dump_with_data() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {id: 2, name: 'Bob'})").unwrap();

    let dump = db.dump().unwrap();
    assert!(dump.contains("CREATE NODE TABLE Person"));
    assert!(dump.contains("CREATE (n:Person"));
    assert!(dump.contains("'Alice'"));
    assert!(dump.contains("'Bob'"));
}

#[test]
fn dump_with_relationships() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE KNOWS(FROM Person TO Person)").unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {id: 2, name: 'Bob'})").unwrap();
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[r:KNOWS]->(b)",
    )
    .unwrap();

    let dump = db.dump().unwrap();
    assert!(dump.contains("CREATE REL TABLE KNOWS"));
    assert!(dump.contains("FROM Person TO Person"));
    assert!(dump.contains("MATCH (a:Person), (b:Person)"));
    assert!(dump.contains("CREATE (a)-[r:KNOWS]->(b)"));
}

#[test]
fn dump_and_restore_roundtrip() {
    // Create database with schema and data
    let db1 = Database::in_memory();
    db1.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db1.execute("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();
    db1.execute("CREATE (n:Person {id: 2, name: 'Bob'})").unwrap();

    // Dump
    let dump = db1.dump().unwrap();

    // Restore into a new database
    let db2 = Database::in_memory();
    db2.execute_script(&dump).unwrap();

    // Verify data
    let r = db2.query("MATCH (n:Person) RETURN n.id ORDER BY n.id").unwrap();
    assert_eq!(r.num_rows(), 2);
    let rows = r.rows();
    assert_eq!(rows[0].get_int(0), Some(1));
    assert_eq!(rows[1].get_int(0), Some(2));
}

#[test]
fn dump_string_with_quotes() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE A(id INT64, val STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:A {id: 1, val: 'it\\'s a test'})").unwrap();

    let dump = db.dump().unwrap();
    // The dump should properly escape the single quote
    assert!(dump.contains("it\\'s a test") || dump.contains("it''s a test"));

    // Should be restorable
    let db2 = Database::in_memory();
    db2.execute_script(&dump).unwrap();
    let r = db2.query("MATCH (n:A) RETURN n.val").unwrap();
    assert_eq!(r.num_rows(), 1);
}
