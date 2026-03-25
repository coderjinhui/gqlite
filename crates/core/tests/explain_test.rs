use gqlite_core::Database;

#[test]
fn explain_basic_scan() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))").unwrap();

    let result = db.query("EXPLAIN MATCH (n:Person) RETURN n.name").unwrap();
    assert!(result.num_rows() > 0, "EXPLAIN should return plan rows");

    let plan_text: String = result.rows().iter().map(|r| r.get_string(0).unwrap()).collect();
    assert!(plan_text.contains("SeqScan"), "plan should contain SeqScan");
    assert!(plan_text.contains("Projection"), "plan should contain Projection");
}

#[test]
fn explain_with_filter() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))").unwrap();

    let result = db.query("EXPLAIN MATCH (n:Person) WHERE n.id = 1 RETURN n").unwrap();
    let plan_text: String = result.rows().iter().map(|r| r.get_string(0).unwrap()).collect();
    assert!(plan_text.contains("Filter"), "plan should contain Filter");
}

#[test]
fn explain_with_join() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE KNOWS(FROM Person TO Person)").unwrap();

    let result =
        db.query("EXPLAIN MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name").unwrap();
    let plan_text: String = result.rows().iter().map(|r| r.get_string(0).unwrap()).collect();
    assert!(plan_text.contains("CsrExpand"), "plan should contain CsrExpand");
}

#[test]
fn explain_ddl() {
    let db = Database::in_memory();
    let result = db.query("EXPLAIN CREATE NODE TABLE Test(id INT64, PRIMARY KEY(id))").unwrap();
    let plan_text: String = result.rows().iter().map(|r| r.get_string(0).unwrap()).collect();
    assert!(plan_text.contains("CreateNodeTable"), "plan should contain CreateNodeTable");
}

#[test]
fn explain_does_not_execute() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();

    // EXPLAIN should NOT actually insert data
    let _ = db.query("EXPLAIN CREATE (n:A {id: 1})").unwrap();

    let r = db.query("MATCH (n:A) RETURN n").unwrap();
    assert_eq!(r.num_rows(), 0, "EXPLAIN should not modify data");
}
