use gqlite_core::Database;

#[test]
fn call_dbms_tables_empty() {
    let db = Database::in_memory();
    let result = db.query("CALL dbms.tables() YIELD name, type").unwrap();
    assert_eq!(result.num_rows(), 0); // no tables yet
    assert_eq!(result.column_names(), vec!["name", "type"]);
}

#[test]
fn call_dbms_tables_with_data() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE REL TABLE KNOWS(FROM Person TO Person)")
        .unwrap();
    let result = db.query("CALL dbms.tables() YIELD name, type").unwrap();
    assert_eq!(result.num_rows(), 2);

    let rows = result.rows();
    // Node table comes first
    assert_eq!(rows[0].get_string(0), Some("Person"));
    assert_eq!(rows[0].get_string(1), Some("NODE"));
    // Then rel table
    assert_eq!(rows[1].get_string(0), Some("KNOWS"));
    assert_eq!(rows[1].get_string(1), Some("REL"));
}

#[test]
fn call_dbms_tables_yield_subset() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id))")
        .unwrap();
    let result = db.query("CALL dbms.tables() YIELD name").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.column_names().len(), 1);
    assert_eq!(result.column_names()[0], "name");
    assert_eq!(result.rows()[0].get_string(0), Some("Person"));
}

#[test]
fn call_unknown_procedure_errors() {
    let db = Database::in_memory();
    let result = db.query("CALL nonexistent() YIELD x");
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("unknown procedure"));
}

#[test]
fn call_no_yield() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))")
        .unwrap();
    let result = db.query("CALL dbms.tables()").unwrap();
    // Without YIELD, return all columns
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.column_names().len(), 2);
    assert_eq!(result.column_names(), vec!["name", "type"]);
}

#[test]
fn call_case_insensitive() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE T(id INT64, PRIMARY KEY(id))")
        .unwrap();
    // CALL and YIELD should be case-insensitive
    let result = db.query("call dbms.tables() yield name").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.column_names()[0], "name");
}

#[test]
fn call_invalid_yield_column() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE T(id INT64, PRIMARY KEY(id))")
        .unwrap();
    let result = db.query("CALL dbms.tables() YIELD nonexistent");
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("does not output column"));
}

#[test]
fn call_multiple_node_and_rel_tables() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE NODE TABLE Movie(id INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE REL TABLE ACTED_IN(FROM Person TO Movie)")
        .unwrap();
    db.execute("CREATE REL TABLE DIRECTED(FROM Person TO Movie)")
        .unwrap();
    let result = db.query("CALL dbms.tables() YIELD name, type").unwrap();
    assert_eq!(result.num_rows(), 4);
}
