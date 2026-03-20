use gqlite_core::Database;

#[test]
fn execute_script_multiple_ddl() {
    let db = Database::in_memory();
    db.execute_script(
        "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY(id)); \
         CREATE NODE TABLE City (id INT64, name STRING, PRIMARY KEY(id));",
    )
    .unwrap();

    let tables = db.node_table_names();
    assert!(tables.contains(&"Person".to_string()), "Person not found: {:?}", tables);
    assert!(tables.contains(&"City".to_string()), "City not found: {:?}", tables);
}

#[test]
fn execute_script_ddl_then_dml() {
    let db = Database::in_memory();
    db.execute_script(
        "CREATE NODE TABLE N(id INT64, name STRING, PRIMARY KEY(id)); \
         CREATE (n:N {id: 1, name: 'Alice'}); \
         CREATE (n:N {id: 2, name: 'Bob'});",
    )
    .unwrap();

    let result = db.query("MATCH (n:N) RETURN n.name ORDER BY n.name").unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "Alice");
    assert_eq!(result.rows()[1].get_string(0).unwrap(), "Bob");
}

#[test]
fn execute_script_stops_on_error() {
    let db = Database::in_memory();
    // First statement succeeds, second references non-existent table
    let result = db.execute_script(
        "CREATE NODE TABLE N(id INT64, PRIMARY KEY(id)); \
         MATCH (n:Bad) RETURN n;",
    );
    // Should fail on the second statement
    assert!(result.is_err());
    // But the first statement should have succeeded
    assert!(db.node_table_names().contains(&"N".to_string()));
}

#[test]
fn execute_script_empty_input() {
    let db = Database::in_memory();
    let result = db.execute_script("").unwrap();
    assert!(result.is_empty());
}

#[test]
fn execute_script_single_statement() {
    let db = Database::in_memory();
    db.execute_script("CREATE NODE TABLE X(id INT64, PRIMARY KEY(id))").unwrap();
    assert!(db.node_table_names().contains(&"X".to_string()));
}
