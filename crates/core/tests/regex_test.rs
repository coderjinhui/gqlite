use gqlite_core::Database;

#[test]
fn regex_basic_match() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, name STRING, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE (n:N {id: 1, name: 'Alice'})").unwrap();
    db.execute("CREATE (n:N {id: 2, name: 'Bob'})").unwrap();
    db.execute("CREATE (n:N {id: 3, name: 'Anna'})").unwrap();
    let result = db
        .query("MATCH (n:N) WHERE n.name =~ 'A.*' RETURN n.name ORDER BY n.name")
        .unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "Alice");
    assert_eq!(result.rows()[1].get_string(0).unwrap(), "Anna");
}

#[test]
fn regex_case_insensitive() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, name STRING, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE (n:N {id: 1, name: 'Alice'})").unwrap();
    let result = db
        .query("MATCH (n:N) WHERE n.name =~ '(?i)alice' RETURN n.name")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn regex_no_match() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, name STRING, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE (n:N {id: 1, name: 'Alice'})").unwrap();
    let result = db
        .query("MATCH (n:N) WHERE n.name =~ '^B.*' RETURN n.name")
        .unwrap();
    assert_eq!(result.num_rows(), 0);
}

#[test]
fn regex_invalid_pattern_errors() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, name STRING, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE (n:N {id: 1, name: 'Alice'})").unwrap();
    let result = db.query("MATCH (n:N) WHERE n.name =~ '[invalid' RETURN n.name");
    assert!(result.is_err());
}
