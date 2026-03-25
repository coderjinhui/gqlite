use gqlite_core::Database;

#[test]
fn in_basic_int_list() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    for i in 1..=5 {
        db.execute(&format!("CREATE (n:N {{id: {}}})", i)).unwrap();
    }
    let result = db.query("MATCH (n:N) WHERE n.id IN [2, 4] RETURN n.id ORDER BY n.id").unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 2);
    assert_eq!(result.rows()[1].get_int(0).unwrap(), 4);
}

#[test]
fn not_in_list() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    for i in 1..=5 {
        db.execute(&format!("CREATE (n:N {{id: {}}})", i)).unwrap();
    }
    let result =
        db.query("MATCH (n:N) WHERE n.id NOT IN [2, 4] RETURN n.id ORDER BY n.id").unwrap();
    assert_eq!(result.num_rows(), 3);
}

#[test]
fn in_string_list() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:N {id: 1, name: 'Alice'})").unwrap();
    db.execute("CREATE (n:N {id: 2, name: 'Bob'})").unwrap();
    db.execute("CREATE (n:N {id: 3, name: 'Carol'})").unwrap();
    let result = db
        .query("MATCH (n:N) WHERE n.name IN ['Alice', 'Carol'] RETURN n.name ORDER BY n.name")
        .unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "Alice");
    assert_eq!(result.rows()[1].get_string(0).unwrap(), "Carol");
}

#[test]
fn in_empty_list() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    let result = db.query("MATCH (n:N) WHERE n.id IN [] RETURN n.id").unwrap();
    assert_eq!(result.num_rows(), 0);
}
