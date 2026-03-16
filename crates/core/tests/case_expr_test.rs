use gqlite_core::Database;

#[test]
fn case_searched_basic() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, age INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE (p:Person {id: 1, age: 25})").unwrap();
    db.execute("CREATE (p:Person {id: 2, age: 45})").unwrap();
    db.execute("CREATE (p:Person {id: 3, age: 10})").unwrap();
    let result = db
        .query(
            "MATCH (p:Person) RETURN p.id, CASE WHEN p.age >= 18 THEN 'adult' ELSE 'minor' END AS category ORDER BY p.id",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 3);
    assert_eq!(result.rows()[0].get_string(1).unwrap(), "adult");
    assert_eq!(result.rows()[1].get_string(1).unwrap(), "adult");
    assert_eq!(result.rows()[2].get_string(1).unwrap(), "minor");
}

#[test]
fn case_simple_form() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Status(id INT64, code INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE (s:Status {id: 1, code: 1})").unwrap();
    db.execute("CREATE (s:Status {id: 2, code: 2})").unwrap();
    db.execute("CREATE (s:Status {id: 3, code: 99})").unwrap();
    let result = db
        .query(
            "MATCH (s:Status) RETURN s.id, CASE s.code WHEN 1 THEN 'active' WHEN 2 THEN 'inactive' ELSE 'unknown' END AS label ORDER BY s.id",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 3);
    assert_eq!(result.rows()[0].get_string(1).unwrap(), "active");
    assert_eq!(result.rows()[1].get_string(1).unwrap(), "inactive");
    assert_eq!(result.rows()[2].get_string(1).unwrap(), "unknown");
}

#[test]
fn case_no_else_returns_null() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Item(id INT64, val INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE (i:Item {id: 1, val: 5})").unwrap();
    let result = db
        .query("MATCH (i:Item) RETURN CASE WHEN i.val > 100 THEN 'big' END AS label")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert!(result.rows()[0].values[0].is_null());
}

#[test]
fn case_in_where_clause() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, val INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE (n:N {id: 1, val: 10})").unwrap();
    db.execute("CREATE (n:N {id: 2, val: 20})").unwrap();
    let result = db
        .query("MATCH (n:N) WHERE CASE WHEN n.val > 15 THEN true ELSE false END RETURN n.id")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 2);
}
