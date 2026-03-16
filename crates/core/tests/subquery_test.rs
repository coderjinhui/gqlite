use gqlite_core::Database;

fn setup() -> Database {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, name STRING, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE (n:N {id: 1, name: 'Alice'})").unwrap();
    db.execute("CREATE (n:N {id: 2, name: 'Bob'})").unwrap();
    db.execute("CREATE (n:N {id: 3, name: 'Charlie'})").unwrap();
    db
}

#[test]
fn subquery_basic() {
    let db = setup();
    let result = db
        .query("CALL { MATCH (n:N) RETURN n.name AS name } RETURN name")
        .unwrap();
    assert_eq!(result.num_rows(), 3);
    let mut names: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get_string(0).unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["Alice", "Bob", "Charlie"]);
}

#[test]
fn subquery_with_limit() {
    let db = setup();
    let result = db
        .query(
            "CALL { MATCH (n:N) RETURN n.name AS name ORDER BY n.name LIMIT 1 } RETURN name",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "Alice");
}

#[test]
fn subquery_with_where_after() {
    let db = setup();
    let result = db
        .query(
            "CALL { MATCH (n:N) RETURN n.id AS id, n.name AS name } WHERE id > 1 RETURN name",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 2);
    let mut names: Vec<String> = result
        .rows()
        .iter()
        .map(|r| r.get_string(0).unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["Bob", "Charlie"]);
}

#[test]
fn subquery_empty_result() {
    let db = setup();
    let result = db
        .query(
            "CALL { MATCH (n:N) WHERE n.id > 999 RETURN n.name AS name } RETURN name",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 0);
}

#[test]
fn subquery_with_preceding_match() {
    let db = setup();
    // Cross-join: each row from outer MATCH paired with subquery result
    let result = db
        .query(
            "MATCH (a:N) CALL { MATCH (b:N) RETURN count(b) AS total } RETURN a.name, total",
        )
        .unwrap();
    // 3 outer rows x 1 subquery row (count = 3) = 3 rows
    assert_eq!(result.num_rows(), 3);
    for row in result.rows() {
        assert_eq!(row.get_int(1).unwrap(), 3);
    }
}
