use gqlite_core::{Database, QueryResult};

#[test]
fn open_database() {
    let db = Database::open("test_open.graph");
    assert!(db.is_ok());
}

#[test]
fn query_returns_empty_result() {
    let db = Database::open("test_query.graph").unwrap();
    let result: QueryResult = db.query("MATCH (n) RETURN n").unwrap();
    assert!(result.rows().is_empty());
}
