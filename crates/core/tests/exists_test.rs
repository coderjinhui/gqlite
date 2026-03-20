use gqlite_core::Database;

fn setup() -> Database {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE City(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE LIVES_IN(FROM Person TO City)").unwrap();
    db.execute("CREATE REL TABLE WORKS_IN(FROM Person TO City)").unwrap();
    db.execute("CREATE (p:Person {id: 1, name: 'Alice'})").unwrap();
    db.execute("CREATE (p:Person {id: 2, name: 'Bob'})").unwrap();
    db.execute("CREATE (c:City {id: 1, name: 'NYC'})").unwrap();
    // Use WHERE clause for property filters (inline property filters not supported in MATCH)
    db.execute(
        "MATCH (p:Person), (c:City) WHERE p.id = 1 AND c.id = 1 CREATE (p)-[:LIVES_IN]->(c)",
    )
    .unwrap();
    db
}

#[test]
fn exists_basic() {
    let db = setup();
    // Alice has LIVES_IN, Bob doesn't
    let result = db
        .query("MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:LIVES_IN]->(:City) } RETURN p.name")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "Alice");
}

#[test]
fn not_exists() {
    let db = setup();
    let result = db
        .query("MATCH (p:Person) WHERE NOT EXISTS { MATCH (p)-[:LIVES_IN]->(:City) } RETURN p.name")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "Bob");
}

#[test]
fn exists_with_no_matches() {
    let db = setup();
    // Nobody has WORKS_IN relationships, so EXISTS should return false for all
    let result = db
        .query("MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:WORKS_IN]->(:City) } RETURN p.name")
        .unwrap();
    assert_eq!(result.num_rows(), 0);
}

#[test]
fn exists_preserves_outer_columns() {
    let db = setup();
    // Verify that EXISTS doesn't disrupt other columns in the outer query
    let result = db
        .query(
            "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:LIVES_IN]->(:City) } RETURN p.name, p.id",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "Alice");
    assert_eq!(result.rows()[0].get_int(1).unwrap(), 1);
}

#[test]
fn not_exists_with_other_conditions() {
    let db = setup();
    // Combine NOT EXISTS with another WHERE condition
    let result = db
        .query(
            "MATCH (p:Person) WHERE p.id >= 1 AND NOT EXISTS { MATCH (p)-[:LIVES_IN]->(:City) } RETURN p.name",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "Bob");
}
