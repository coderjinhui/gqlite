use gqlite_core::Database;

/// Helper: create a City graph with weighted roads.
fn setup_city_graph() -> Database {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE City(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE ROAD(FROM City TO City, distance DOUBLE)").unwrap();
    db.execute("CREATE (c:City {id: 1, name: 'A'})").unwrap();
    db.execute("CREATE (c:City {id: 2, name: 'B'})").unwrap();
    db.execute("CREATE (c:City {id: 3, name: 'C'})").unwrap();
    // A->B: 1.0, B->C: 2.0, A->C: 10.0
    db.execute(
        "MATCH (a:City), (b:City) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:ROAD {distance: 1.0}]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:City), (b:City) WHERE a.id = 2 AND b.id = 3 CREATE (a)-[:ROAD {distance: 2.0}]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:City), (b:City) WHERE a.id = 1 AND b.id = 3 CREATE (a)-[:ROAD {distance: 10.0}]->(b)",
    )
    .unwrap();
    db
}

#[test]
fn dijkstra_basic() {
    let db = setup_city_graph();
    let result = db.query("CALL dijkstra(1, 3, 'ROAD', 'distance') YIELD path, cost").unwrap();
    assert_eq!(result.num_rows(), 1);
    // Shortest: A->B->C, cost = 3.0 (not A->C which is 10.0)
    assert_eq!(result.rows()[0].get_float(1).unwrap(), 3.0);
}

#[test]
fn dijkstra_no_path() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N, w DOUBLE)").unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    db.execute("CREATE (n:N {id: 2})").unwrap();
    // No edges — expect 0 rows
    let result = db.query("CALL dijkstra(1, 2, 'E', 'w') YIELD path, cost").unwrap();
    assert_eq!(result.num_rows(), 0);
}

#[test]
fn dijkstra_direct_is_shortest() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N, w DOUBLE)").unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    db.execute("CREATE (n:N {id: 2})").unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:E {w: 5.0}]->(b)")
        .unwrap();
    let result = db.query("CALL dijkstra(1, 2, 'E', 'w') YIELD cost").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_float(0).unwrap(), 5.0);
}

#[test]
fn dijkstra_same_source_and_target() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N, w DOUBLE)").unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    // Source == target, cost should be 0.0 with path [1]
    let result = db.query("CALL dijkstra(1, 1, 'E', 'w') YIELD path, cost").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_float(1).unwrap(), 0.0);
}

#[test]
fn dijkstra_path_contains_correct_ids() {
    let db = setup_city_graph();
    let result = db.query("CALL dijkstra(1, 3, 'ROAD', 'distance') YIELD path, cost").unwrap();
    assert_eq!(result.num_rows(), 1);
    // The path should be [1, 2, 3] (city IDs, via A->B->C)
    let path_str = format!("{}", result.rows()[0].values[0]);
    assert!(path_str.contains("1"), "path should contain city 1");
    assert!(path_str.contains("2"), "path should contain city 2");
    assert!(path_str.contains("3"), "path should contain city 3");
}
