use gqlite_core::Database;

fn setup_diamond() -> Database {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N)").unwrap();
    // Diamond: A(1) -> B(2) -> D(4), A(1) -> C(3) -> D(4)
    db.execute("CREATE (n:N {id: 1})").unwrap(); // A
    db.execute("CREATE (n:N {id: 2})").unwrap(); // B
    db.execute("CREATE (n:N {id: 3})").unwrap(); // C
    db.execute("CREATE (n:N {id: 4})").unwrap(); // D
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:E]->(b)")
        .unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 2 AND b.id = 4 CREATE (a)-[:E]->(b)")
        .unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 3 CREATE (a)-[:E]->(b)")
        .unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 3 AND b.id = 4 CREATE (a)-[:E]->(b)")
        .unwrap();
    db
}

#[test]
fn all_shortest_paths_diamond() {
    let db = setup_diamond();
    // A->D: two paths of length 2: A->B->D and A->C->D
    let result = db
        .query(
            "MATCH (a:N), (b:N), \
             p = allShortestPaths((a)-[:E*..10]->(b)) \
             WHERE a.id = 1 AND b.id = 4 \
             RETURN length(p) AS len",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 2); // Two paths
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 2);
    assert_eq!(result.rows()[1].get_int(0).unwrap(), 2);
}

#[test]
fn all_shortest_paths_single_path() {
    let db = setup_diamond();
    // B->D: only one path of length 1
    let result = db
        .query(
            "MATCH (a:N), (b:N), \
             p = allShortestPaths((a)-[:E*..10]->(b)) \
             WHERE a.id = 2 AND b.id = 4 \
             RETURN length(p) AS len",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 1);
}

#[test]
fn all_shortest_paths_no_path() {
    let db = setup_diamond();
    let result = db
        .query(
            "MATCH (a:N), (b:N), \
             p = allShortestPaths((a)-[:E*..10]->(b)) \
             WHERE a.id = 4 AND b.id = 1 \
             RETURN p",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 0);
}

#[test]
fn all_shortest_paths_same_node() {
    let db = setup_diamond();
    // Same node: should return one path of length 0
    let result = db
        .query(
            "MATCH (a:N), (b:N), \
             p = allShortestPaths((a)-[:E*..10]->(b)) \
             WHERE a.id = 1 AND b.id = 1 \
             RETURN length(p) AS len",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 0);
}

#[test]
fn all_shortest_paths_multiple_equal_length() {
    // Build a graph with 3 shortest paths of equal length
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N)").unwrap();
    // Nodes 1..6
    for i in 1..=6 {
        db.execute(&format!("CREATE (n:N {{id: {}}})", i)).unwrap();
    }
    // Three paths of length 2 from 1 to 6:
    // 1->2->6, 1->3->6, 1->4->6
    // Plus a longer path: 1->5->2->6 (length 3, should NOT appear)
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:E]->(b)")
        .unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 3 CREATE (a)-[:E]->(b)")
        .unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 4 CREATE (a)-[:E]->(b)")
        .unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 5 CREATE (a)-[:E]->(b)")
        .unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 2 AND b.id = 6 CREATE (a)-[:E]->(b)")
        .unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 3 AND b.id = 6 CREATE (a)-[:E]->(b)")
        .unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 4 AND b.id = 6 CREATE (a)-[:E]->(b)")
        .unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 5 AND b.id = 2 CREATE (a)-[:E]->(b)")
        .unwrap();

    let result = db
        .query(
            "MATCH (a:N), (b:N), \
             p = allShortestPaths((a)-[:E*..10]->(b)) \
             WHERE a.id = 1 AND b.id = 6 \
             RETURN length(p) AS len",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 3); // Three shortest paths of length 2
    for row in result.rows() {
        assert_eq!(row.get_int(0).unwrap(), 2);
    }
}

#[test]
fn shortest_path_still_returns_single() {
    // Verify that shortestPath (not allShortestPaths) still returns only one path
    let db = setup_diamond();
    let result = db
        .query(
            "MATCH (a:N), (b:N), \
             p = shortestPath((a)-[:E*..10]->(b)) \
             WHERE a.id = 1 AND b.id = 4 \
             RETURN length(p) AS len",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1); // Only one path
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 2);
}
