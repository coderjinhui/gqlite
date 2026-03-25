use gqlite_core::Database;

fn setup_graph() -> Database {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE KNOWS(FROM Person TO Person)").unwrap();
    // Create: A(1), B(2), C(3), D(4)
    db.execute("CREATE (p:Person {id: 1, name: 'A'})").unwrap();
    db.execute("CREATE (p:Person {id: 2, name: 'B'})").unwrap();
    db.execute("CREATE (p:Person {id: 3, name: 'C'})").unwrap();
    db.execute("CREATE (p:Person {id: 4, name: 'D'})").unwrap();
    // A->B, B->C, C->D, A->D (direct shortcut)
    db.execute("MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:KNOWS]->(b)")
        .unwrap();
    db.execute("MATCH (a:Person), (b:Person) WHERE a.id = 2 AND b.id = 3 CREATE (a)-[:KNOWS]->(b)")
        .unwrap();
    db.execute("MATCH (a:Person), (b:Person) WHERE a.id = 3 AND b.id = 4 CREATE (a)-[:KNOWS]->(b)")
        .unwrap();
    db.execute("MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 4 CREATE (a)-[:KNOWS]->(b)")
        .unwrap();
    db
}

#[test]
fn shortest_path_direct() {
    let db = setup_graph();
    // A->D direct is shortest (1 hop)
    let result = db
        .query(
            "MATCH (a:Person), (b:Person), \
             p = shortestPath((a)-[:KNOWS*..10]->(b)) \
             WHERE a.id = 1 AND b.id = 4 \
             RETURN length(p) AS len",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 1);
}

#[test]
fn shortest_path_multi_hop() {
    let db = setup_graph();
    // B->D: B->C->D (2 hops)
    let result = db
        .query(
            "MATCH (a:Person), (b:Person), \
             p = shortestPath((a)-[:KNOWS*..10]->(b)) \
             WHERE a.id = 2 AND b.id = 4 \
             RETURN length(p) AS len",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 2);
}

#[test]
fn shortest_path_no_path() {
    let db = setup_graph();
    // D->A: no directed path exists
    let result = db
        .query(
            "MATCH (a:Person), (b:Person), \
             p = shortestPath((a)-[:KNOWS*..10]->(b)) \
             WHERE a.id = 4 AND b.id = 1 \
             RETURN p",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 0);
}

#[test]
fn shortest_path_same_node() {
    let db = setup_graph();
    // A->A: path of length 0 (same source and destination)
    let result = db
        .query(
            "MATCH (a:Person), (b:Person), \
             p = shortestPath((a)-[:KNOWS*0..10]->(b)) \
             WHERE a.id = 1 AND b.id = 1 \
             RETURN length(p) AS len",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 0);
}

#[test]
fn shortest_path_returns_path_nodes() {
    let db = setup_graph();
    // B->D: shortest is B->C->D, path should have 3 node IDs
    let result = db
        .query(
            "MATCH (a:Person), (b:Person), \
             p = shortestPath((a)-[:KNOWS*..10]->(b)) \
             WHERE a.id = 2 AND b.id = 4 \
             RETURN p",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    // p is a list; display it to verify it has 3 elements (B, C, D)
    let path_str = format!("{}", result.rows()[0].values[0]);
    assert!(path_str.starts_with('['));
    assert!(path_str.ends_with(']'));
    // Count comma-separated elements: 3 node IDs
    let commas = path_str.matches(',').count();
    assert_eq!(commas, 2, "path should have 3 nodes (2 commas): {}", path_str);
}

#[test]
fn shortest_path_nodes_function() {
    let db = setup_graph();
    // nodes(p) should return the list of node IDs on the path
    let result = db
        .query(
            "MATCH (a:Person), (b:Person), \
             p = shortestPath((a)-[:KNOWS*..10]->(b)) \
             WHERE a.id = 1 AND b.id = 4 \
             RETURN nodes(p) AS n",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let nodes_str = format!("{}", result.rows()[0].values[0]);
    assert!(nodes_str.starts_with('['));
}

#[test]
fn shortest_path_with_where_clause() {
    let db = setup_graph();
    // Use WHERE to filter source/dest nodes
    let result = db
        .query(
            "MATCH (a:Person), (b:Person), \
             p = shortestPath((a)-[:KNOWS*..10]->(b)) \
             WHERE a.id = 1 AND b.id = 4 \
             RETURN length(p) AS len",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 1);
}

#[test]
fn shortest_path_adjacent_nodes() {
    let db = setup_graph();
    // A->B: direct 1-hop connection
    let result = db
        .query(
            "MATCH (a:Person), (b:Person), \
             p = shortestPath((a)-[:KNOWS*..10]->(b)) \
             WHERE a.id = 1 AND b.id = 2 \
             RETURN length(p) AS len",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 1);
}

#[test]
fn shortest_path_three_hops() {
    let db = setup_graph();
    // A->C: A->B->C (2 hops, not going through D)
    let result = db
        .query(
            "MATCH (a:Person), (b:Person), \
             p = shortestPath((a)-[:KNOWS*..10]->(b)) \
             WHERE a.id = 1 AND b.id = 3 \
             RETURN length(p) AS len",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 2);
}
