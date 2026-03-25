use gqlite_core::Database;

#[test]
fn lpa_single_community() {
    // Fully connected triangle: 1-2, 2-3, 1-3 — all should converge to same community
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N)").unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    db.execute("CREATE (n:N {id: 2})").unwrap();
    db.execute("CREATE (n:N {id: 3})").unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:E]->(b)").unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 2 AND b.id = 3 CREATE (a)-[:E]->(b)").unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 3 CREATE (a)-[:E]->(b)").unwrap();

    let result = db.query("CALL label_propagation('E') YIELD node_id, community").unwrap();
    assert_eq!(result.num_rows(), 3);

    // All nodes should share the same community
    let c0 = result.rows()[0].get_int(1).unwrap();
    assert_eq!(result.rows()[1].get_int(1).unwrap(), c0);
    assert_eq!(result.rows()[2].get_int(1).unwrap(), c0);
}

#[test]
fn lpa_two_communities() {
    // Two disconnected groups: {1,2,3} and {4,5,6}
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N)").unwrap();
    for i in 1..=6 {
        db.execute(&format!("CREATE (n:N {{id: {}}})", i)).unwrap();
    }
    // Group 1: triangle 1-2-3
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:E]->(b)").unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 2 AND b.id = 3 CREATE (a)-[:E]->(b)").unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 3 CREATE (a)-[:E]->(b)").unwrap();
    // Group 2: triangle 4-5-6
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 4 AND b.id = 5 CREATE (a)-[:E]->(b)").unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 5 AND b.id = 6 CREATE (a)-[:E]->(b)").unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 4 AND b.id = 6 CREATE (a)-[:E]->(b)").unwrap();

    let result = db.query("CALL label_propagation('E') YIELD node_id, community").unwrap();
    assert_eq!(result.num_rows(), 6);

    let rows = result.rows();
    let comms: Vec<i64> = rows.iter().map(|r| r.get_int(1).unwrap()).collect();

    // Nodes 0,1,2 (id 1,2,3) should share one community
    assert_eq!(comms[0], comms[1]);
    assert_eq!(comms[1], comms[2]);

    // Nodes 3,4,5 (id 4,5,6) should share another community
    assert_eq!(comms[3], comms[4]);
    assert_eq!(comms[4], comms[5]);

    // The two communities must differ
    assert_ne!(comms[0], comms[3]);
}

#[test]
fn lpa_isolated_node() {
    // One isolated node with no edges — community should be itself
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N)").unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    db.execute("CREATE (n:N {id: 2})").unwrap();
    // Only an edge between 1 and 2; node 3 is isolated
    db.execute("CREATE (n:N {id: 3})").unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:E]->(b)").unwrap();

    let result = db.query("CALL label_propagation('E') YIELD node_id, community").unwrap();
    assert_eq!(result.num_rows(), 3);

    let rows = result.rows();
    let comms: Vec<i64> = rows.iter().map(|r| r.get_int(1).unwrap()).collect();

    // Nodes 0 and 1 (id 1,2) should be in same community
    assert_eq!(comms[0], comms[1]);

    // Node 2 (id 3, isolated) should be in its own community
    assert_ne!(comms[2], comms[0]);
}

#[test]
fn lpa_unknown_rel_table_errors() {
    let db = Database::in_memory();
    let result = db.query("CALL label_propagation('NonExistent') YIELD node_id, community");
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("not found"));
}

#[test]
fn lpa_with_yield_filter() {
    // Use WHERE on YIELD to filter by community
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N)").unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    db.execute("CREATE (n:N {id: 2})").unwrap();
    db.execute("CREATE (n:N {id: 3})").unwrap();
    // Triangle so all end up in same community
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:E]->(b)").unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 2 AND b.id = 3 CREATE (a)-[:E]->(b)").unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 3 CREATE (a)-[:E]->(b)").unwrap();

    // First, find the community value
    let result = db.query("CALL label_propagation('E') YIELD node_id, community").unwrap();
    let community_val = result.rows()[0].get_int(1).unwrap();

    // Now filter by that community value
    let filtered = db
        .query(&format!(
            "CALL label_propagation('E') YIELD node_id, community WHERE community = {} RETURN node_id",
            community_val
        ))
        .unwrap();
    // All 3 nodes should be returned since they all share the same community
    assert_eq!(filtered.num_rows(), 3);
}
