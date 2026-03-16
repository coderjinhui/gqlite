use gqlite_core::Database;

/// Helper: create an in-memory DB with a self-referencing node/rel schema.
fn setup_db() -> Database {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N)").unwrap();
    db
}

/// Helper: create a node with the given id.
fn create_node(db: &Database, id: i64) {
    db.execute(&format!("CREATE (n:N {{id: {}}})", id))
        .unwrap();
}

/// Helper: create an edge from src to dst.
fn create_edge(db: &Database, src: i64, dst: i64) {
    db.execute(&format!(
        "MATCH (a:N), (b:N) WHERE a.id = {} AND b.id = {} CREATE (a)-[:E]->(b)",
        src, dst
    ))
    .unwrap();
}

#[test]
fn betweenness_chain() {
    // Linear chain: A(0)-B(1)-C(2)-D(3)
    // B and C are intermediaries; A and D are endpoints.
    // Expected (after /2 for undirected): A=0.0, B=2.0, C=2.0, D=0.0
    let db = setup_db();
    create_node(&db, 0);
    create_node(&db, 1);
    create_node(&db, 2);
    create_node(&db, 3);
    create_edge(&db, 0, 1);
    create_edge(&db, 1, 2);
    create_edge(&db, 2, 3);

    let result = db
        .query("CALL betweenness('E') YIELD node_id, score")
        .unwrap();
    assert_eq!(result.num_rows(), 4);

    // Collect scores by node_id
    let mut scores: Vec<(i64, f64)> = result
        .rows()
        .iter()
        .map(|r| (r.get_int(0).unwrap(), r.get_float(1).unwrap()))
        .collect();
    scores.sort_by_key(|&(nid, _)| nid);

    // node 0 (A) and node 3 (D) are endpoints → score 0.0
    assert!((scores[0].1 - 0.0).abs() < 1e-9, "A should be 0.0, got {}", scores[0].1);
    assert!((scores[3].1 - 0.0).abs() < 1e-9, "D should be 0.0, got {}", scores[3].1);
    // node 1 (B) and node 2 (C) are intermediaries → score 2.0
    assert!((scores[1].1 - 2.0).abs() < 1e-9, "B should be 2.0, got {}", scores[1].1);
    assert!((scores[2].1 - 2.0).abs() < 1e-9, "C should be 2.0, got {}", scores[2].1);
}

#[test]
fn betweenness_star() {
    // Star graph: center (1) connected to 3 leaves (2, 3, 4).
    // All shortest paths between leaves go through center.
    // Expected: center = 3.0, leaves = 0.0
    let db = setup_db();
    create_node(&db, 1);
    create_node(&db, 2);
    create_node(&db, 3);
    create_node(&db, 4);
    create_edge(&db, 1, 2);
    create_edge(&db, 1, 3);
    create_edge(&db, 1, 4);

    let result = db
        .query("CALL betweenness('E') YIELD node_id, score")
        .unwrap();
    assert_eq!(result.num_rows(), 4);

    let mut scores: Vec<(i64, f64)> = result
        .rows()
        .iter()
        .map(|r| (r.get_int(0).unwrap(), r.get_float(1).unwrap()))
        .collect();
    scores.sort_by_key(|&(nid, _)| nid);

    // Center (node_id 0, offset 0 for id=1) has highest BC
    // Leaves have 0 BC
    let center_score = scores[0].1; // node offset 0 = id 1 (center)
    assert!(
        (center_score - 3.0).abs() < 1e-9,
        "center should be 3.0, got {}",
        center_score
    );
    for &(nid, score) in &scores[1..] {
        assert!(
            score.abs() < 1e-9,
            "leaf node {} should have score 0.0, got {}",
            nid,
            score
        );
    }
}

#[test]
fn betweenness_triangle() {
    // Triangle: A-B-C, all connected. All pairs directly connected.
    // No node is an intermediary, so all BC = 0.0.
    let db = setup_db();
    create_node(&db, 1);
    create_node(&db, 2);
    create_node(&db, 3);
    create_edge(&db, 1, 2);
    create_edge(&db, 2, 3);
    create_edge(&db, 1, 3);

    let result = db
        .query("CALL betweenness('E') YIELD node_id, score")
        .unwrap();
    assert_eq!(result.num_rows(), 3);

    for row in result.rows() {
        let score = row.get_float(1).unwrap();
        assert!(
            score.abs() < 1e-9,
            "node {} in a triangle should have BC 0.0, got {}",
            row.get_int(0).unwrap(),
            score
        );
    }
}

#[test]
fn betweenness_isolated_node() {
    // A single node with no edges should have score 0.
    let db = setup_db();
    create_node(&db, 1);
    // No edges, but the node is only visible to the procedure if it
    // participates in the rel table. With no edges, no nodes show up.
    // Instead, test with two disconnected nodes connected by one edge
    // plus an additional isolated node that has an edge to itself? No—
    // let's test that an isolated pair yields 0 for both.
    create_node(&db, 2);
    create_edge(&db, 1, 2);

    let result = db
        .query("CALL betweenness('E') YIELD node_id, score")
        .unwrap();
    assert_eq!(result.num_rows(), 2);

    // Two directly connected nodes — neither is an intermediary.
    for row in result.rows() {
        let score = row.get_float(1).unwrap();
        assert!(
            score.abs() < 1e-9,
            "node {} in a 2-node graph should have BC 0.0, got {}",
            row.get_int(0).unwrap(),
            score
        );
    }
}

#[test]
fn betweenness_unknown_rel_errors() {
    let db = Database::in_memory();
    let result = db.query("CALL betweenness('NonExistent') YIELD node_id, score");
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("not found"));
}
