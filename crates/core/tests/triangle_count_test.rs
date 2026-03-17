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
fn triangle_count_single_triangle() {
    // Triangle: A-B, B-C, A-C (undirected view)
    let db = setup_db();
    create_node(&db, 1);
    create_node(&db, 2);
    create_node(&db, 3);
    create_edge(&db, 1, 2);
    create_edge(&db, 2, 3);
    create_edge(&db, 1, 3);

    let result = db
        .query("CALL triangle_count('E') YIELD node_id, triangles")
        .unwrap();
    assert_eq!(result.num_rows(), 3);

    // Each node participates in exactly 1 triangle
    for row in result.rows() {
        let tri = row.get_int(1).unwrap();
        assert_eq!(
            tri, 1,
            "node {} should have 1 triangle, got {}",
            row.get_int(0).unwrap(),
            tri
        );
    }
}

#[test]
fn triangle_count_no_triangles() {
    // Linear chain: 1->2->3, no closing edge, so no triangles
    let db = setup_db();
    create_node(&db, 1);
    create_node(&db, 2);
    create_node(&db, 3);
    create_edge(&db, 1, 2);
    create_edge(&db, 2, 3);

    let result = db
        .query("CALL triangle_count('E') YIELD node_id, triangles")
        .unwrap();
    assert_eq!(result.num_rows(), 3);

    for row in result.rows() {
        let tri = row.get_int(1).unwrap();
        assert_eq!(
            tri, 0,
            "node {} should have 0 triangles in a chain, got {}",
            row.get_int(0).unwrap(),
            tri
        );
    }
}

#[test]
fn triangle_count_star_graph() {
    // Star: center (1) connects to leaves (2, 3, 4), no leaf-leaf edges.
    // No triangles because leaves are not connected to each other.
    let db = setup_db();
    create_node(&db, 1);
    create_node(&db, 2);
    create_node(&db, 3);
    create_node(&db, 4);
    create_edge(&db, 1, 2);
    create_edge(&db, 1, 3);
    create_edge(&db, 1, 4);

    let result = db
        .query("CALL triangle_count('E') YIELD node_id, triangles")
        .unwrap();
    assert_eq!(result.num_rows(), 4);

    for row in result.rows() {
        let tri = row.get_int(1).unwrap();
        assert_eq!(
            tri, 0,
            "node {} should have 0 triangles in a star, got {}",
            row.get_int(0).unwrap(),
            tri
        );
    }
}

#[test]
fn triangle_count_complete_k4() {
    // K4: 4 nodes, all pairs connected. Each node is in C(3,2)=3 triangles.
    // Total distinct triangles = C(4,3) = 4.
    let db = setup_db();
    create_node(&db, 1);
    create_node(&db, 2);
    create_node(&db, 3);
    create_node(&db, 4);
    // All 6 directed edges (one direction suffices for undirected view,
    // but add all pairs to be thorough)
    create_edge(&db, 1, 2);
    create_edge(&db, 1, 3);
    create_edge(&db, 1, 4);
    create_edge(&db, 2, 3);
    create_edge(&db, 2, 4);
    create_edge(&db, 3, 4);

    let result = db
        .query("CALL triangle_count('E') YIELD node_id, triangles")
        .unwrap();
    assert_eq!(result.num_rows(), 4);

    // Each node participates in exactly 3 triangles
    for row in result.rows() {
        let tri = row.get_int(1).unwrap();
        assert_eq!(
            tri, 3,
            "node {} in K4 should have 3 triangles, got {}",
            row.get_int(0).unwrap(),
            tri
        );
    }

    // Total = sum of per-node counts / 3 (each triangle counted by 3 nodes) = 4
    let total: i64 = result.rows().iter().map(|r| r.get_int(1).unwrap()).sum();
    assert_eq!(total, 12); // 4 triangles * 3 nodes each = 12
}

#[test]
fn triangle_count_unknown_rel_errors() {
    let db = Database::in_memory();
    let result = db.query("CALL triangle_count('NonExistent') YIELD node_id, triangles");
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("not found"));
}
