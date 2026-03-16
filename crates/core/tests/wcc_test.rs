use gqlite_core::Database;

#[test]
fn wcc_single_component() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N)").unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    db.execute("CREATE (n:N {id: 2})").unwrap();
    db.execute("CREATE (n:N {id: 3})").unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:E]->(b)")
        .unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 2 AND b.id = 3 CREATE (a)-[:E]->(b)")
        .unwrap();

    let result = db
        .query("CALL wcc('E') YIELD node_id, component_id")
        .unwrap();
    assert_eq!(result.num_rows(), 3);

    // All nodes should be in the same component
    let c0 = result.rows()[0].get_int(1).unwrap();
    assert_eq!(result.rows()[1].get_int(1).unwrap(), c0);
    assert_eq!(result.rows()[2].get_int(1).unwrap(), c0);
}

#[test]
fn wcc_multiple_components() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N)").unwrap();
    for i in 1..=4 {
        db.execute(&format!("CREATE (n:N {{id: {}}})", i))
            .unwrap();
    }
    // Component 1: nodes 1-2
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:E]->(b)")
        .unwrap();
    // Component 2: nodes 3-4
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 3 AND b.id = 4 CREATE (a)-[:E]->(b)")
        .unwrap();

    let result = db
        .query("CALL wcc('E') YIELD node_id, component_id")
        .unwrap();
    assert_eq!(result.num_rows(), 4);

    let rows = result.rows();
    // Nodes 0,1 (offsets for id 1,2) share one component
    let c_first = rows[0].get_int(1).unwrap();
    assert_eq!(rows[1].get_int(1).unwrap(), c_first);

    // Nodes 2,3 (offsets for id 3,4) share another component
    let c_second = rows[2].get_int(1).unwrap();
    assert_eq!(rows[3].get_int(1).unwrap(), c_second);

    // The two components must be different
    assert_ne!(c_first, c_second);
}

#[test]
fn wcc_isolated_nodes() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N)").unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    db.execute("CREATE (n:N {id: 2})").unwrap();
    // No edges — each node is its own component

    let result = db
        .query("CALL wcc('E') YIELD node_id, component_id")
        .unwrap();
    assert_eq!(result.num_rows(), 2);

    let c0 = result.rows()[0].get_int(1).unwrap();
    let c1 = result.rows()[1].get_int(1).unwrap();
    assert_ne!(c0, c1);
}

#[test]
fn wcc_wrong_table_errors() {
    let db = Database::in_memory();
    let result = db.query("CALL wcc('NonExistent') YIELD node_id, component_id");
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("not found"));
}

#[test]
fn wcc_no_args_errors() {
    let db = Database::in_memory();
    let result = db.query("CALL wcc() YIELD node_id");
    assert!(result.is_err());
}

#[test]
fn wcc_yield_subset() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N)").unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();

    // Only yield component_id
    let result = db
        .query("CALL wcc('E') YIELD component_id")
        .unwrap();
    assert_eq!(result.column_names(), vec!["component_id"]);
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn wcc_chain_graph() {
    // Chain: 1->2->3->4->5 — all should be one component
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N)").unwrap();
    for i in 1..=5 {
        db.execute(&format!("CREATE (n:N {{id: {}}})", i))
            .unwrap();
    }
    for i in 1..5 {
        db.execute(&format!(
            "MATCH (a:N), (b:N) WHERE a.id = {} AND b.id = {} CREATE (a)-[:E]->(b)",
            i,
            i + 1
        ))
        .unwrap();
    }

    let result = db
        .query("CALL wcc('E') YIELD node_id, component_id")
        .unwrap();
    assert_eq!(result.num_rows(), 5);

    // All nodes in same component
    let c0 = result.rows()[0].get_int(1).unwrap();
    for row in result.rows() {
        assert_eq!(row.get_int(1).unwrap(), c0);
    }
}

#[test]
fn wcc_three_components() {
    // Three isolated groups: {1,2}, {3}, {4,5,6}
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N)").unwrap();
    for i in 1..=6 {
        db.execute(&format!("CREATE (n:N {{id: {}}})", i))
            .unwrap();
    }
    // Group 1: 1-2
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:E]->(b)")
        .unwrap();
    // Group 2: node 3 alone (no edges)
    // Group 3: 4-5-6
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 4 AND b.id = 5 CREATE (a)-[:E]->(b)")
        .unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 5 AND b.id = 6 CREATE (a)-[:E]->(b)")
        .unwrap();

    let result = db
        .query("CALL wcc('E') YIELD node_id, component_id")
        .unwrap();
    assert_eq!(result.num_rows(), 6);

    let rows = result.rows();
    // Collect component IDs for each node (sorted by offset: 0..5)
    let comps: Vec<i64> = rows.iter().map(|r| r.get_int(1).unwrap()).collect();

    // Node 0 and 1 (id 1,2) same component
    assert_eq!(comps[0], comps[1]);
    // Node 2 (id 3) alone
    assert_ne!(comps[2], comps[0]);
    assert_ne!(comps[2], comps[3]);
    // Node 3, 4, 5 (id 4,5,6) same component
    assert_eq!(comps[3], comps[4]);
    assert_eq!(comps[4], comps[5]);

    // Count distinct components
    let mut unique: Vec<i64> = comps.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), 3);
}
