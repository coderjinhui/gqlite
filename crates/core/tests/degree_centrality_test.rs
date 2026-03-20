use gqlite_core::Database;

#[test]
fn degree_centrality_basic() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N)").unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    db.execute("CREATE (n:N {id: 2})").unwrap();
    db.execute("CREATE (n:N {id: 3})").unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:E]->(b)").unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 3 CREATE (a)-[:E]->(b)").unwrap();

    let result =
        db.query("CALL degree_centrality('E') YIELD node_id, out_degree, in_degree").unwrap();
    assert_eq!(result.num_rows(), 3);

    // Rows are sorted by offset (insertion order): node 1 (offset 0), node 2 (offset 1), node 3 (offset 2)
    let rows = result.rows();

    // Node 1 (offset 0): out=2, in=0
    assert_eq!(rows[0].get_int(0), Some(0)); // node_id = offset 0
    assert_eq!(rows[0].get_int(1), Some(2)); // out_degree
    assert_eq!(rows[0].get_int(2), Some(0)); // in_degree

    // Node 2 (offset 1): out=0, in=1
    assert_eq!(rows[1].get_int(0), Some(1));
    assert_eq!(rows[1].get_int(1), Some(0));
    assert_eq!(rows[1].get_int(2), Some(1));

    // Node 3 (offset 2): out=0, in=1
    assert_eq!(rows[2].get_int(0), Some(2));
    assert_eq!(rows[2].get_int(1), Some(0));
    assert_eq!(rows[2].get_int(2), Some(1));
}

#[test]
fn degree_centrality_isolated_nodes() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N)").unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    db.execute("CREATE (n:N {id: 2})").unwrap();
    // No edges created

    let result =
        db.query("CALL degree_centrality('E') YIELD node_id, out_degree, in_degree").unwrap();
    assert_eq!(result.num_rows(), 2);

    let rows = result.rows();
    // All degrees should be 0
    for row in rows {
        assert_eq!(row.get_int(1), Some(0)); // out_degree
        assert_eq!(row.get_int(2), Some(0)); // in_degree
    }
}

#[test]
fn degree_centrality_wrong_table_errors() {
    let db = Database::in_memory();
    let result = db.query("CALL degree_centrality('NonExistent') YIELD node_id");
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("not found"));
}

#[test]
fn degree_centrality_yield_subset() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N)").unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 1 CREATE (a)-[:E]->(b)").unwrap();

    // Only yield out_degree
    let result = db.query("CALL degree_centrality('E') YIELD out_degree").unwrap();
    assert_eq!(result.column_names(), vec!["out_degree"]);
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_int(0), Some(1));
}

#[test]
fn degree_centrality_bidirectional() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N)").unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    db.execute("CREATE (n:N {id: 2})").unwrap();
    // 1 -> 2 and 2 -> 1
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:E]->(b)").unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 2 AND b.id = 1 CREATE (a)-[:E]->(b)").unwrap();

    let result =
        db.query("CALL degree_centrality('E') YIELD node_id, out_degree, in_degree").unwrap();
    assert_eq!(result.num_rows(), 2);

    let rows = result.rows();
    // Node 1: out=1, in=1
    assert_eq!(rows[0].get_int(1), Some(1));
    assert_eq!(rows[0].get_int(2), Some(1));
    // Node 2: out=1, in=1
    assert_eq!(rows[1].get_int(1), Some(1));
    assert_eq!(rows[1].get_int(2), Some(1));
}

#[test]
fn degree_centrality_no_args_errors() {
    let db = Database::in_memory();
    let result = db.query("CALL degree_centrality() YIELD node_id");
    assert!(result.is_err());
}
