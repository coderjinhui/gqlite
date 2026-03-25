use gqlite_core::Database;

#[test]
fn pagerank_simple_chain() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N)").unwrap();
    // A -> B -> C
    db.execute("CREATE (n:N {id: 1})").unwrap();
    db.execute("CREATE (n:N {id: 2})").unwrap();
    db.execute("CREATE (n:N {id: 3})").unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:E]->(b)").unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 2 AND b.id = 3 CREATE (a)-[:E]->(b)").unwrap();

    let result = db.query("CALL pagerank('E') YIELD node_id, score").unwrap();
    assert_eq!(result.num_rows(), 3);

    // Collect scores by node_id
    let rows = result.rows();
    let score_1 = rows[0].get_float(1).unwrap();
    let score_2 = rows[1].get_float(1).unwrap();
    let score_3 = rows[2].get_float(1).unwrap();

    // C (sink node) should have highest score
    assert!(
        score_3 > score_2,
        "sink node C should have higher score than B: {} vs {}",
        score_3,
        score_2
    );
    assert!(
        score_2 > score_1,
        "node B should have higher score than source A: {} vs {}",
        score_2,
        score_1
    );
}

#[test]
fn pagerank_isolated_nodes() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N)").unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    db.execute("CREATE (n:N {id: 2})").unwrap();

    let result = db.query("CALL pagerank('E') YIELD node_id, score").unwrap();
    assert_eq!(result.num_rows(), 2);

    // Both should have equal score (1/N teleport only)
    let s1 = result.rows()[0].get_float(1).unwrap();
    let s2 = result.rows()[1].get_float(1).unwrap();
    assert!((s1 - s2).abs() < 0.01, "isolated nodes should have equal score: {} vs {}", s1, s2);
}

#[test]
fn pagerank_scores_sum_to_one() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N)").unwrap();
    for i in 1..=4 {
        db.execute(&format!("CREATE (n:N {{id: {}}})", i)).unwrap();
    }
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:E]->(b)").unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 2 AND b.id = 3 CREATE (a)-[:E]->(b)").unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 3 AND b.id = 1 CREATE (a)-[:E]->(b)").unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 4 CREATE (a)-[:E]->(b)").unwrap();

    let result = db.query("CALL pagerank('E') YIELD node_id, score").unwrap();
    let total: f64 = result.rows().iter().map(|r| r.get_float(1).unwrap()).sum();
    assert!((total - 1.0).abs() < 0.01, "scores should sum to ~1.0, got {}", total);
}

#[test]
fn pagerank_wrong_table_errors() {
    let db = Database::in_memory();
    let result = db.query("CALL pagerank('NonExistent') YIELD node_id, score");
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("not found"));
}

#[test]
fn pagerank_no_args_errors() {
    let db = Database::in_memory();
    let result = db.query("CALL pagerank() YIELD node_id");
    assert!(result.is_err());
}

#[test]
fn pagerank_yield_subset() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N)").unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();

    // Only yield score
    let result = db.query("CALL pagerank('E') YIELD score").unwrap();
    assert_eq!(result.column_names(), vec!["score"]);
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn pagerank_cycle_graph() {
    // Cycle: 1->2->3->1 — all nodes should have equal score
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N)").unwrap();
    for i in 1..=3 {
        db.execute(&format!("CREATE (n:N {{id: {}}})", i)).unwrap();
    }
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:E]->(b)").unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 2 AND b.id = 3 CREATE (a)-[:E]->(b)").unwrap();
    db.execute("MATCH (a:N), (b:N) WHERE a.id = 3 AND b.id = 1 CREATE (a)-[:E]->(b)").unwrap();

    let result = db.query("CALL pagerank('E') YIELD node_id, score").unwrap();
    assert_eq!(result.num_rows(), 3);

    let scores: Vec<f64> = result.rows().iter().map(|r| r.get_float(1).unwrap()).collect();

    // In a symmetric cycle, all scores should be equal (~1/3)
    for s in &scores {
        assert!((s - 1.0 / 3.0).abs() < 0.01, "cycle node should have score ~1/3, got {}", s);
    }
}
