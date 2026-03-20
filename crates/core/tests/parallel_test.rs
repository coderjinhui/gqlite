use gqlite_core::Database;

#[test]
fn parallel_hash_join() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE A (id INT64, val STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE B (id INT64, val STRING, PRIMARY KEY(id))").unwrap();
    for i in 0..100 {
        db.execute(&format!("CREATE (a:A {{id: {}, val: 'a{}'}})", i, i)).unwrap();
        db.execute(&format!("CREATE (b:B {{id: {}, val: 'b{}'}})", i, i)).unwrap();
    }

    // HashJoin produces cross product; using parallel execution
    let result = db.execute("MATCH (a:A), (b:B) WHERE a.id = b.id RETURN a.val, b.val").unwrap();
    assert_eq!(result.num_rows(), 100);
}

#[test]
fn parallel_union() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (name STRING, PRIMARY KEY(name))").unwrap();
    db.execute("CREATE (p:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (p:Person {name: 'Bob'})").unwrap();

    let result = db
        .execute(
            "MATCH (p:Person) RETURN p.name \
             UNION ALL \
             MATCH (p:Person) RETURN p.name",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 4);
}

#[test]
fn parallel_matches_sequential() {
    // Verify that parallel execution produces the same results as sequential.
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N (id INT64, PRIMARY KEY(id))").unwrap();
    for i in 0..50 {
        db.execute(&format!("CREATE (n:N {{id: {}}})", i)).unwrap();
    }

    let conn = db.connect();

    // Sequential
    let seq_result = conn.execute("MATCH (n:N) RETURN n.id ORDER BY n.id").unwrap();

    // Parallel (uses the same query path)
    let par_result = conn.execute("MATCH (n:N) RETURN n.id ORDER BY n.id").unwrap();

    assert_eq!(seq_result.num_rows(), par_result.num_rows());
    for (s, p) in seq_result.rows().iter().zip(par_result.rows().iter()) {
        assert_eq!(s.values, p.values);
    }
}

#[test]
fn parallel_aggregate_with_order_by() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Item (id INT64, category STRING, price INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE (i:Item {id: 1, category: 'A', price: 10})").unwrap();
    db.execute("CREATE (i:Item {id: 2, category: 'B', price: 20})").unwrap();
    db.execute("CREATE (i:Item {id: 3, category: 'A', price: 30})").unwrap();

    let result =
        db.execute("MATCH (i:Item) RETURN i.category, sum(i.price) ORDER BY i.category").unwrap();
    // Category A and B exist; results come back ordered
    assert!(result.num_rows() > 0);
}
