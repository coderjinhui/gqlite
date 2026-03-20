//! Scenario: storage durability and recovery stress tests.
//!
//! End-to-end tests exercising bulk operations, checkpoint/recovery cycles,
//! transaction semantics, dump/restore, integrity checks, and schema evolution
//! under various conditions.

use gqlite_core::Database;
use std::path::PathBuf;

fn temp_db_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("gqlite_durability_test");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(format!("{}_{}.graph", name, std::process::id()))
}

fn cleanup(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(path.with_extension("graph.wal"));
    let _ = std::fs::remove_file(path.with_extension("graph.lock"));
    let _ = std::fs::remove_file(path.with_extension("graph.tmp"));
}

// ============================================================
// 1. bulk_insert_1000_nodes
// ============================================================

#[test]
fn bulk_insert_1000_nodes() {
    let path = temp_db_path("bulk_1000");
    cleanup(&path);

    let db = Database::open(&path).unwrap();
    db.execute("CREATE NODE TABLE Item(id INT64, val STRING, PRIMARY KEY(id))").unwrap();

    for i in 1..=1000 {
        db.execute(&format!("CREATE (n:Item {{id: {}, val: 'item_{}'}})", i, i)).unwrap();
    }

    let result = db.query("MATCH (n:Item) RETURN COUNT(n)").unwrap();
    assert_eq!(result.rows()[0].get_int(0), Some(1000));

    cleanup(&path);
}

// ============================================================
// 2. bulk_insert_with_relationships
// ============================================================

#[test]
fn bulk_insert_with_relationships() {
    let path = temp_db_path("bulk_rels");
    cleanup(&path);

    let db = Database::open(&path).unwrap();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE E(FROM N TO N)").unwrap();

    // Insert 200 nodes
    for i in 1..=200 {
        db.execute(&format!("CREATE (n:N {{id: {}}})", i)).unwrap();
    }

    // Insert 500 relationships (src -> dst with wrapping)
    for i in 1..=500 {
        let src = (i % 200) + 1;
        let dst = ((i * 7) % 200) + 1;
        if src != dst {
            // Ignore errors from duplicate relationships
            let _ = db.execute(&format!(
                "MATCH (a:N), (b:N) WHERE a.id = {} AND b.id = {} CREATE (a)-[:E]->(b)",
                src, dst
            ));
        }
    }

    // Verify node count
    let result = db.query("MATCH (n:N) RETURN COUNT(n)").unwrap();
    assert_eq!(result.rows()[0].get_int(0), Some(200));

    // Verify relationships exist
    let result = db.query("MATCH (a:N)-[:E]->(b:N) RETURN COUNT(a)").unwrap();
    let rel_count = result.rows()[0].get_int(0).unwrap();
    assert!(rel_count > 0, "should have some relationships");

    cleanup(&path);
}

// ============================================================
// 3. checkpoint_and_recovery
// ============================================================

#[test]
fn checkpoint_and_recovery() {
    let path = temp_db_path("ckpt_recovery");
    cleanup(&path);

    // Phase 1: Write data, checkpoint, write more data
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE A(id INT64, name STRING, PRIMARY KEY(id))").unwrap();

        // Pre-checkpoint data
        for i in 1..=10 {
            db.execute(&format!("CREATE (n:A {{id: {}, name: 'pre_{}'}})", i, i)).unwrap();
        }
        db.checkpoint().unwrap();

        // Post-checkpoint data (WAL only)
        for i in 11..=20 {
            db.execute(&format!("CREATE (n:A {{id: {}, name: 'post_{}'}})", i, i)).unwrap();
        }
    }

    // Phase 2: Reopen and verify both pre- and post-checkpoint data
    {
        let db = Database::open(&path).unwrap();
        let result = db.query("MATCH (n:A) RETURN COUNT(n)").unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(20));

        // Verify pre-checkpoint data
        let result = db.query("MATCH (n:A) WHERE n.id = 5 RETURN n.name").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows()[0].get_string(0), Some("pre_5"));

        // Verify post-checkpoint data
        let result = db.query("MATCH (n:A) WHERE n.id = 15 RETURN n.name").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows()[0].get_string(0), Some("post_15"));
    }

    cleanup(&path);
}

// ============================================================
// 4. recovery_without_checkpoint
// ============================================================

#[test]
fn recovery_without_checkpoint() {
    let path = temp_db_path("no_ckpt_recovery");
    cleanup(&path);

    // Phase 1: Write data without any checkpoint
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE B(id INT64, val STRING, PRIMARY KEY(id))").unwrap();

        for i in 1..=50 {
            db.execute(&format!("CREATE (n:B {{id: {}, val: 'v{}'}})", i, i)).unwrap();
        }
        // No checkpoint — all data is in WAL only
    }

    // Phase 2: Reopen from WAL replay
    {
        let db = Database::open(&path).unwrap();
        let result = db.query("MATCH (n:B) RETURN COUNT(n)").unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(50));

        // Spot-check a few values
        let result = db.query("MATCH (n:B) WHERE n.id = 25 RETURN n.val").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows()[0].get_string(0), Some("v25"));
    }

    cleanup(&path);
}

// ============================================================
// 5. multiple_checkpoint_cycles
// ============================================================

#[test]
fn multiple_checkpoint_cycles() {
    let path = temp_db_path("multi_ckpt");
    cleanup(&path);

    // Cycle 1
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE C(id INT64, cycle INT64, PRIMARY KEY(id))").unwrap();

        for i in 1..=10 {
            db.execute(&format!("CREATE (n:C {{id: {}, cycle: 1}})", i)).unwrap();
        }
        db.checkpoint().unwrap();
    }

    // Cycle 2
    {
        let db = Database::open(&path).unwrap();
        for i in 11..=20 {
            db.execute(&format!("CREATE (n:C {{id: {}, cycle: 2}})", i)).unwrap();
        }
        db.checkpoint().unwrap();
    }

    // Cycle 3
    {
        let db = Database::open(&path).unwrap();
        for i in 21..=30 {
            db.execute(&format!("CREATE (n:C {{id: {}, cycle: 3}})", i)).unwrap();
        }
        db.checkpoint().unwrap();
    }

    // Verify all 30 nodes survived 3 checkpoint cycles
    {
        let db = Database::open(&path).unwrap();
        let result = db.query("MATCH (n:C) RETURN COUNT(n)").unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(30));

        // Verify each cycle's data
        for cycle in 1..=3 {
            let result = db
                .query(&format!("MATCH (n:C) WHERE n.cycle = {} RETURN COUNT(n)", cycle))
                .unwrap();
            assert_eq!(
                result.rows()[0].get_int(0),
                Some(10),
                "cycle {} should have 10 nodes",
                cycle
            );
        }
    }

    cleanup(&path);
}

// ============================================================
// 6. transaction_commit_persistence
// ============================================================

#[test]
fn transaction_commit_persistence() {
    let path = temp_db_path("txn_commit");
    cleanup(&path);

    // Phase 1: Insert via explicit transaction
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE D(id INT64, PRIMARY KEY(id))").unwrap();

        db.execute_script(
            "BEGIN; \
             CREATE (n:D {id: 1}); \
             CREATE (n:D {id: 2}); \
             CREATE (n:D {id: 3}); \
             COMMIT;",
        )
        .unwrap();
    }

    // Phase 2: Reopen and verify
    {
        let db = Database::open(&path).unwrap();
        let result = db.query("MATCH (n:D) RETURN COUNT(n)").unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(3));
    }

    cleanup(&path);
}

// ============================================================
// 7. transaction_rollback_no_trace
// ============================================================

#[test]
fn transaction_rollback_no_trace() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE E(id INT64, PRIMARY KEY(id))").unwrap();

    // Insert one node via auto-commit
    db.execute("CREATE (n:E {id: 1})").unwrap();

    // Rollback should discard new inserts
    db.execute_script(
        "BEGIN; \
         CREATE (n:E {id: 2}); \
         CREATE (n:E {id: 3}); \
         ROLLBACK;",
    )
    .unwrap();

    // Only the original node should remain
    let result = db.query("MATCH (n:E) RETURN COUNT(n)").unwrap();
    assert_eq!(result.rows()[0].get_int(0), Some(1));

    // Verify specific nodes
    let result = db.query("MATCH (n:E) WHERE n.id = 1 RETURN n.id").unwrap();
    assert_eq!(result.num_rows(), 1);

    let result = db.query("MATCH (n:E) WHERE n.id = 2 RETURN n.id").unwrap();
    assert_eq!(result.num_rows(), 0);
}

// ============================================================
// 8. dump_and_restore_full
// ============================================================

#[test]
fn dump_and_restore_full() {
    // Source database with complex data
    let db1 = Database::in_memory();
    db1.execute("CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY KEY(id))")
        .unwrap();
    db1.execute("CREATE NODE TABLE City(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db1.execute("CREATE REL TABLE LIVES_IN(FROM Person TO City)").unwrap();
    db1.execute("CREATE REL TABLE KNOWS(FROM Person TO Person)").unwrap();

    // Insert people
    for i in 1..=20 {
        db1.execute(&format!(
            "CREATE (n:Person {{id: {}, name: 'person_{}', age: {}}})",
            i,
            i,
            20 + i
        ))
        .unwrap();
    }

    // Insert cities
    for i in 1..=5 {
        db1.execute(&format!("CREATE (n:City {{id: {}, name: 'city_{}'}})", i, i)).unwrap();
    }

    // Create relationships: person -> city
    for i in 1..=20 {
        let city_id = (i % 5) + 1;
        db1.execute(&format!(
            "MATCH (p:Person), (c:City) WHERE p.id = {} AND c.id = {} CREATE (p)-[:LIVES_IN]->(c)",
            i, city_id
        ))
        .unwrap();
    }

    // Create relationships: person -> person
    for i in 1..=10 {
        db1.execute(&format!(
            "MATCH (a:Person), (b:Person) WHERE a.id = {} AND b.id = {} CREATE (a)-[:KNOWS]->(b)",
            i,
            i + 10
        ))
        .unwrap();
    }

    // Dump
    let dump = db1.dump().unwrap();
    assert!(!dump.is_empty());

    // Restore into new database
    let db2 = Database::in_memory();
    db2.execute_script(&dump).unwrap();

    // Compare node counts
    let r1 = db1.query("MATCH (p:Person) RETURN COUNT(p)").unwrap();
    let r2 = db2.query("MATCH (p:Person) RETURN COUNT(p)").unwrap();
    assert_eq!(r1.rows()[0].get_int(0), r2.rows()[0].get_int(0));

    let r1 = db1.query("MATCH (c:City) RETURN COUNT(c)").unwrap();
    let r2 = db2.query("MATCH (c:City) RETURN COUNT(c)").unwrap();
    assert_eq!(r1.rows()[0].get_int(0), r2.rows()[0].get_int(0));

    // Compare relationship counts
    let r1 = db1.query("MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN COUNT(p)").unwrap();
    let r2 = db2.query("MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN COUNT(p)").unwrap();
    assert_eq!(r1.rows()[0].get_int(0), r2.rows()[0].get_int(0));

    let r1 = db1.query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN COUNT(a)").unwrap();
    let r2 = db2.query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN COUNT(a)").unwrap();
    assert_eq!(r1.rows()[0].get_int(0), r2.rows()[0].get_int(0));

    // Spot-check: person_5's data
    let r1 = db1.query("MATCH (p:Person) WHERE p.id = 5 RETURN p.name, p.age").unwrap();
    let r2 = db2.query("MATCH (p:Person) WHERE p.id = 5 RETURN p.name, p.age").unwrap();
    assert_eq!(r1.rows()[0].get_string(0), r2.rows()[0].get_string(0));
    assert_eq!(r1.rows()[0].get_int(1), r2.rows()[0].get_int(1));
}

// ============================================================
// 9. integrity_check_clean
// ============================================================

#[test]
fn integrity_check_clean() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE F(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE LINK(FROM F TO F)").unwrap();

    for i in 1..=50 {
        db.execute(&format!("CREATE (n:F {{id: {}, name: 'node_{}'}})", i, i)).unwrap();
    }

    for i in 1..=30 {
        let dst = (i % 50) + 1;
        if i != dst {
            let _ = db.execute(&format!(
                "MATCH (a:F), (b:F) WHERE a.id = {} AND b.id = {} CREATE (a)-[:LINK]->(b)",
                i, dst
            ));
        }
    }

    let issues = db.check();
    assert!(issues.is_empty(), "integrity check found issues: {:?}", issues);
}

// ============================================================
// 10. concurrent_read_after_write
// ============================================================

#[test]
fn concurrent_read_after_write() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE G(id INT64, val INT64, PRIMARY KEY(id))").unwrap();

    // Write a batch of data
    for i in 1..=100 {
        db.execute(&format!("CREATE (n:G {{id: {}, val: {}}})", i, i * 10)).unwrap();
    }

    // Multiple consecutive reads — all should return consistent results
    for _ in 0..20 {
        let result = db.query("MATCH (n:G) RETURN COUNT(n)").unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(100), "read should always return 100 nodes");
    }

    // Spot-check specific values across multiple reads
    for _ in 0..10 {
        let result = db.query("MATCH (n:G) WHERE n.id = 42 RETURN n.val").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows()[0].get_int(0), Some(420));
    }
}

// ============================================================
// 11. stress_mixed_operations
// ============================================================

#[test]
fn stress_mixed_operations() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE H(id INT64, val INT64, PRIMARY KEY(id))").unwrap();

    let mut live_ids: Vec<i64> = Vec::new();
    let mut next_id: i64 = 1;
    let mut insert_count = 0;
    let mut update_count = 0;
    let mut delete_count = 0;
    let mut query_count = 0;

    for step in 0..600 {
        let op = step % 10;
        match op {
            // INSERT (50%)
            0..=4 => {
                db.execute(&format!("CREATE (n:H {{id: {}, val: {}}})", next_id, next_id * 3))
                    .unwrap();
                live_ids.push(next_id);
                next_id += 1;
                insert_count += 1;
            }
            // QUERY (20%)
            5..=6 => {
                let result = db.query("MATCH (n:H) RETURN COUNT(n)").unwrap();
                let count = result.rows()[0].get_int(0).unwrap();
                assert_eq!(count, live_ids.len() as i64, "count mismatch at step {}", step);
                query_count += 1;
            }
            // UPDATE (15%)
            7..=8 => {
                if !live_ids.is_empty() {
                    let target = live_ids[step % live_ids.len()];
                    db.execute(&format!(
                        "MATCH (n:H) WHERE n.id = {} SET n.val = {}",
                        target,
                        target * 100
                    ))
                    .unwrap();
                    update_count += 1;
                }
            }
            // DELETE (15%)
            _ => {
                if !live_ids.is_empty() {
                    let idx = step % live_ids.len();
                    let target = live_ids[idx];
                    db.execute(&format!("MATCH (n:H) WHERE n.id = {} DELETE n", target)).unwrap();
                    live_ids.remove(idx);
                    delete_count += 1;
                }
            }
        }
    }

    // Verify final state
    let result = db.query("MATCH (n:H) RETURN COUNT(n)").unwrap();
    let final_count = result.rows()[0].get_int(0).unwrap();
    assert_eq!(final_count, live_ids.len() as i64);

    // Verify total operations exceed 500
    let total = insert_count + update_count + delete_count + query_count;
    assert!(total >= 500, "total ops {} should be >= 500", total);

    // Integrity check
    let issues = db.check();
    assert!(issues.is_empty(), "integrity issues: {:?}", issues);
}

// ============================================================
// 12. large_string_values
// ============================================================

#[test]
fn large_string_values() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Txt(id INT64, content STRING, PRIMARY KEY(id))").unwrap();

    // Create strings of various lengths
    let short = "hello";
    let medium: String = "x".repeat(500);
    let long: String = "A".repeat(2000);
    let very_long: String = (0..1500).map(|i| (b'a' + (i % 26) as u8) as char).collect();

    db.execute(&format!("CREATE (n:Txt {{id: 1, content: '{}'}})", short)).unwrap();
    db.execute(&format!("CREATE (n:Txt {{id: 2, content: '{}'}})", medium)).unwrap();
    db.execute(&format!("CREATE (n:Txt {{id: 3, content: '{}'}})", long)).unwrap();
    db.execute(&format!("CREATE (n:Txt {{id: 4, content: '{}'}})", very_long)).unwrap();

    // Verify each string was stored and retrieved correctly
    let result = db.query("MATCH (n:Txt) WHERE n.id = 1 RETURN n.content").unwrap();
    assert_eq!(result.rows()[0].get_string(0), Some(short));

    let result = db.query("MATCH (n:Txt) WHERE n.id = 2 RETURN n.content").unwrap();
    assert_eq!(result.rows()[0].get_string(0), Some(medium.as_str()));

    let result = db.query("MATCH (n:Txt) WHERE n.id = 3 RETURN n.content").unwrap();
    assert_eq!(result.rows()[0].get_string(0), Some(long.as_str()));

    let result = db.query("MATCH (n:Txt) WHERE n.id = 4 RETURN n.content").unwrap();
    assert_eq!(result.rows()[0].get_string(0), Some(very_long.as_str()));

    // Verify count
    let result = db.query("MATCH (n:Txt) RETURN COUNT(n)").unwrap();
    assert_eq!(result.rows()[0].get_int(0), Some(4));
}

// ============================================================
// 13. null_handling
// ============================================================

#[test]
fn null_handling() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE NullTest(id INT64, name STRING, score INT64, PRIMARY KEY(id))")
        .unwrap();

    // Insert nodes — some with NULL columns via ALTER + non-SET approach
    // Insert full rows first
    db.execute("CREATE (n:NullTest {id: 1, name: 'Alice', score: 100})").unwrap();
    db.execute("CREATE (n:NullTest {id: 2, name: 'Bob', score: 200})").unwrap();

    // Set some values to NULL via SET
    db.execute("MATCH (n:NullTest) WHERE n.id = 2 SET n.score = NULL").unwrap();

    // Add a node with NULL name
    db.execute("CREATE (n:NullTest {id: 3, score: 300})").unwrap();

    // Test IS NULL filter
    let result = db.query("MATCH (n:NullTest) WHERE n.score IS NULL RETURN n.id").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_int(0), Some(2));

    // Test IS NOT NULL filter
    let result =
        db.query("MATCH (n:NullTest) WHERE n.score IS NOT NULL RETURN n.id ORDER BY n.id").unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.rows()[0].get_int(0), Some(1));
    assert_eq!(result.rows()[1].get_int(0), Some(3));

    // Test IS NULL on name column
    let result = db.query("MATCH (n:NullTest) WHERE n.name IS NULL RETURN n.id").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_int(0), Some(3));

    // Verify total count unchanged
    let result = db.query("MATCH (n:NullTest) RETURN COUNT(n)").unwrap();
    assert_eq!(result.rows()[0].get_int(0), Some(3));
}

// ============================================================
// 14. schema_evolution
// ============================================================

#[test]
fn schema_evolution() {
    let db = Database::in_memory();

    // Step 1: Create table
    db.execute("CREATE NODE TABLE Evolve(id INT64, name STRING, PRIMARY KEY(id))").unwrap();

    // Step 2: Insert data
    db.execute("CREATE (n:Evolve {id: 1, name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Evolve {id: 2, name: 'Bob'})").unwrap();

    // Verify initial state
    let result = db.query("MATCH (n:Evolve) RETURN COUNT(n)").unwrap();
    assert_eq!(result.rows()[0].get_int(0), Some(2));

    // Step 3: ALTER ADD COLUMN
    db.execute("ALTER TABLE Evolve ADD age INT64").unwrap();

    // Existing rows should have NULL for new column
    let result = db.query("MATCH (n:Evolve) WHERE n.age IS NULL RETURN COUNT(n)").unwrap();
    assert_eq!(result.rows()[0].get_int(0), Some(2));

    // Step 4: SET the new column values
    db.execute("MATCH (n:Evolve) WHERE n.id = 1 SET n.age = 30").unwrap();
    db.execute("MATCH (n:Evolve) WHERE n.id = 2 SET n.age = 25").unwrap();

    // Verify updated values
    let result = db.query("MATCH (n:Evolve) WHERE n.id = 1 RETURN n.name, n.age").unwrap();
    assert_eq!(result.rows()[0].get_string(0), Some("Alice"));
    assert_eq!(result.rows()[0].get_int(1), Some(30));

    let result = db.query("MATCH (n:Evolve) WHERE n.id = 2 RETURN n.name, n.age").unwrap();
    assert_eq!(result.rows()[0].get_string(0), Some("Bob"));
    assert_eq!(result.rows()[0].get_int(1), Some(25));

    // Step 5: ALTER DROP COLUMN
    db.execute("ALTER TABLE Evolve DROP COLUMN age").unwrap();

    // Verify remaining columns still work
    let result = db.query("MATCH (n:Evolve) RETURN n.id, n.name ORDER BY n.id").unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.rows()[0].get_int(0), Some(1));
    assert_eq!(result.rows()[0].get_string(1), Some("Alice"));
    assert_eq!(result.rows()[1].get_int(0), Some(2));
    assert_eq!(result.rows()[1].get_string(1), Some("Bob"));
}

// ============================================================
// 15. reopen_multiple_times
// ============================================================

#[test]
fn reopen_multiple_times() {
    let path = temp_db_path("reopen_multi");
    cleanup(&path);

    // Session 0: Create schema
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE R(id INT64, session INT64, PRIMARY KEY(id))").unwrap();
    }

    let mut next_id = 1;

    // 5 cycles of open -> write -> close -> reopen
    for session in 1..=5 {
        // Write phase
        {
            let db = Database::open(&path).unwrap();
            for _ in 0..10 {
                db.execute(&format!("CREATE (n:R {{id: {}, session: {}}})", next_id, session))
                    .unwrap();
                next_id += 1;
            }
        }

        // Immediate reopen + verify cumulative count
        {
            let db = Database::open(&path).unwrap();
            let result = db.query("MATCH (n:R) RETURN COUNT(n)").unwrap();
            let expected = session * 10;
            assert_eq!(
                result.rows()[0].get_int(0),
                Some(expected),
                "after session {}, expected {} nodes",
                session,
                expected
            );
        }
    }

    // Final verification: all 50 nodes present
    {
        let db = Database::open(&path).unwrap();
        let result = db.query("MATCH (n:R) RETURN COUNT(n)").unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(50));

        // Verify each session contributed 10 nodes
        for session in 1..=5 {
            let result = db
                .query(&format!("MATCH (n:R) WHERE n.session = {} RETURN COUNT(n)", session))
                .unwrap();
            assert_eq!(
                result.rows()[0].get_int(0),
                Some(10),
                "session {} should have 10 nodes",
                session
            );
        }

        // Integrity check on final state
        let issues = db.check();
        assert!(issues.is_empty(), "integrity issues: {:?}", issues);
    }

    cleanup(&path);
}

// ============================================================
// 16. concurrent_writers_persist_after_reopen
// ============================================================

#[test]
fn concurrent_writers_persist_after_reopen() {
    let path = temp_db_path("concurrent_writers");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE NODE TABLE ConcurrentItem(id INT64, worker INT64, payload STRING, PRIMARY KEY(id))",
        )
        .unwrap();

        let mut handles = Vec::new();
        for worker in 0..4_i64 {
            let db = db.clone();
            handles.push(std::thread::spawn(move || {
                for offset in 0..50_i64 {
                    let id = worker * 1000 + offset + 1;
                    loop {
                        match db.execute(&format!(
                            "CREATE (n:ConcurrentItem {{id: {}, worker: {}, payload: 'worker{}_item{}'}})",
                            id, worker, worker, offset
                        )) {
                            Ok(_) => break,
                            Err(err)
                                if err
                                    .to_string()
                                    .contains("another write transaction is active") =>
                            {
                                std::thread::yield_now();
                            }
                            Err(err) => panic!("unexpected concurrent write error: {}", err),
                        }
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        db.checkpoint().unwrap();
    }

    {
        let db = Database::open(&path).unwrap();

        let result = db.query("MATCH (n:ConcurrentItem) RETURN COUNT(n)").unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(200));

        for worker in 0..4_i64 {
            let result = db
                .query(&format!(
                    "MATCH (n:ConcurrentItem) WHERE n.worker = {} RETURN COUNT(n)",
                    worker
                ))
                .unwrap();
            assert_eq!(
                result.rows()[0].get_int(0),
                Some(50),
                "worker {} should have inserted 50 rows",
                worker
            );
        }

        let payload =
            db.query("MATCH (n:ConcurrentItem) WHERE n.id = 2001 RETURN n.payload").unwrap();
        assert_eq!(payload.rows()[0].get_string(0), Some("worker2_item0"));

        let issues = db.check();
        assert!(issues.is_empty(), "integrity issues after concurrent writes: {:?}", issues);
    }

    cleanup(&path);
}
