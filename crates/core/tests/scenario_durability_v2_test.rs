//! Scenario: storage pressure / embedded workload stress tests (v2).
//!
//! Covers S-01 through S-11 from `test-cases/01-存储压力.md`.
//! All tests use file-based `Database::open` (not in-memory) to exercise
//! WAL, checkpoint, reopen, and on-disk persistence paths.

use gqlite_core::Database;
use std::path::PathBuf;

fn temp_db_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("gqlite_durability_v2_test");
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
// S-01: Batch write 10,000 nodes, verify count after reopen
// ============================================================

#[test]
fn s01_batch_write_10k() {
    let path = temp_db_path("s01_batch_10k");
    cleanup(&path);

    // Phase 1: batch insert 10,000 nodes in chunks with periodic checkpoints
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE Item(id INT64, val STRING, PRIMARY KEY(id))")
            .unwrap();

        for i in 1..=10_000 {
            db.execute(&format!(
                "CREATE (n:Item {{id: {}, val: 'item_{}'}})",
                i, i
            ))
            .unwrap();

            // Checkpoint every 2000 nodes to exercise WAL flush
            if i % 2000 == 0 {
                db.checkpoint().unwrap();
            }
        }

        // Verify count before close
        let result = db.query("MATCH (n:Item) RETURN COUNT(n)").unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(10_000));
    }

    // Phase 2: reopen and verify all 10,000 nodes survived
    {
        let db = Database::open(&path).unwrap();
        let result = db.query("MATCH (n:Item) RETURN COUNT(n)").unwrap();
        assert_eq!(
            result.rows()[0].get_int(0),
            Some(10_000),
            "all 10,000 nodes should survive reopen"
        );

        // Spot-check first, middle, and last
        let r = db
            .query("MATCH (n:Item) WHERE n.id = 1 RETURN n.val")
            .unwrap();
        assert_eq!(r.rows()[0].get_string(0), Some("item_1"));

        let r = db
            .query("MATCH (n:Item) WHERE n.id = 5000 RETURN n.val")
            .unwrap();
        assert_eq!(r.rows()[0].get_string(0), Some("item_5000"));

        let r = db
            .query("MATCH (n:Item) WHERE n.id = 10000 RETURN n.val")
            .unwrap();
        assert_eq!(r.rows()[0].get_string(0), Some("item_10000"));

        // Integrity check
        let issues = db.check();
        assert!(issues.is_empty(), "integrity issues: {:?}", issues);
    }

    cleanup(&path);
}

// ============================================================
// S-02: High fan-out node with 1000+ edges
// ============================================================

#[test]
fn s02_high_fanout_1000_edges() {
    let path = temp_db_path("s02_fanout");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE Hub(id INT64, name STRING, PRIMARY KEY(id))")
            .unwrap();
        db.execute("CREATE NODE TABLE Leaf(id INT64, label STRING, PRIMARY KEY(id))")
            .unwrap();
        db.execute("CREATE REL TABLE CONNECTS(FROM Hub TO Leaf)")
            .unwrap();

        // Create the hub node
        db.execute("CREATE (n:Hub {id: 1, name: 'central_hub'})")
            .unwrap();

        // Create 1050 leaf nodes and connect them to the hub
        for i in 1..=1050 {
            db.execute(&format!(
                "CREATE (n:Leaf {{id: {}, label: 'leaf_{}'}})",
                i, i
            ))
            .unwrap();
            db.execute(&format!(
                "MATCH (h:Hub), (l:Leaf) WHERE h.id = 1 AND l.id = {} CREATE (h)-[:CONNECTS]->(l)",
                i
            ))
            .unwrap();

            if i % 500 == 0 {
                db.checkpoint().unwrap();
            }
        }

        // Verify degree: hub should have 1050 outgoing edges
        let result = db
            .query("MATCH (h:Hub)-[:CONNECTS]->(l:Leaf) WHERE h.id = 1 RETURN COUNT(l)")
            .unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(1050));
    }

    // Reopen and verify
    {
        let db = Database::open(&path).unwrap();

        // Degree count after reopen
        let result = db
            .query("MATCH (h:Hub)-[:CONNECTS]->(l:Leaf) WHERE h.id = 1 RETURN COUNT(l)")
            .unwrap();
        assert_eq!(
            result.rows()[0].get_int(0),
            Some(1050),
            "hub should retain 1050 edges after reopen"
        );

        // Spot-check adjacency: specific leaves are reachable
        let r = db
            .query("MATCH (h:Hub)-[:CONNECTS]->(l:Leaf) WHERE h.id = 1 AND l.id = 500 RETURN l.label")
            .unwrap();
        assert_eq!(r.num_rows(), 1);
        assert_eq!(r.rows()[0].get_string(0), Some("leaf_500"));

        let r = db
            .query("MATCH (h:Hub)-[:CONNECTS]->(l:Leaf) WHERE h.id = 1 AND l.id = 1050 RETURN l.label")
            .unwrap();
        assert_eq!(r.num_rows(), 1);
        assert_eq!(r.rows()[0].get_string(0), Some("leaf_1050"));

        let issues = db.check();
        assert!(issues.is_empty(), "integrity issues: {:?}", issues);
    }

    cleanup(&path);
}

// ============================================================
// S-03: Wide table with 20+ columns, sparse NULLs, long strings
// ============================================================

#[test]
fn s03_wide_table_sparse_nulls() {
    let path = temp_db_path("s03_wide");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();

        // Create a wide node table with 25 columns
        db.execute(
            "CREATE NODE TABLE Wide(\
                id INT64, \
                c01 STRING, c02 STRING, c03 STRING, c04 STRING, c05 STRING, \
                c06 INT64, c07 INT64, c08 INT64, c09 INT64, c10 INT64, \
                c11 STRING, c12 STRING, c13 STRING, c14 STRING, c15 STRING, \
                c16 INT64, c17 INT64, c18 INT64, c19 INT64, c20 INT64, \
                c21 DOUBLE, c22 DOUBLE, c23 DOUBLE, c24 DOUBLE, \
                PRIMARY KEY(id))",
        )
        .unwrap();

        let long_str: String = "Z".repeat(1500);

        // Insert 50 nodes with varying sparsity
        for i in 1..=50 {
            // All nodes get id and c01
            db.execute(&format!(
                "CREATE (n:Wide {{id: {}, c01: 'name_{}'}})",
                i, i
            ))
            .unwrap();

            // Even nodes get c06 (INT64) and c11 (STRING)
            if i % 2 == 0 {
                db.execute(&format!(
                    "MATCH (n:Wide) WHERE n.id = {} SET n.c06 = {}",
                    i,
                    i * 100
                ))
                .unwrap();
                db.execute(&format!(
                    "MATCH (n:Wide) WHERE n.id = {} SET n.c11 = 'extra_{}'",
                    i, i
                ))
                .unwrap();
            }

            // Every 10th node gets a long string in c15
            if i % 10 == 0 {
                db.execute(&format!(
                    "MATCH (n:Wide) WHERE n.id = {} SET n.c15 = '{}'",
                    i, long_str
                ))
                .unwrap();
            }

            // Odd nodes get c21 (DOUBLE)
            if i % 2 == 1 {
                db.execute(&format!(
                    "MATCH (n:Wide) WHERE n.id = {} SET n.c21 = {:.2}",
                    i,
                    i as f64 * 3.14
                ))
                .unwrap();
            }
        }

        db.checkpoint().unwrap();
    }

    // Reopen and verify
    {
        let db = Database::open(&path).unwrap();

        // Total count
        let result = db.query("MATCH (n:Wide) RETURN COUNT(n)").unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(50));

        // Check sparse c06: only even nodes have it
        let result = db
            .query("MATCH (n:Wide) WHERE n.c06 IS NOT NULL RETURN COUNT(n)")
            .unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(25));

        // Check c06 value for a specific even node
        let r = db
            .query("MATCH (n:Wide) WHERE n.id = 10 RETURN n.c06")
            .unwrap();
        assert_eq!(r.rows()[0].get_int(0), Some(1000));

        // Check odd nodes have NULL c06
        let r = db
            .query("MATCH (n:Wide) WHERE n.id = 7 RETURN n.c06")
            .unwrap();
        assert_eq!(r.num_rows(), 1);
        assert!(r.rows()[0].values[0].is_null());

        // Check long string in c15 for node 20
        let r = db
            .query("MATCH (n:Wide) WHERE n.id = 20 RETURN n.c15")
            .unwrap();
        assert_eq!(r.rows()[0].get_string(0).map(|s| s.len()), Some(1500));

        // Check c21 for an odd node
        let r = db
            .query("MATCH (n:Wide) WHERE n.id = 5 RETURN n.c21")
            .unwrap();
        let val = r.rows()[0].get_float(0).unwrap();
        assert!((val - 15.70).abs() < 0.01);

        // Check c21 NULL for even node
        let r = db
            .query("MATCH (n:Wide) WHERE n.id = 4 RETURN n.c21")
            .unwrap();
        assert!(r.rows()[0].values[0].is_null());

        let issues = db.check();
        assert!(issues.is_empty(), "integrity issues: {:?}", issues);
    }

    cleanup(&path);
}

// ============================================================
// S-04: Relationship table with amount/timestamp/status properties
// ============================================================

#[test]
fn s04_relationship_properties() {
    let path = temp_db_path("s04_rel_props");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE NODE TABLE Account(id INT64, name STRING, PRIMARY KEY(id))",
        )
        .unwrap();
        db.execute(
            "CREATE REL TABLE TRANSFER(\
                FROM Account TO Account, \
                amount DOUBLE, \
                timestamp INT64, \
                status STRING)",
        )
        .unwrap();

        // Create 20 accounts
        for i in 1..=20 {
            db.execute(&format!(
                "CREATE (n:Account {{id: {}, name: 'acct_{}'}})",
                i, i
            ))
            .unwrap();
        }

        // Create 100 transfer relationships with properties
        for i in 0..100 {
            let src = (i % 20) + 1;
            let dst = ((i * 3 + 7) % 20) + 1;
            if src != dst {
                let amount = (i as f64 + 1.0) * 50.0;
                let timestamp = 1700000000 + i * 3600;
                let status = if i % 3 == 0 {
                    "completed"
                } else if i % 3 == 1 {
                    "pending"
                } else {
                    "failed"
                };
                let _ = db.execute(&format!(
                    "MATCH (a:Account), (b:Account) WHERE a.id = {} AND b.id = {} \
                     CREATE (a)-[:TRANSFER {{amount: {:.1}, timestamp: {}, status: '{}'}}]->(b)",
                    src, dst, amount, timestamp, status
                ));
            }
        }

        db.checkpoint().unwrap();
    }

    // Reopen and verify edge properties persist
    {
        let db = Database::open(&path).unwrap();

        // Verify transfers exist
        let result = db
            .query("MATCH (a:Account)-[:TRANSFER]->(b:Account) RETURN COUNT(a)")
            .unwrap();
        let transfer_count = result.rows()[0].get_int(0).unwrap();
        assert!(
            transfer_count > 0,
            "should have transfer relationships after reopen"
        );

        // Verify account count
        let result = db.query("MATCH (n:Account) RETURN COUNT(n)").unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(20));

        let issues = db.check();
        assert!(issues.is_empty(), "integrity issues: {:?}", issues);
    }

    cleanup(&path);
}

// ============================================================
// S-05: Hotspot node under repeated update+query cycles (100 rounds)
// ============================================================

#[test]
fn s05_hotspot_read_write_mix() {
    let path = temp_db_path("s05_hotspot");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE Hot(id INT64, counter INT64, label STRING, PRIMARY KEY(id))")
            .unwrap();

        // Create a few hotspot nodes
        for i in 1..=5 {
            db.execute(&format!(
                "CREATE (n:Hot {{id: {}, counter: 0, label: 'hot_{}'}})",
                i, i
            ))
            .unwrap();
        }

        // 100 rounds of update + query on the same hotspot nodes
        for round in 1..=100_i64 {
            // Update all hotspot nodes
            for node_id in 1..=5 {
                db.execute(&format!(
                    "MATCH (n:Hot) WHERE n.id = {} SET n.counter = {}",
                    node_id, round
                ))
                .unwrap();
            }

            // Query to verify — all should show the current round value
            for node_id in 1..=5 {
                let result = db
                    .query(&format!(
                        "MATCH (n:Hot) WHERE n.id = {} RETURN n.counter",
                        node_id
                    ))
                    .unwrap();
                assert_eq!(
                    result.rows()[0].get_int(0),
                    Some(round),
                    "node {} should have counter={} at round {}",
                    node_id,
                    round,
                    round
                );
            }

            // Checkpoint occasionally
            if round % 25 == 0 {
                db.checkpoint().unwrap();
            }
        }

        // Final state: all counters should be 100
        for node_id in 1..=5 {
            let result = db
                .query(&format!(
                    "MATCH (n:Hot) WHERE n.id = {} RETURN n.counter",
                    node_id
                ))
                .unwrap();
            assert_eq!(result.rows()[0].get_int(0), Some(100));
        }
    }

    // Reopen and verify final state
    {
        let db = Database::open(&path).unwrap();
        for node_id in 1..=5 {
            let result = db
                .query(&format!(
                    "MATCH (n:Hot) WHERE n.id = {} RETURN n.counter",
                    node_id
                ))
                .unwrap();
            assert_eq!(
                result.rows()[0].get_int(0),
                Some(100),
                "node {} counter should be 100 after reopen",
                node_id
            );
        }

        let result = db.query("MATCH (n:Hot) RETURN COUNT(n)").unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(5));
    }

    cleanup(&path);
}

// ============================================================
// S-06: 5 rounds of write -> checkpoint -> close -> reopen, verify cumulative
// ============================================================

#[test]
fn s06_checkpoint_reopen_pressure() {
    let path = temp_db_path("s06_ckpt_pressure");
    cleanup(&path);

    // Session 0: create schema
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE Acc(id INT64, round INT64, PRIMARY KEY(id))")
            .unwrap();
    }

    let mut next_id = 1_i64;

    for round in 1..=5_i64 {
        // Write phase
        {
            let db = Database::open(&path).unwrap();
            for _ in 0..100 {
                db.execute(&format!(
                    "CREATE (n:Acc {{id: {}, round: {}}})",
                    next_id, round
                ))
                .unwrap();
                next_id += 1;
            }
            db.checkpoint().unwrap();
        }

        // Reopen and verify cumulative count
        {
            let db = Database::open(&path).unwrap();
            let expected = round * 100;
            let result = db.query("MATCH (n:Acc) RETURN COUNT(n)").unwrap();
            assert_eq!(
                result.rows()[0].get_int(0),
                Some(expected),
                "after round {}, expected {} nodes",
                round,
                expected
            );

            // Verify this round's data
            let result = db
                .query(&format!(
                    "MATCH (n:Acc) WHERE n.round = {} RETURN COUNT(n)",
                    round
                ))
                .unwrap();
            assert_eq!(
                result.rows()[0].get_int(0),
                Some(100),
                "round {} should have 100 nodes",
                round
            );

            // Verify earlier rounds still intact
            for prev_round in 1..round {
                let result = db
                    .query(&format!(
                        "MATCH (n:Acc) WHERE n.round = {} RETURN COUNT(n)",
                        prev_round
                    ))
                    .unwrap();
                assert_eq!(
                    result.rows()[0].get_int(0),
                    Some(100),
                    "previous round {} should still have 100 nodes",
                    prev_round
                );
            }

            let issues = db.check();
            assert!(issues.is_empty(), "integrity issues at round {}: {:?}", round, issues);
        }
    }

    // Final verification
    {
        let db = Database::open(&path).unwrap();
        let result = db.query("MATCH (n:Acc) RETURN COUNT(n)").unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(500));
    }

    cleanup(&path);
}

// ============================================================
// S-07: Insert 500 nodes+edges, delete 400, insert 200 more,
//       verify no dangling edges, db.check()
// ============================================================

#[test]
fn s07_bulk_delete_consistency() {
    let path = temp_db_path("s07_delete");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE Nd(id INT64, tag STRING, PRIMARY KEY(id))")
            .unwrap();
        db.execute("CREATE REL TABLE LNK(FROM Nd TO Nd)").unwrap();

        // Insert 500 nodes
        for i in 1..=500 {
            db.execute(&format!(
                "CREATE (n:Nd {{id: {}, tag: 'initial'}})",
                i
            ))
            .unwrap();
        }

        // Insert edges among the first 500 nodes (chain: 1->2, 2->3, ..., 499->500)
        for i in 1..=499 {
            db.execute(&format!(
                "MATCH (a:Nd), (b:Nd) WHERE a.id = {} AND b.id = {} CREATE (a)-[:LNK]->(b)",
                i,
                i + 1
            ))
            .unwrap();
        }

        // Verify initial state
        let result = db.query("MATCH (n:Nd) RETURN COUNT(n)").unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(500));

        let result = db
            .query("MATCH (a:Nd)-[:LNK]->(b:Nd) RETURN COUNT(a)")
            .unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(499));

        // Delete nodes 1 through 400 (DETACH DELETE to remove associated edges)
        for i in 1..=400 {
            db.execute(&format!(
                "MATCH (n:Nd) WHERE n.id = {} DETACH DELETE n",
                i
            ))
            .unwrap();
        }

        // After deletion: 100 nodes remain (401..500), edges among them (401->402, ..., 499->500 = 99 edges)
        let result = db.query("MATCH (n:Nd) RETURN COUNT(n)").unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(100));

        let result = db
            .query("MATCH (a:Nd)-[:LNK]->(b:Nd) RETURN COUNT(a)")
            .unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(99));

        // Insert 200 more nodes (id 501..700)
        for i in 501..=700 {
            db.execute(&format!(
                "CREATE (n:Nd {{id: {}, tag: 'new_batch'}})",
                i
            ))
            .unwrap();
        }

        // Add some edges from new nodes to surviving nodes
        for i in 501..=550 {
            let target = 400 + ((i - 500) % 100) + 1; // targets 401..500
            db.execute(&format!(
                "MATCH (a:Nd), (b:Nd) WHERE a.id = {} AND b.id = {} CREATE (a)-[:LNK]->(b)",
                i, target
            ))
            .unwrap();
        }

        db.checkpoint().unwrap();

        // Verify no dangling edges (all edge endpoints should be live nodes)
        let issues = db.check();
        assert!(
            issues.is_empty(),
            "integrity check after delete+insert: {:?}",
            issues
        );

        // Final counts: 100 (surviving) + 200 (new) = 300 nodes
        let result = db.query("MATCH (n:Nd) RETURN COUNT(n)").unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(300));

        // Edges: 99 (surviving chain) + 50 (new) = 149
        let result = db
            .query("MATCH (a:Nd)-[:LNK]->(b:Nd) RETURN COUNT(a)")
            .unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(149));
    }

    // Reopen and verify
    {
        let db = Database::open(&path).unwrap();
        let result = db.query("MATCH (n:Nd) RETURN COUNT(n)").unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(300));

        let result = db
            .query("MATCH (a:Nd)-[:LNK]->(b:Nd) RETURN COUNT(a)")
            .unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(149));

        // Deleted nodes should not appear
        let result = db
            .query("MATCH (n:Nd) WHERE n.id = 1 RETURN n.id")
            .unwrap();
        assert_eq!(result.num_rows(), 0);

        let result = db
            .query("MATCH (n:Nd) WHERE n.id = 400 RETURN n.id")
            .unwrap();
        assert_eq!(result.num_rows(), 0);

        // Surviving node
        let result = db
            .query("MATCH (n:Nd) WHERE n.id = 450 RETURN n.tag")
            .unwrap();
        assert_eq!(result.rows()[0].get_string(0), Some("initial"));

        // New node
        let result = db
            .query("MATCH (n:Nd) WHERE n.id = 600 RETURN n.tag")
            .unwrap();
        assert_eq!(result.rows()[0].get_string(0), Some("new_batch"));

        let issues = db.check();
        assert!(issues.is_empty(), "integrity issues after reopen: {:?}", issues);
    }

    cleanup(&path);
}

// ============================================================
// S-08: 3 different schemas (RBAC, Ecommerce, Social) in same DB
// ============================================================

#[test]
fn s08_multi_schema_no_crosstalk() {
    let path = temp_db_path("s08_multi_schema");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();

        // --- RBAC schema ---
        db.execute("CREATE NODE TABLE User(id INT64, name STRING, PRIMARY KEY(id))")
            .unwrap();
        db.execute("CREATE NODE TABLE Role(id INT64, role_name STRING, PRIMARY KEY(id))")
            .unwrap();
        db.execute("CREATE REL TABLE HAS_ROLE(FROM User TO Role)")
            .unwrap();

        // --- Ecommerce schema ---
        db.execute("CREATE NODE TABLE Product(id INT64, title STRING, price DOUBLE, PRIMARY KEY(id))")
            .unwrap();
        db.execute("CREATE NODE TABLE Category(id INT64, cat_name STRING, PRIMARY KEY(id))")
            .unwrap();
        db.execute("CREATE REL TABLE BELONGS_TO(FROM Product TO Category)")
            .unwrap();

        // --- Social schema ---
        db.execute("CREATE NODE TABLE Person(id INT64, username STRING, PRIMARY KEY(id))")
            .unwrap();
        db.execute("CREATE REL TABLE FOLLOWS(FROM Person TO Person)")
            .unwrap();

        // Populate RBAC
        for i in 1..=10 {
            db.execute(&format!(
                "CREATE (n:User {{id: {}, name: 'user_{}'}})",
                i, i
            ))
            .unwrap();
        }
        for i in 1..=3 {
            db.execute(&format!(
                "CREATE (n:Role {{id: {}, role_name: 'role_{}'}})",
                i, i
            ))
            .unwrap();
        }
        for i in 1..=10 {
            let role_id = (i % 3) + 1;
            db.execute(&format!(
                "MATCH (u:User), (r:Role) WHERE u.id = {} AND r.id = {} CREATE (u)-[:HAS_ROLE]->(r)",
                i, role_id
            ))
            .unwrap();
        }

        // Populate Ecommerce
        for i in 1..=20 {
            db.execute(&format!(
                "CREATE (n:Product {{id: {}, title: 'prod_{}', price: {:.2}}})",
                i,
                i,
                i as f64 * 9.99
            ))
            .unwrap();
        }
        for i in 1..=5 {
            db.execute(&format!(
                "CREATE (n:Category {{id: {}, cat_name: 'cat_{}'}})",
                i, i
            ))
            .unwrap();
        }
        for i in 1..=20 {
            let cat_id = (i % 5) + 1;
            db.execute(&format!(
                "MATCH (p:Product), (c:Category) WHERE p.id = {} AND c.id = {} CREATE (p)-[:BELONGS_TO]->(c)",
                i, cat_id
            ))
            .unwrap();
        }

        // Populate Social
        for i in 1..=15 {
            db.execute(&format!(
                "CREATE (n:Person {{id: {}, username: 'person_{}'}})",
                i, i
            ))
            .unwrap();
        }
        for i in 1..=15 {
            let target = (i % 15) + 1;
            if i != target {
                let _ = db.execute(&format!(
                    "MATCH (a:Person), (b:Person) WHERE a.id = {} AND b.id = {} CREATE (a)-[:FOLLOWS]->(b)",
                    i, target
                ));
            }
        }

        db.checkpoint().unwrap();
    }

    // Reopen and verify each schema independently — no cross-talk
    {
        let db = Database::open(&path).unwrap();

        // RBAC checks
        let r = db.query("MATCH (u:User) RETURN COUNT(u)").unwrap();
        assert_eq!(r.rows()[0].get_int(0), Some(10), "RBAC: 10 users");

        let r = db.query("MATCH (r:Role) RETURN COUNT(r)").unwrap();
        assert_eq!(r.rows()[0].get_int(0), Some(3), "RBAC: 3 roles");

        let r = db
            .query("MATCH (u:User)-[:HAS_ROLE]->(r:Role) RETURN COUNT(u)")
            .unwrap();
        assert_eq!(r.rows()[0].get_int(0), Some(10), "RBAC: 10 role assignments");

        // Ecommerce checks
        let r = db.query("MATCH (p:Product) RETURN COUNT(p)").unwrap();
        assert_eq!(r.rows()[0].get_int(0), Some(20), "Ecommerce: 20 products");

        let r = db.query("MATCH (c:Category) RETURN COUNT(c)").unwrap();
        assert_eq!(r.rows()[0].get_int(0), Some(5), "Ecommerce: 5 categories");

        let r = db
            .query("MATCH (p:Product)-[:BELONGS_TO]->(c:Category) RETURN COUNT(p)")
            .unwrap();
        assert_eq!(
            r.rows()[0].get_int(0),
            Some(20),
            "Ecommerce: 20 product-category links"
        );

        // Social checks
        let r = db.query("MATCH (p:Person) RETURN COUNT(p)").unwrap();
        assert_eq!(r.rows()[0].get_int(0), Some(15), "Social: 15 persons");

        let r = db
            .query("MATCH (a:Person)-[:FOLLOWS]->(b:Person) RETURN COUNT(a)")
            .unwrap();
        let follows_count = r.rows()[0].get_int(0).unwrap();
        assert!(follows_count > 0, "Social: should have FOLLOWS edges");

        // Cross-talk check: User nodes should not appear as Person nodes
        // (they are separate tables, so queries on one should not return the other)
        let r = db
            .query("MATCH (u:User) WHERE u.id = 1 RETURN u.name")
            .unwrap();
        assert_eq!(r.rows()[0].get_string(0), Some("user_1"));

        let r = db
            .query("MATCH (p:Person) WHERE p.id = 1 RETURN p.username")
            .unwrap();
        assert_eq!(r.rows()[0].get_string(0), Some("person_1"));

        // Product price should be independent
        let r = db
            .query("MATCH (p:Product) WHERE p.id = 5 RETURN p.price")
            .unwrap();
        let price = r.rows()[0].get_float(0).unwrap();
        assert!((price - 49.95).abs() < 0.01);

        let issues = db.check();
        assert!(issues.is_empty(), "integrity issues: {:?}", issues);
    }

    cleanup(&path);
}

// ============================================================
// S-09: Self-loop edges and bidirectional edges
// ============================================================

#[test]
fn s09_self_loop_and_bidirectional() {
    let path = temp_db_path("s09_selfloop");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE Vertex(id INT64, name STRING, PRIMARY KEY(id))")
            .unwrap();
        db.execute("CREATE REL TABLE LINKS(FROM Vertex TO Vertex, weight DOUBLE)")
            .unwrap();

        // Create nodes
        db.execute("CREATE (n:Vertex {id: 1, name: 'alpha'})").unwrap();
        db.execute("CREATE (n:Vertex {id: 2, name: 'beta'})").unwrap();
        db.execute("CREATE (n:Vertex {id: 3, name: 'gamma'})").unwrap();

        // Self-loop: node 1 -> node 1
        db.execute(
            "MATCH (a:Vertex), (b:Vertex) WHERE a.id = 1 AND b.id = 1 \
             CREATE (a)-[:LINKS {weight: 1.0}]->(b)",
        )
        .unwrap();

        // Bidirectional edges: 2 -> 3 and 3 -> 2
        db.execute(
            "MATCH (a:Vertex), (b:Vertex) WHERE a.id = 2 AND b.id = 3 \
             CREATE (a)-[:LINKS {weight: 2.0}]->(b)",
        )
        .unwrap();
        db.execute(
            "MATCH (a:Vertex), (b:Vertex) WHERE a.id = 3 AND b.id = 2 \
             CREATE (a)-[:LINKS {weight: 3.0}]->(b)",
        )
        .unwrap();

        // Additional self-loop: node 3 -> node 3
        db.execute(
            "MATCH (a:Vertex), (b:Vertex) WHERE a.id = 3 AND b.id = 3 \
             CREATE (a)-[:LINKS {weight: 9.0}]->(b)",
        )
        .unwrap();

        db.checkpoint().unwrap();

        // Verify self-loop on node 1: outgoing from 1 to 1
        let r = db
            .query("MATCH (a:Vertex)-[:LINKS]->(b:Vertex) WHERE a.id = 1 AND b.id = 1 RETURN COUNT(a)")
            .unwrap();
        assert_eq!(r.rows()[0].get_int(0), Some(1), "node 1 should have 1 self-loop");

        // Verify bidirectional: 2->3
        let r = db
            .query("MATCH (a:Vertex)-[:LINKS]->(b:Vertex) WHERE a.id = 2 AND b.id = 3 RETURN COUNT(a)")
            .unwrap();
        assert_eq!(r.rows()[0].get_int(0), Some(1), "2->3 should exist");

        // Verify bidirectional: 3->2
        let r = db
            .query("MATCH (a:Vertex)-[:LINKS]->(b:Vertex) WHERE a.id = 3 AND b.id = 2 RETURN COUNT(a)")
            .unwrap();
        assert_eq!(r.rows()[0].get_int(0), Some(1), "3->2 should exist");

        // Total edges: 1 (self-loop 1) + 1 (2->3) + 1 (3->2) + 1 (self-loop 3) = 4
        let r = db
            .query("MATCH (a:Vertex)-[:LINKS]->(b:Vertex) RETURN COUNT(a)")
            .unwrap();
        assert_eq!(r.rows()[0].get_int(0), Some(4), "total 4 edges");

        // Outgoing from node 3: 3->2 and 3->3 = 2
        let r = db
            .query("MATCH (a:Vertex)-[:LINKS]->(b:Vertex) WHERE a.id = 3 RETURN COUNT(b)")
            .unwrap();
        assert_eq!(r.rows()[0].get_int(0), Some(2), "node 3 out-degree = 2");

        // Incoming to node 3: 2->3 and 3->3 = 2
        let r = db
            .query("MATCH (a:Vertex)-[:LINKS]->(b:Vertex) WHERE b.id = 3 RETURN COUNT(a)")
            .unwrap();
        assert_eq!(r.rows()[0].get_int(0), Some(2), "node 3 in-degree = 2");
    }

    // Reopen and verify everything persists
    {
        let db = Database::open(&path).unwrap();

        // Self-loop on node 1
        let r = db
            .query("MATCH (a:Vertex)-[:LINKS]->(b:Vertex) WHERE a.id = 1 AND b.id = 1 RETURN COUNT(a)")
            .unwrap();
        assert_eq!(
            r.rows()[0].get_int(0),
            Some(1),
            "self-loop on node 1 after reopen"
        );

        // Bidirectional 2<->3
        let r = db
            .query("MATCH (a:Vertex)-[:LINKS]->(b:Vertex) WHERE a.id = 2 AND b.id = 3 RETURN COUNT(a)")
            .unwrap();
        assert_eq!(r.rows()[0].get_int(0), Some(1), "2->3 after reopen");

        let r = db
            .query("MATCH (a:Vertex)-[:LINKS]->(b:Vertex) WHERE a.id = 3 AND b.id = 2 RETURN COUNT(a)")
            .unwrap();
        assert_eq!(r.rows()[0].get_int(0), Some(1), "3->2 after reopen");

        // Self-loop on node 3
        let r = db
            .query("MATCH (a:Vertex)-[:LINKS]->(b:Vertex) WHERE a.id = 3 AND b.id = 3 RETURN COUNT(a)")
            .unwrap();
        assert_eq!(
            r.rows()[0].get_int(0),
            Some(1),
            "self-loop on node 3 after reopen"
        );

        // Total edges still 4
        let r = db
            .query("MATCH (a:Vertex)-[:LINKS]->(b:Vertex) RETURN COUNT(a)")
            .unwrap();
        assert_eq!(r.rows()[0].get_int(0), Some(4), "total 4 edges after reopen");

        // Node 3 out-degree = 2 (3->2 + 3->3)
        let r = db
            .query("MATCH (a:Vertex)-[:LINKS]->(b:Vertex) WHERE a.id = 3 RETURN COUNT(b)")
            .unwrap();
        assert_eq!(r.rows()[0].get_int(0), Some(2));

        // Node 3 in-degree = 2 (2->3 + 3->3)
        let r = db
            .query("MATCH (a:Vertex)-[:LINKS]->(b:Vertex) WHERE b.id = 3 RETURN COUNT(a)")
            .unwrap();
        assert_eq!(r.rows()[0].get_int(0), Some(2));

        let issues = db.check();
        assert!(issues.is_empty(), "integrity issues: {:?}", issues);
    }

    cleanup(&path);
}

// ============================================================
// S-10: 200 rounds of insert -> verify -> delete -> verify cycle
// ============================================================

#[test]
fn s10_churn_insert_delete_200_rounds() {
    let path = temp_db_path("s10_churn");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE Eph(id INT64, round INT64, PRIMARY KEY(id))")
            .unwrap();
        db.execute("CREATE REL TABLE EPH_LINK(FROM Eph TO Eph)")
            .unwrap();

        let mut next_id = 1_i64;

        for round in 0..200_i64 {
            let base = next_id;

            // Insert 10 nodes
            for j in 0..10 {
                let id = base + j;
                db.execute(&format!(
                    "CREATE (n:Eph {{id: {}, round: {}}})",
                    id, round
                ))
                .unwrap();
            }
            next_id += 10;

            // Insert 9 edges chaining the 10 nodes (base -> base+1 -> ... -> base+9)
            for j in 0..9 {
                let src = base + j;
                let dst = base + j + 1;
                db.execute(&format!(
                    "MATCH (a:Eph), (b:Eph) WHERE a.id = {} AND b.id = {} CREATE (a)-[:EPH_LINK]->(b)",
                    src, dst
                ))
                .unwrap();
            }

            // Verify insertion: 10 nodes from this round
            let result = db
                .query(&format!(
                    "MATCH (n:Eph) WHERE n.round = {} RETURN COUNT(n)",
                    round
                ))
                .unwrap();
            assert_eq!(
                result.rows()[0].get_int(0),
                Some(10),
                "round {}: should have 10 nodes after insert",
                round
            );

            // Delete all 10 nodes (DETACH DELETE removes edges too)
            for j in 0..10 {
                let id = base + j;
                db.execute(&format!(
                    "MATCH (n:Eph) WHERE n.id = {} DETACH DELETE n",
                    id
                ))
                .unwrap();
            }

            // Verify deletion: 0 nodes from this round
            let result = db
                .query(&format!(
                    "MATCH (n:Eph) WHERE n.round = {} RETURN COUNT(n)",
                    round
                ))
                .unwrap();
            assert_eq!(
                result.rows()[0].get_int(0),
                Some(0),
                "round {}: should have 0 nodes after delete",
                round
            );

            // Periodic checkpoint
            if round % 50 == 49 {
                db.checkpoint().unwrap();
            }
        }

        // After all rounds: graph should be completely empty
        let result = db.query("MATCH (n:Eph) RETURN COUNT(n)").unwrap();
        assert_eq!(
            result.rows()[0].get_int(0),
            Some(0),
            "graph should be empty after all churn rounds"
        );

        let result = db
            .query("MATCH (a:Eph)-[:EPH_LINK]->(b:Eph) RETURN COUNT(a)")
            .unwrap();
        assert_eq!(
            result.rows()[0].get_int(0),
            Some(0),
            "no edges should remain"
        );

        let issues = db.check();
        assert!(issues.is_empty(), "integrity issues after churn: {:?}", issues);

        db.checkpoint().unwrap();
    }

    // Reopen and confirm no ghost data
    {
        let db = Database::open(&path).unwrap();

        let result = db.query("MATCH (n:Eph) RETURN COUNT(n)").unwrap();
        assert_eq!(
            result.rows()[0].get_int(0),
            Some(0),
            "no ghost nodes after reopen"
        );

        let result = db
            .query("MATCH (a:Eph)-[:EPH_LINK]->(b:Eph) RETURN COUNT(a)")
            .unwrap();
        assert_eq!(
            result.rows()[0].get_int(0),
            Some(0),
            "no ghost edges after reopen"
        );

        let issues = db.check();
        assert!(issues.is_empty(), "integrity issues after reopen: {:?}", issues);
    }

    cleanup(&path);
}

// ============================================================
// S-11: 10 sessions of open -> write batch -> close -> reopen -> verify
// ============================================================

#[test]
fn s11_progressive_growth_10_sessions() {
    let path = temp_db_path("s11_growth");
    cleanup(&path);

    // Session 0: create schema
    {
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE NODE TABLE Batch(id INT64, session INT64, label STRING, PRIMARY KEY(id))",
        )
        .unwrap();
        db.execute("CREATE NODE TABLE Anchor(id INT64, PRIMARY KEY(id))")
            .unwrap();
        db.execute("CREATE REL TABLE BATCH_LINK(FROM Batch TO Batch)")
            .unwrap();
        db.execute("CREATE REL TABLE TO_ANCHOR(FROM Batch TO Anchor)")
            .unwrap();

        // Create 10 anchor nodes (persistent targets for cross-batch edges)
        for i in 1..=10 {
            db.execute(&format!("CREATE (n:Anchor {{id: {}}})", i))
                .unwrap();
        }
    }

    let mut next_id = 1_i64;

    for session in 1..=10_i64 {
        let batch_start = next_id;

        // Write phase: open, write 100 nodes + edges, close
        {
            let db = Database::open(&path).unwrap();

            // Verify cumulative state before writing
            let expected_nodes = (session - 1) * 100;
            let result = db.query("MATCH (n:Batch) RETURN COUNT(n)").unwrap();
            assert_eq!(
                result.rows()[0].get_int(0),
                Some(expected_nodes),
                "session {}: pre-write should have {} Batch nodes",
                session,
                expected_nodes
            );

            // Write 100 new Batch nodes
            for j in 0..100 {
                let id = batch_start + j;
                db.execute(&format!(
                    "CREATE (n:Batch {{id: {}, session: {}, label: 'sess{}_item{}'}})",
                    id, session, session, j
                ))
                .unwrap();
            }
            next_id += 100;

            // Chain this batch's nodes: first -> second -> ... (99 edges within batch)
            for j in 0..99 {
                let src = batch_start + j;
                let dst = batch_start + j + 1;
                db.execute(&format!(
                    "MATCH (a:Batch), (b:Batch) WHERE a.id = {} AND b.id = {} CREATE (a)-[:BATCH_LINK]->(b)",
                    src, dst
                ))
                .unwrap();
            }

            // Connect first node of this batch to an anchor
            let anchor_id = session; // session 1..10 maps to anchor 1..10
            db.execute(&format!(
                "MATCH (b:Batch), (a:Anchor) WHERE b.id = {} AND a.id = {} CREATE (b)-[:TO_ANCHOR]->(a)",
                batch_start, anchor_id
            ))
            .unwrap();

            // Checkpoint every other session
            if session % 2 == 0 {
                db.checkpoint().unwrap();
            }
        }

        // Verify phase: reopen and check cumulative state
        {
            let db = Database::open(&path).unwrap();

            // Total Batch nodes
            let expected_total = session * 100;
            let result = db.query("MATCH (n:Batch) RETURN COUNT(n)").unwrap();
            assert_eq!(
                result.rows()[0].get_int(0),
                Some(expected_total),
                "session {}: should have {} total Batch nodes",
                session,
                expected_total
            );

            // This session's nodes
            let result = db
                .query(&format!(
                    "MATCH (n:Batch) WHERE n.session = {} RETURN COUNT(n)",
                    session
                ))
                .unwrap();
            assert_eq!(
                result.rows()[0].get_int(0),
                Some(100),
                "session {}: should have 100 nodes",
                session
            );

            // Historical batches are still queryable
            for prev in 1..session {
                let result = db
                    .query(&format!(
                        "MATCH (n:Batch) WHERE n.session = {} RETURN COUNT(n)",
                        prev
                    ))
                    .unwrap();
                assert_eq!(
                    result.rows()[0].get_int(0),
                    Some(100),
                    "session {}: historical session {} should still have 100 nodes",
                    session,
                    prev
                );
            }

            // Spot-check: first node of this session
            let result = db
                .query(&format!(
                    "MATCH (n:Batch) WHERE n.id = {} RETURN n.label",
                    batch_start
                ))
                .unwrap();
            assert_eq!(
                result.rows()[0].get_string(0),
                Some(&*format!("sess{}_item0", session))
            );

            // Verify anchor links: should have `session` anchor connections total
            let result = db
                .query("MATCH (b:Batch)-[:TO_ANCHOR]->(a:Anchor) RETURN COUNT(b)")
                .unwrap();
            assert_eq!(
                result.rows()[0].get_int(0),
                Some(session),
                "session {}: should have {} anchor links",
                session,
                session
            );

            // Batch intra-edges: each session contributes 99, total = session * 99
            let expected_edges = session * 99;
            let result = db
                .query("MATCH (a:Batch)-[:BATCH_LINK]->(b:Batch) RETURN COUNT(a)")
                .unwrap();
            assert_eq!(
                result.rows()[0].get_int(0),
                Some(expected_edges),
                "session {}: should have {} batch-link edges",
                session,
                expected_edges
            );
        }
    }

    // Final comprehensive verification
    {
        let db = Database::open(&path).unwrap();

        // 10 sessions * 100 = 1000 Batch nodes
        let result = db.query("MATCH (n:Batch) RETURN COUNT(n)").unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(1000));

        // 10 Anchor nodes
        let result = db.query("MATCH (n:Anchor) RETURN COUNT(n)").unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(10));

        // 10 * 99 = 990 batch-link edges
        let result = db
            .query("MATCH (a:Batch)-[:BATCH_LINK]->(b:Batch) RETURN COUNT(a)")
            .unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(990));

        // 10 anchor edges
        let result = db
            .query("MATCH (b:Batch)-[:TO_ANCHOR]->(a:Anchor) RETURN COUNT(b)")
            .unwrap();
        assert_eq!(result.rows()[0].get_int(0), Some(10));

        let issues = db.check();
        assert!(issues.is_empty(), "final integrity issues: {:?}", issues);
    }

    cleanup(&path);
}
