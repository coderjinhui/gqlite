//! Soak test: sustained random read/write workload.
//!
//! Run: `cargo test --test soak_test --release -- --ignored --nocapture`
//!
//! This test is `#[ignore]` by default so it doesn't run in normal CI.

use gqlite_core::Database;

/// Run a mixed read/write workload for `duration_secs` seconds.
fn run_soak(duration_secs: u64) {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Item(id INT64, val INT64, PRIMARY KEY(id))").unwrap();

    let start = std::time::Instant::now();
    let mut insert_count = 0u64;
    let mut query_count = 0u64;
    let mut update_count = 0u64;
    let mut delete_count = 0u64;
    let mut error_count = 0u64;
    let mut next_id = 1i64;

    while start.elapsed().as_secs() < duration_secs {
        let op = next_id % 10;

        match op {
            0..=5 => {
                // INSERT (60%)
                match db.execute(&format!(
                    "CREATE (n:Item {{id: {}, val: {}}})",
                    next_id,
                    next_id * 7
                )) {
                    Ok(_) => insert_count += 1,
                    Err(_) => error_count += 1,
                }
            }
            6..=7 => {
                // QUERY (20%)
                match db.query("MATCH (n:Item) RETURN COUNT(n)") {
                    Ok(r) => {
                        assert!(r.num_rows() > 0);
                        query_count += 1;
                    }
                    Err(_) => error_count += 1,
                }
            }
            8 => {
                // UPDATE (10%)
                let target = next_id / 2;
                match db.execute(&format!(
                    "MATCH (n:Item) WHERE n.id = {} SET n.val = {}",
                    target,
                    next_id * 3
                )) {
                    Ok(_) => update_count += 1,
                    Err(_) => error_count += 1,
                }
            }
            _ => {
                // DELETE (10%)
                let target = next_id / 3;
                match db.execute(&format!("MATCH (n:Item) WHERE n.id = {} DELETE n", target)) {
                    Ok(_) => delete_count += 1,
                    Err(_) => error_count += 1,
                }
            }
        }

        next_id += 1;
    }

    let elapsed = start.elapsed();
    let total_ops = insert_count + query_count + update_count + delete_count;

    println!("=== Soak Test Results ===");
    println!("Duration: {:?}", elapsed);
    println!("Total ops: {}", total_ops);
    println!(
        "  INSERT: {}, QUERY: {}, UPDATE: {}, DELETE: {}",
        insert_count, query_count, update_count, delete_count
    );
    println!("Errors: {}", error_count);
    println!("Throughput: {:.0} ops/sec", total_ops as f64 / elapsed.as_secs_f64());

    // Verify database is still usable
    let r = db.query("MATCH (n:Item) RETURN COUNT(n)").unwrap();
    println!("Final row count: {}", r.rows()[0].get_int(0).unwrap_or(0));

    // Integrity check
    let issues = db.check();
    assert!(issues.is_empty(), "integrity issues: {:?}", issues);
    println!("Integrity check: PASS");
}

/// Short soak test (10 seconds) — runs in normal test suite.
#[test]
fn soak_10s() {
    run_soak(10);
}

/// Long soak test (5 minutes) — ignored by default.
#[test]
#[ignore]
fn soak_5m() {
    run_soak(300);
}
