//! Cross-cutting scenario tests (G-01 through G-12).
//!
//! These tests exercise concerns that cut across all business domains:
//! persistence, checkpoint consistency, WAL recovery, edge properties,
//! edge-level operations, hotspot nodes, dense subgraphs, wide schemas,
//! idempotent MERGE, mixed read/write stress, multi-tenant isolation,
//! and transaction rollback recovery.
//!
//! Uses a representative ecommerce-like schema:
//!   - Customer(id, name, email)
//!   - Product(id, name, price)
//!   - Purchase(id, customer_id, product_id, amount) -- modeled as node for queryable properties
//!   - Review(id, customer_id, product_id, rating) -- modeled as node for queryable properties
//!   - PURCHASED(Customer -> Product)
//!   - MADE_PURCHASE(Customer -> Purchase)
//!   - PURCHASE_OF(Purchase -> Product)
//!   - REVIEWED(Customer -> Product)
//!   - WROTE_REVIEW(Customer -> Review)
//!   - REVIEW_OF(Review -> Product)
//!   - ROAD(City -> City, distance DOUBLE) -- for Dijkstra edge-property test

use gqlite_core::Database;
use std::path::PathBuf;

// ── File-based DB helpers ───────────────────────────────────

fn temp_db_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("gqlite_cross_cutting_test");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(format!("{}_{}.graph", name, std::process::id()))
}

fn cleanup(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(path.with_extension("graph.wal"));
    let _ = std::fs::remove_file(path.with_extension("graph.lock"));
    let _ = std::fs::remove_file(path.with_extension("graph.tmp"));
}

// ── Shared schema / data helpers ────────────────────────────

/// Create the ecommerce schema with purchase/review as intermediary nodes
/// so that amount/rating properties are queryable.
fn create_ecommerce_schema(db: &Database) {
    db.execute("CREATE NODE TABLE Customer(id INT64, name STRING, email STRING, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE NODE TABLE Product(id INT64, name STRING, price DOUBLE, PRIMARY KEY(id))")
        .unwrap();
    // Purchase and Review as intermediary nodes for queryable edge properties.
    db.execute(
        "CREATE NODE TABLE Purchase(id INT64, amount DOUBLE, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute(
        "CREATE NODE TABLE Review(id INT64, rating INT64, PRIMARY KEY(id))",
    )
    .unwrap();

    // Relationship tables
    db.execute("CREATE REL TABLE PURCHASED(FROM Customer TO Product)").unwrap();
    db.execute("CREATE REL TABLE MADE_PURCHASE(FROM Customer TO Purchase)").unwrap();
    db.execute("CREATE REL TABLE PURCHASE_OF(FROM Purchase TO Product)").unwrap();
    db.execute("CREATE REL TABLE WROTE_REVIEW(FROM Customer TO Review)").unwrap();
    db.execute("CREATE REL TABLE REVIEW_OF(FROM Review TO Product)").unwrap();
}

/// Insert a standard set of customers and products.
fn insert_ecommerce_data(db: &Database) {
    // 10 customers
    for i in 1..=10 {
        db.execute(&format!(
            "CREATE (c:Customer {{id: {}, name: 'Customer{}', email: 'c{}@shop.com'}})",
            i, i, i
        ))
        .unwrap();
    }

    // 10 products
    let products = [
        (1, "Laptop", 999.99),
        (2, "Phone", 699.99),
        (3, "Tablet", 499.99),
        (4, "Headphones", 149.99),
        (5, "Monitor", 349.99),
        (6, "Keyboard", 79.99),
        (7, "Mouse", 39.99),
        (8, "Webcam", 59.99),
        (9, "Charger", 29.99),
        (10, "Cable", 9.99),
    ];
    for (id, name, price) in &products {
        db.execute(&format!(
            "CREATE (p:Product {{id: {}, name: '{}', price: {}}})",
            id, name, price
        ))
        .unwrap();
    }

    // 20 purchase relationships with amount via intermediary Purchase node
    let purchases: [(i64, i64, f64); 20] = [
        (1, 1, 999.99),
        (1, 2, 699.99),
        (1, 5, 349.99),
        (2, 1, 999.99),
        (2, 3, 499.99),
        (3, 2, 699.99),
        (3, 4, 149.99),
        (3, 6, 79.99),
        (4, 5, 349.99),
        (4, 7, 39.99),
        (5, 1, 999.99),
        (5, 8, 59.99),
        (6, 3, 499.99),
        (6, 9, 29.99),
        (7, 10, 9.99),
        (7, 1, 999.99),
        (8, 2, 699.99),
        (8, 4, 149.99),
        (9, 6, 79.99),
        (10, 7, 39.99),
    ];
    for (idx, (cid, pid, amount)) in purchases.iter().enumerate() {
        let purchase_id = (idx + 1) as i64;
        // Create Purchase node with amount property
        db.execute(&format!(
            "CREATE (pur:Purchase {{id: {}, amount: {:.2}}})",
            purchase_id, amount
        ))
        .unwrap();
        // Direct edge Customer -> Product
        db.execute(&format!(
            "MATCH (c:Customer), (p:Product) WHERE c.id = {} AND p.id = {} \
             CREATE (c)-[:PURCHASED]->(p)",
            cid, pid
        ))
        .unwrap();
        // Customer -> Purchase -> Product chain
        db.execute(&format!(
            "MATCH (c:Customer), (pur:Purchase) WHERE c.id = {} AND pur.id = {} \
             CREATE (c)-[:MADE_PURCHASE]->(pur)",
            cid, purchase_id
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (pur:Purchase), (p:Product) WHERE pur.id = {} AND p.id = {} \
             CREATE (pur)-[:PURCHASE_OF]->(p)",
            purchase_id, pid
        ))
        .unwrap();
    }

    // 15 review relationships with rating via intermediary Review node
    let reviews: [(i64, i64, i64); 15] = [
        (1, 1, 5),
        (1, 2, 4),
        (2, 1, 5),
        (2, 3, 3),
        (3, 2, 4),
        (3, 4, 5),
        (4, 5, 3),
        (5, 1, 4),
        (5, 8, 2),
        (6, 3, 4),
        (7, 10, 1),
        (7, 1, 5),
        (8, 2, 3),
        (9, 6, 4),
        (10, 7, 5),
    ];
    for (idx, (cid, pid, rating)) in reviews.iter().enumerate() {
        let review_id = (idx + 1) as i64;
        // Create Review node with rating property
        db.execute(&format!(
            "CREATE (rev:Review {{id: {}, rating: {}}})",
            review_id, rating
        ))
        .unwrap();
        // Direct edge Customer -> Product
        db.execute(&format!(
            "MATCH (c:Customer), (p:Product) WHERE c.id = {} AND p.id = {} \
             CREATE (c)-[:PURCHASED]->(p)",
            cid, pid
        ))
        .unwrap_or_else(|_| gqlite_core::QueryResult::empty()); // might already exist, ignore duplicate error
        // Customer -> Review -> Product chain
        db.execute(&format!(
            "MATCH (c:Customer), (rev:Review) WHERE c.id = {} AND rev.id = {} \
             CREATE (c)-[:WROTE_REVIEW]->(rev)",
            cid, review_id
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (rev:Review), (p:Product) WHERE rev.id = {} AND p.id = {} \
             CREATE (rev)-[:REVIEW_OF]->(p)",
            review_id, pid
        ))
        .unwrap();
    }
}

/// Full in-memory setup.
fn setup_ecommerce_db() -> Database {
    let db = Database::in_memory();
    create_ecommerce_schema(&db);
    insert_ecommerce_data(&db);
    db
}

// ============================================================
// G-01: File restart consistency
// ============================================================

#[test]
fn g01_file_restart_consistency() {
    let path = temp_db_path("g01_restart");
    cleanup(&path);

    // Capture counts before close.
    let (customer_count_before, product_count_before, purchased_count_before, multi_hop_before);
    {
        let db = Database::open(&path).unwrap();
        create_ecommerce_schema(&db);
        insert_ecommerce_data(&db);

        let r = db.query("MATCH (c:Customer) RETURN COUNT(c)").unwrap();
        customer_count_before = r.rows()[0].get_int(0).unwrap();
        let r = db.query("MATCH (p:Product) RETURN COUNT(p)").unwrap();
        product_count_before = r.rows()[0].get_int(0).unwrap();

        let r = db
            .query("MATCH (c:Customer)-[:PURCHASED]->(p:Product) RETURN COUNT(c)")
            .unwrap();
        purchased_count_before = r.rows()[0].get_int(0).unwrap();

        // Multi-hop: customers who purchased products also purchased by Customer1
        let r = db
            .query(
                "MATCH (c1:Customer)-[:PURCHASED]->(p:Product)<-[:PURCHASED]-(c2:Customer) \
                 WHERE c1.id = 1 AND c2.id <> 1 \
                 RETURN DISTINCT c2.name",
            )
            .unwrap();
        multi_hop_before = r.num_rows() as i64;
    }

    // Reopen and verify
    {
        let db = Database::open(&path).unwrap();

        let r = db.query("MATCH (c:Customer) RETURN COUNT(c)").unwrap();
        assert_eq!(r.rows()[0].get_int(0).unwrap(), customer_count_before, "customer count mismatch");

        let r = db.query("MATCH (p:Product) RETURN COUNT(p)").unwrap();
        assert_eq!(r.rows()[0].get_int(0).unwrap(), product_count_before, "product count mismatch");

        let r = db
            .query("MATCH (c:Customer)-[:PURCHASED]->(p:Product) RETURN COUNT(c)")
            .unwrap();
        assert_eq!(r.rows()[0].get_int(0).unwrap(), purchased_count_before, "purchase count mismatch");

        let r = db
            .query(
                "MATCH (c1:Customer)-[:PURCHASED]->(p:Product)<-[:PURCHASED]-(c2:Customer) \
                 WHERE c1.id = 1 AND c2.id <> 1 \
                 RETURN DISTINCT c2.name",
            )
            .unwrap();
        assert_eq!(
            r.num_rows() as i64, multi_hop_before,
            "multi-hop query result mismatch after restart"
        );

        let issues = db.check();
        assert!(issues.is_empty(), "integrity check failed after restart: {:?}", issues);
    }

    cleanup(&path);
}

// ============================================================
// G-02: Checkpoint after complex query consistency
// ============================================================

#[test]
fn g02_checkpoint_complex_query_consistency() {
    let path = temp_db_path("g02_ckpt");
    cleanup(&path);

    let db = Database::open(&path).unwrap();
    create_ecommerce_schema(&db);
    insert_ecommerce_data(&db);

    // Pre-checkpoint queries
    let r_pre_agg = db
        .query(
            "MATCH (c:Customer)-[:PURCHASED]->(p:Product) \
             RETURN c.name, COUNT(p) ORDER BY c.name",
        )
        .unwrap();
    let pre_agg_rows: Vec<(&str, i64)> = r_pre_agg
        .rows()
        .iter()
        .map(|r| (r.get_string(0).unwrap(), r.get_int(1).unwrap()))
        .collect();

    // Variable-length path: customers reachable from Customer1 via shared purchases
    let r_pre_path = db
        .query(
            "MATCH (c1:Customer)-[:PURCHASED]->(p:Product)<-[:PURCHASED]-(c2:Customer) \
             WHERE c1.id = 1 AND c2.id <> 1 \
             RETURN DISTINCT c2.id ORDER BY c2.id",
        )
        .unwrap();
    let pre_path_ids: Vec<i64> =
        r_pre_path.rows().iter().map(|r| r.get_int(0).unwrap()).collect();

    // Checkpoint
    db.checkpoint().unwrap();

    // Post-checkpoint: exact same queries
    let r_post_agg = db
        .query(
            "MATCH (c:Customer)-[:PURCHASED]->(p:Product) \
             RETURN c.name, COUNT(p) ORDER BY c.name",
        )
        .unwrap();
    let post_agg_rows: Vec<(&str, i64)> = r_post_agg
        .rows()
        .iter()
        .map(|r| (r.get_string(0).unwrap(), r.get_int(1).unwrap()))
        .collect();

    assert_eq!(pre_agg_rows.len(), post_agg_rows.len(), "aggregation row count changed");
    for (pre, post) in pre_agg_rows.iter().zip(post_agg_rows.iter()) {
        assert_eq!(pre, post, "aggregation result changed after checkpoint");
    }

    let r_post_path = db
        .query(
            "MATCH (c1:Customer)-[:PURCHASED]->(p:Product)<-[:PURCHASED]-(c2:Customer) \
             WHERE c1.id = 1 AND c2.id <> 1 \
             RETURN DISTINCT c2.id ORDER BY c2.id",
        )
        .unwrap();
    let post_path_ids: Vec<i64> =
        r_post_path.rows().iter().map(|r| r.get_int(0).unwrap()).collect();
    assert_eq!(pre_path_ids, post_path_ids, "path query changed after checkpoint");

    let issues = db.check();
    assert!(issues.is_empty(), "integrity issues after checkpoint: {:?}", issues);

    cleanup(&path);
}

// ============================================================
// G-03: WAL recovery after mixed writes
// ============================================================

#[test]
fn g03_wal_recovery_business_state() {
    let path = temp_db_path("g03_wal");
    cleanup(&path);

    // Phase 1: pre-checkpoint data + checkpoint + post-checkpoint data (WAL only)
    {
        let db = Database::open(&path).unwrap();
        create_ecommerce_schema(&db);

        // Pre-checkpoint: 5 customers + 5 products
        for i in 1..=5 {
            db.execute(&format!(
                "CREATE (c:Customer {{id: {}, name: 'PreCust{}', email: 'pre{}@shop.com'}})",
                i, i, i
            ))
            .unwrap();
        }
        for i in 1..=5 {
            db.execute(&format!(
                "CREATE (p:Product {{id: {}, name: 'PreProd{}', price: {:.2}}})",
                i,
                i,
                i as f64 * 10.0
            ))
            .unwrap();
        }
        // Pre-checkpoint relationships
        for i in 1..=5 {
            db.execute(&format!(
                "MATCH (c:Customer), (p:Product) WHERE c.id = {} AND p.id = {} \
                 CREATE (c)-[:PURCHASED {{amount: {:.2}}}]->(p)",
                i,
                i,
                i as f64 * 100.0
            ))
            .unwrap();
        }

        db.checkpoint().unwrap();

        // Post-checkpoint: 5 more customers + 5 more products (WAL only)
        for i in 6..=10 {
            db.execute(&format!(
                "CREATE (c:Customer {{id: {}, name: 'PostCust{}', email: 'post{}@shop.com'}})",
                i, i, i
            ))
            .unwrap();
        }
        for i in 6..=10 {
            db.execute(&format!(
                "CREATE (p:Product {{id: {}, name: 'PostProd{}', price: {:.2}}})",
                i,
                i,
                i as f64 * 10.0
            ))
            .unwrap();
        }
        // Post-checkpoint relationships
        for i in 6..=10 {
            db.execute(&format!(
                "MATCH (c:Customer), (p:Product) WHERE c.id = {} AND p.id = {} \
                 CREATE (c)-[:PURCHASED {{amount: {:.2}}}]->(p)",
                i,
                i,
                i as f64 * 100.0
            ))
            .unwrap();
        }
        // Update a pre-checkpoint node
        db.execute("MATCH (c:Customer) WHERE c.id = 1 SET c.name = 'UpdatedCust1'")
            .unwrap();
        // No checkpoint — WAL data only
    }

    // Phase 2: reopen, verify all data recovered
    {
        let db = Database::open(&path).unwrap();

        // All 10 customers
        let r = db.query("MATCH (c:Customer) RETURN COUNT(c)").unwrap();
        assert_eq!(r.rows()[0].get_int(0).unwrap(), 10, "should recover all 10 customers");

        // All 10 products
        let r = db.query("MATCH (p:Product) RETURN COUNT(p)").unwrap();
        assert_eq!(r.rows()[0].get_int(0).unwrap(), 10, "should recover all 10 products");

        // All 10 purchase relationships
        let r = db
            .query("MATCH (c:Customer)-[:PURCHASED]->(p:Product) RETURN COUNT(c)")
            .unwrap();
        assert_eq!(r.rows()[0].get_int(0).unwrap(), 10, "should recover all 10 purchases");

        // Pre-checkpoint customer spot check
        let r = db.query("MATCH (c:Customer) WHERE c.id = 3 RETURN c.name").unwrap();
        assert_eq!(r.num_rows(), 1);
        assert_eq!(r.rows()[0].get_string(0).unwrap(), "PreCust3");

        // Post-checkpoint customer spot check
        let r = db.query("MATCH (c:Customer) WHERE c.id = 8 RETURN c.name").unwrap();
        assert_eq!(r.num_rows(), 1);
        assert_eq!(r.rows()[0].get_string(0).unwrap(), "PostCust8");

        // Updated node recovered correctly
        let r = db.query("MATCH (c:Customer) WHERE c.id = 1 RETURN c.name").unwrap();
        assert_eq!(r.rows()[0].get_string(0).unwrap(), "UpdatedCust1");

        let issues = db.check();
        assert!(issues.is_empty(), "integrity issues after WAL recovery: {:?}", issues);
    }

    cleanup(&path);
}

// ============================================================
// G-04: Edge property filtering, returning, aggregation
// ============================================================

#[test]
fn g04_edge_property_filter_return_aggregate() {
    let db = setup_ecommerce_db();

    // 1. Filter on Purchase.amount > 500.0 via the intermediary Purchase node
    // Purchases with amount > 500: (1,1,999.99), (1,2,699.99), (2,1,999.99),
    //   (3,2,699.99), (5,1,999.99), (7,1,999.99), (8,2,699.99) = 7
    let r = db
        .query(
            "MATCH (c:Customer)-[:MADE_PURCHASE]->(pur:Purchase)-[:PURCHASE_OF]->(p:Product) \
             WHERE pur.amount > 500.0 \
             RETURN COUNT(c)",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 1);
    let high_value_count = r.rows()[0].get_int(0).unwrap();
    assert_eq!(high_value_count, 7, "should have 7 purchases with amount > 500");

    // 2. RETURN Review.rating for a specific customer
    // Customer1 reviewed Product1 (rating=5) and Product2 (rating=4)
    let r = db
        .query(
            "MATCH (c:Customer)-[:WROTE_REVIEW]->(rev:Review)-[:REVIEW_OF]->(p:Product) \
             WHERE c.id = 1 \
             RETURN p.name, rev.rating ORDER BY p.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 2);
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "Laptop");
    assert_eq!(r.rows()[0].get_int(1).unwrap(), 5);
    assert_eq!(r.rows()[1].get_string(0).unwrap(), "Phone");
    assert_eq!(r.rows()[1].get_int(1).unwrap(), 4);

    // 3. Aggregation on Review.rating: sum and count for a specific product
    // Product1 (Laptop): ratings 5,5,4,5 = sum 19, count 4
    let r = db
        .query(
            "MATCH (c:Customer)-[:WROTE_REVIEW]->(rev:Review)-[:REVIEW_OF]->(p:Product) \
             WHERE p.id = 1 \
             RETURN SUM(rev.rating), COUNT(c)",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 1);
    let sum_rating = r.rows()[0].get_int(0).unwrap();
    let num_reviews = r.rows()[0].get_int(1).unwrap();
    assert_eq!(num_reviews, 4, "Laptop should have 4 reviews");
    assert_eq!(sum_rating, 19, "Laptop ratings sum should be 19");

    // 4. Filter combining purchase amount and product price
    // Purchases of products priced > 200 AND amount > 500
    let r = db
        .query(
            "MATCH (c:Customer)-[:MADE_PURCHASE]->(pur:Purchase)-[:PURCHASE_OF]->(p:Product) \
             WHERE pur.amount > 500.0 AND p.price > 200.0 \
             RETURN c.name, p.name, pur.amount ORDER BY pur.amount DESC",
        )
        .unwrap();
    // All purchases with amount > 500 are for Laptop(999.99), Phone(699.99),
    // Monitor(349.99) — all priced > 200
    assert!(r.num_rows() >= 5, "should have multiple high-value purchases of expensive products");
    // The first row (highest amount) should be 999.99
    let top_amount = r.rows()[0].get_float(2).unwrap();
    assert!((top_amount - 999.99).abs() < 0.01, "top amount should be 999.99");

    // 5. COUNT reviews by rating value
    let r = db
        .query(
            "MATCH (c:Customer)-[:WROTE_REVIEW]->(rev:Review) \
             RETURN rev.rating, COUNT(c) ORDER BY rev.rating",
        )
        .unwrap();
    assert!(r.num_rows() >= 3, "should have at least 3 distinct rating values");

    // 6. Verify edge properties are stored correctly via Dijkstra
    // Create a small weighted graph to test edge property storage via dijkstra
    db.execute("CREATE NODE TABLE City(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE ROAD(FROM City TO City, distance DOUBLE)").unwrap();
    db.execute("CREATE (c:City {id: 1, name: 'A'})").unwrap();
    db.execute("CREATE (c:City {id: 2, name: 'B'})").unwrap();
    db.execute("CREATE (c:City {id: 3, name: 'C'})").unwrap();
    db.execute(
        "MATCH (a:City), (b:City) WHERE a.id = 1 AND b.id = 2 \
         CREATE (a)-[:ROAD {distance: 1.0}]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:City), (b:City) WHERE a.id = 2 AND b.id = 3 \
         CREATE (a)-[:ROAD {distance: 2.0}]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:City), (b:City) WHERE a.id = 1 AND b.id = 3 \
         CREATE (a)-[:ROAD {distance: 10.0}]->(b)",
    )
    .unwrap();

    // Dijkstra reads edge properties — verifies they are correctly stored
    let r = db
        .query("CALL dijkstra(1, 3, 'ROAD', 'distance') YIELD path, cost")
        .unwrap();
    assert_eq!(r.num_rows(), 1);
    assert_eq!(
        r.rows()[0].get_float(1).unwrap(),
        3.0,
        "shortest path cost should be 3.0 (A->B->C), not 10.0 (A->C)"
    );
}

// ============================================================
// G-05: Edge-level delete and update via node operations
// ============================================================

#[test]
fn g05_edge_delete_and_update() {
    let db = setup_ecommerce_db();

    // Pre-check: Customer3 has PURCHASED edges
    let r = db
        .query("MATCH (c:Customer)-[:PURCHASED]->(p:Product) WHERE c.id = 3 RETURN COUNT(p)")
        .unwrap();
    let c3_purchases_before = r.rows()[0].get_int(0).unwrap();
    assert!(c3_purchases_before > 0, "Customer3 should have purchases");

    // Pre-check: total PURCHASED edge count
    let r = db
        .query("MATCH (c:Customer)-[:PURCHASED]->(p:Product) RETURN COUNT(c)")
        .unwrap();
    let total_purchases_before = r.rows()[0].get_int(0).unwrap();

    // Pre-check: total customer count
    let r = db.query("MATCH (c:Customer) RETURN COUNT(c)").unwrap();
    let total_customers_before = r.rows()[0].get_int(0).unwrap();

    // DETACH DELETE Customer3 — removes the node AND its edges
    db.execute("MATCH (c:Customer) WHERE c.id = 3 DETACH DELETE c").unwrap();

    // Verify Customer3 is gone
    let r = db.query("MATCH (c:Customer) WHERE c.id = 3 RETURN c.name").unwrap();
    assert_eq!(r.num_rows(), 0, "Customer3 should be deleted");

    // Verify Customer3's PURCHASED edges are gone
    let r = db
        .query("MATCH (c:Customer)-[:PURCHASED]->(p:Product) WHERE c.id = 3 RETURN COUNT(p)")
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 0, "Customer3's purchases should be removed");

    // Verify Customer3's MADE_PURCHASE edges are gone
    let r = db
        .query("MATCH (c:Customer)-[:MADE_PURCHASE]->(pur:Purchase) WHERE c.id = 3 RETURN COUNT(pur)")
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 0, "Customer3's purchase links should be removed");

    // Other customers still exist
    let r = db.query("MATCH (c:Customer) RETURN COUNT(c)").unwrap();
    assert_eq!(
        r.rows()[0].get_int(0).unwrap(),
        total_customers_before - 1,
        "only one customer should be removed"
    );

    // Other customers' PURCHASED edges are intact
    let r = db
        .query("MATCH (c:Customer)-[:PURCHASED]->(p:Product) RETURN COUNT(c)")
        .unwrap();
    assert_eq!(
        r.rows()[0].get_int(0).unwrap(),
        total_purchases_before - c3_purchases_before,
        "only Customer3's purchases should be removed"
    );

    // All products still exist (DETACH DELETE removes node+edges, not connected nodes)
    let r = db.query("MATCH (p:Product) RETURN COUNT(p)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 10, "products should not be affected");

    // Test node property update (SET) — update Customer1's name
    db.execute("MATCH (c:Customer) WHERE c.id = 1 SET c.name = 'VIP_Customer1'")
        .unwrap();
    let r = db.query("MATCH (c:Customer) WHERE c.id = 1 RETURN c.name").unwrap();
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "VIP_Customer1");

    // Verify Customer1's PURCHASED edges are still intact after SET
    let r = db
        .query("MATCH (c:Customer)-[:PURCHASED]->(p:Product) WHERE c.id = 1 RETURN COUNT(p)")
        .unwrap();
    assert!(
        r.rows()[0].get_int(0).unwrap() > 0,
        "Customer1's purchases should survive after SET"
    );

    let issues = db.check();
    assert!(issues.is_empty(), "integrity issues after edge operations: {:?}", issues);
}

// ============================================================
// G-06: High-degree hotspot node (500+ edges)
// ============================================================

#[test]
fn g06_hotspot_node_high_degree() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Customer(id INT64, name STRING, spent DOUBLE, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE NODE TABLE Product(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE PURCHASED(FROM Customer TO Product)").unwrap();

    // Create one "hot" product (id=1)
    db.execute("CREATE (p:Product {id: 1, name: 'BestSeller'})").unwrap();

    // Create 550 customers, each purchasing the hot product
    let num_customers: i64 = 550;
    for i in 1..=num_customers {
        db.execute(&format!(
            "CREATE (c:Customer {{id: {}, name: 'Buyer{}', spent: {:.2}}})",
            i,
            i,
            (i as f64) * 1.5
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (c:Customer), (p:Product) WHERE c.id = {} AND p.id = 1 \
             CREATE (c)-[:PURCHASED]->(p)",
            i
        ))
        .unwrap();
    }

    // Verify degree (in-degree on Product1)
    let r = db
        .query(
            "MATCH (c:Customer)-[:PURCHASED]->(p:Product) \
             WHERE p.id = 1 \
             RETURN COUNT(c)",
        )
        .unwrap();
    assert_eq!(
        r.rows()[0].get_int(0).unwrap(),
        num_customers,
        "hot product should have {} purchasers",
        num_customers
    );

    // Verify total customer count
    let r = db.query("MATCH (c:Customer) RETURN COUNT(c)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), num_customers);

    // Aggregation over node properties of connected customers
    let r = db
        .query(
            "MATCH (c:Customer)-[:PURCHASED]->(p:Product) \
             WHERE p.id = 1 \
             RETURN SUM(c.spent)",
        )
        .unwrap();
    let total_spent = r.rows()[0].get_float(0).unwrap();
    // sum(1..=550 * 1.5) = 1.5 * (550 * 551 / 2) = 1.5 * 151525 = 227287.5
    let expected = 1.5 * (num_customers as f64 * (num_customers as f64 + 1.0) / 2.0);
    assert!(
        (total_spent - expected).abs() < 0.01,
        "total spent {} should be close to {}",
        total_spent,
        expected
    );

    // Query specific customers of the hotspot — should not panic
    let r = db
        .query(
            "MATCH (c:Customer)-[:PURCHASED]->(p:Product) \
             WHERE p.id = 1 AND c.id <= 5 \
             RETURN c.name ORDER BY c.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 5);

    let issues = db.check();
    assert!(issues.is_empty(), "integrity issues with hotspot: {:?}", issues);
}

// ============================================================
// G-07: Dense subgraph (clique) and variable-length paths
// ============================================================

#[test]
fn g07_dense_subgraph_clique() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE KNOWS(FROM Person TO Person)").unwrap();

    // Create 10-node clique: all pairs connected (directed)
    let clique_size = 10;
    for i in 1..=clique_size {
        db.execute(&format!(
            "CREATE (p:Person {{id: {}, name: 'P{}'}})",
            i, i
        ))
        .unwrap();
    }

    // Create all directed edges (i -> j for i != j)
    let mut edge_count = 0;
    for i in 1..=clique_size {
        for j in 1..=clique_size {
            if i != j {
                db.execute(&format!(
                    "MATCH (a:Person), (b:Person) WHERE a.id = {} AND b.id = {} \
                     CREATE (a)-[:KNOWS]->(b)",
                    i, j
                ))
                .unwrap();
                edge_count += 1;
            }
        }
    }

    // Verify node count
    let r = db.query("MATCH (p:Person) RETURN COUNT(p)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), clique_size);

    // Verify edge count: n*(n-1) = 10*9 = 90
    let r = db
        .query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN COUNT(a)")
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), edge_count);

    // Variable-length path: from P1, 1 hop should reach all other 9 nodes
    let r = db
        .query(
            "MATCH (a:Person)-[:KNOWS*1..1]->(b:Person) \
             WHERE a.id = 1 \
             RETURN DISTINCT b.id ORDER BY b.id",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 9, "P1 should directly reach 9 other nodes in clique");

    // Variable-length path: from P1, 2 hops — should still reach all 9 (clique property)
    let r = db
        .query(
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) \
             WHERE a.id = 1 AND b.id <> 1 \
             RETURN DISTINCT b.id ORDER BY b.id",
        )
        .unwrap();
    assert_eq!(
        r.num_rows(),
        9,
        "P1 should reach all 9 other nodes within 2 hops in clique"
    );

    // Count all edges: should be n*(n-1) = 90
    let r = db
        .query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) \
             RETURN COUNT(a)",
        )
        .unwrap();
    assert_eq!(
        r.rows()[0].get_int(0).unwrap(),
        edge_count,
        "total edge count in clique should be {}",
        edge_count
    );

    let issues = db.check();
    assert!(issues.is_empty(), "integrity issues in dense subgraph: {:?}", issues);
}

// ============================================================
// G-08: Wide schema (15+ columns) with NULLs and long strings
// ============================================================

#[test]
fn g08_wide_schema_nulls_long_strings() {
    let path = temp_db_path("g08_wide");
    cleanup(&path);

    // Phase 1: create wide schema, insert data, close
    {
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE NODE TABLE WideNode(\
                id INT64, \
                col_str1 STRING, \
                col_str2 STRING, \
                col_str3 STRING, \
                col_str4 STRING, \
                col_str5 STRING, \
                col_int1 INT64, \
                col_int2 INT64, \
                col_int3 INT64, \
                col_int4 INT64, \
                col_int5 INT64, \
                col_float1 DOUBLE, \
                col_float2 DOUBLE, \
                col_float3 DOUBLE, \
                col_bool1 BOOL, \
                col_bool2 BOOL, \
                PRIMARY KEY(id)\
            )",
        )
        .unwrap();

        // Row 1: all fields populated, col_str1 is a long string
        let long_str: String = (0..2000).map(|i| (b'a' + (i % 26) as u8) as char).collect();
        db.execute(&format!(
            "CREATE (n:WideNode {{id: 1, \
                col_str1: '{}', col_str2: 'short', col_str3: 'medium text here', \
                col_str4: 'value4', col_str5: 'value5', \
                col_int1: 100, col_int2: 200, col_int3: 300, col_int4: 400, col_int5: 500, \
                col_float1: 1.1, col_float2: 2.2, col_float3: 3.3, \
                col_bool1: true, col_bool2: false}})",
            long_str
        ))
        .unwrap();

        // Row 2: some NULLs (omit col_str2, col_int2, col_float2, col_bool2)
        db.execute(
            "CREATE (n:WideNode {id: 2, \
                col_str1: 'row2_str1', col_str3: 'row2_str3', \
                col_str4: 'row2_str4', col_str5: 'row2_str5', \
                col_int1: 10, col_int3: 30, col_int4: 40, col_int5: 50, \
                col_float1: 0.1, col_float3: 0.3, \
                col_bool1: false})",
        )
        .unwrap();

        // Row 3: mostly NULLs, only id and one long string
        let very_long: String = "X".repeat(5000);
        db.execute(&format!(
            "CREATE (n:WideNode {{id: 3, col_str1: '{}'}})",
            very_long
        ))
        .unwrap();

        // Row 4: all numeric, no strings (except id)
        db.execute(
            "CREATE (n:WideNode {id: 4, \
                col_int1: 1000, col_int2: 2000, col_int3: 3000, col_int4: 4000, col_int5: 5000, \
                col_float1: 99.9, col_float2: 88.8, col_float3: 77.7, \
                col_bool1: true, col_bool2: true})",
        )
        .unwrap();

        // Row 5: empty strings
        db.execute(
            "CREATE (n:WideNode {id: 5, \
                col_str1: '', col_str2: '', col_str3: '', col_str4: '', col_str5: ''})",
        )
        .unwrap();
    }

    // Phase 2: reopen and verify
    {
        let db = Database::open(&path).unwrap();

        let r = db.query("MATCH (n:WideNode) RETURN COUNT(n)").unwrap();
        assert_eq!(r.rows()[0].get_int(0).unwrap(), 5, "should have 5 wide rows");

        // Row 1: verify long string preserved
        let r = db
            .query("MATCH (n:WideNode) WHERE n.id = 1 RETURN n.col_str1, n.col_int1, n.col_bool1")
            .unwrap();
        assert_eq!(r.num_rows(), 1);
        let s = r.rows()[0].get_string(0).unwrap();
        assert_eq!(s.len(), 2000, "long string should be 2000 chars");
        assert_eq!(r.rows()[0].get_int(1).unwrap(), 100);
        assert_eq!(r.rows()[0].get_bool(2).unwrap(), true);

        // Row 2: verify NULLs
        let r = db
            .query("MATCH (n:WideNode) WHERE n.id = 2 RETURN n.col_str2, n.col_int2, n.col_float2, n.col_bool2")
            .unwrap();
        assert_eq!(r.num_rows(), 1);
        assert!(r.rows()[0].values[0].is_null(), "col_str2 should be NULL for row 2");
        assert!(r.rows()[0].values[1].is_null(), "col_int2 should be NULL for row 2");
        assert!(r.rows()[0].values[2].is_null(), "col_float2 should be NULL for row 2");
        assert!(r.rows()[0].values[3].is_null(), "col_bool2 should be NULL for row 2");

        // Row 3: mostly NULLs, long string preserved
        let r = db
            .query("MATCH (n:WideNode) WHERE n.id = 3 RETURN n.col_str1, n.col_int1")
            .unwrap();
        assert_eq!(r.num_rows(), 1);
        let s = r.rows()[0].get_string(0).unwrap();
        assert_eq!(s.len(), 5000, "very long string should be 5000 chars");
        assert!(r.rows()[0].values[1].is_null(), "col_int1 should be NULL for row 3");

        // IS NULL filter
        let r = db
            .query("MATCH (n:WideNode) WHERE n.col_str2 IS NULL RETURN n.id ORDER BY n.id")
            .unwrap();
        // Rows 2, 3, 4 have NULL col_str2
        assert!(r.num_rows() >= 3, "at least 3 rows should have NULL col_str2");

        // IS NOT NULL filter
        let r = db
            .query("MATCH (n:WideNode) WHERE n.col_bool1 IS NOT NULL RETURN n.id ORDER BY n.id")
            .unwrap();
        // Rows 1, 2, 4 have non-NULL col_bool1
        assert_eq!(r.num_rows(), 3, "3 rows should have non-NULL col_bool1");

        let issues = db.check();
        assert!(issues.is_empty(), "integrity issues with wide schema: {:?}", issues);
    }

    cleanup(&path);
}

// ============================================================
// G-09: MERGE idempotent import
// ============================================================

#[test]
fn g09_merge_idempotent_import() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Customer(id INT64, name STRING, email STRING, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE NODE TABLE Product(id INT64, name STRING, price DOUBLE, PRIMARY KEY(id))")
        .unwrap();

    // First import batch
    for i in 1..=5 {
        db.execute(&format!(
            "MERGE (c:Customer {{id: {}, name: 'Cust{}'}}) ON CREATE SET c.email = 'cust{}@v1.com'",
            i, i, i
        ))
        .unwrap();
    }
    for i in 1..=3 {
        db.execute(&format!(
            "MERGE (p:Product {{id: {}, name: 'Prod{}'}}) ON CREATE SET p.price = {:.2}",
            i,
            i,
            i as f64 * 10.0
        ))
        .unwrap();
    }

    let r = db.query("MATCH (c:Customer) RETURN COUNT(c)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 5);
    let r = db.query("MATCH (p:Product) RETURN COUNT(p)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 3);

    // Replay the same batch — should not create duplicates
    for i in 1..=5 {
        db.execute(&format!(
            "MERGE (c:Customer {{id: {}, name: 'Cust{}'}}) ON MATCH SET c.email = 'cust{}@v2.com'",
            i, i, i
        ))
        .unwrap();
    }
    for i in 1..=3 {
        db.execute(&format!(
            "MERGE (p:Product {{id: {}, name: 'Prod{}'}}) ON MATCH SET p.price = {:.2}",
            i,
            i,
            i as f64 * 20.0
        ))
        .unwrap();
    }

    // Count unchanged
    let r = db.query("MATCH (c:Customer) RETURN COUNT(c)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 5, "MERGE should not create duplicates");
    let r = db.query("MATCH (p:Product) RETURN COUNT(p)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 3, "MERGE should not create duplicates");

    // ON MATCH SET should have updated email to v2
    let r = db
        .query("MATCH (c:Customer) WHERE c.id = 1 RETURN c.email")
        .unwrap();
    assert_eq!(
        r.rows()[0].get_string(0).unwrap(),
        "cust1@v2.com",
        "ON MATCH SET should update email"
    );

    // ON MATCH SET should have updated price
    let r = db
        .query("MATCH (p:Product) WHERE p.id = 2 RETURN p.price")
        .unwrap();
    assert!(
        (r.rows()[0].get_float(0).unwrap() - 40.0).abs() < 0.01,
        "ON MATCH SET should update price to 40.0"
    );

    // Third replay with only MERGE (no ON CREATE / ON MATCH)
    for i in 1..=5 {
        db.execute(&format!("MERGE (c:Customer {{id: {}, name: 'Cust{}'}})", i, i)).unwrap();
    }
    let r = db.query("MATCH (c:Customer) RETURN COUNT(c)").unwrap();
    assert_eq!(
        r.rows()[0].get_int(0).unwrap(),
        5,
        "third MERGE replay should still be idempotent"
    );

    // MERGE with a new customer — should create
    db.execute("MERGE (c:Customer {id: 6, name: 'Cust6'}) ON CREATE SET c.email = 'new@shop.com'")
        .unwrap();
    let r = db.query("MATCH (c:Customer) RETURN COUNT(c)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 6, "MERGE of new node should create it");
}

// ============================================================
// G-10: Mixed read/write stress with db.check()
// ============================================================

#[test]
fn g10_mixed_read_write_consistency() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Item(id INT64, val INT64, label STRING, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE REL TABLE LINK(FROM Item TO Item)").unwrap();

    let mut live_ids: Vec<i64> = Vec::new();
    let mut next_id: i64 = 1;
    let mut insert_count = 0;
    let mut update_count = 0;
    let mut delete_count = 0;
    let mut query_count = 0;
    let mut _link_count: i64 = 0;

    for step in 0..400 {
        let op = step % 10;
        match op {
            // INSERT node (40%)
            0..=3 => {
                db.execute(&format!(
                    "CREATE (n:Item {{id: {}, val: {}, label: 'item_{}'}})",
                    next_id,
                    next_id * 3,
                    next_id
                ))
                .unwrap();
                live_ids.push(next_id);
                next_id += 1;
                insert_count += 1;

                // Also create a link to a random existing node if possible
                if live_ids.len() >= 2 {
                    let src = live_ids[live_ids.len() - 1];
                    let dst = live_ids[step % (live_ids.len() - 1)];
                    if src != dst {
                        let _ = db.execute(&format!(
                            "MATCH (a:Item), (b:Item) WHERE a.id = {} AND b.id = {} \
                             CREATE (a)-[:LINK]->(b)",
                            src, dst
                        ));
                        _link_count += 1;
                    }
                }
            }
            // QUERY (20%)
            4..=5 => {
                let result = db.query("MATCH (n:Item) RETURN COUNT(n)").unwrap();
                let count = result.rows()[0].get_int(0).unwrap();
                assert_eq!(count, live_ids.len() as i64, "count mismatch at step {}", step);
                query_count += 1;
            }
            // UPDATE (20%)
            6..=7 => {
                if !live_ids.is_empty() {
                    let target = live_ids[step % live_ids.len()];
                    db.execute(&format!(
                        "MATCH (n:Item) WHERE n.id = {} SET n.val = {}",
                        target,
                        target * 100
                    ))
                    .unwrap();
                    update_count += 1;
                }
            }
            // DELETE (20%)
            _ => {
                if !live_ids.is_empty() {
                    let idx = step % live_ids.len();
                    let target = live_ids[idx];
                    // Use DETACH DELETE since node might have edges
                    db.execute(&format!("MATCH (n:Item) WHERE n.id = {} DETACH DELETE n", target))
                        .unwrap();
                    live_ids.remove(idx);
                    delete_count += 1;
                }
            }
        }
    }

    // Verify final node count
    let result = db.query("MATCH (n:Item) RETURN COUNT(n)").unwrap();
    let final_count = result.rows()[0].get_int(0).unwrap();
    assert_eq!(final_count, live_ids.len() as i64, "final count mismatch");

    // Verify all live IDs are queryable
    for &id in &live_ids {
        let r = db.query(&format!("MATCH (n:Item) WHERE n.id = {} RETURN n.id", id)).unwrap();
        assert_eq!(r.num_rows(), 1, "live node id={} should exist", id);
    }

    // Verify total operations
    let total = insert_count + update_count + delete_count + query_count;
    assert!(total >= 300, "total ops {} should be >= 300", total);

    // Integrity check
    let issues = db.check();
    assert!(issues.is_empty(), "integrity issues after mixed ops: {:?}", issues);
}

// ============================================================
// G-11: Multi-tenant data isolation
// ============================================================

#[test]
fn g11_multi_tenant_isolation() {
    let db = Database::in_memory();
    db.execute(
        "CREATE NODE TABLE TenantUser(id INT64, org_id INT64, name STRING, role STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute(
        "CREATE NODE TABLE TenantProject(id INT64, org_id INT64, name STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE WORKS_ON(FROM TenantUser TO TenantProject)").unwrap();

    // Org 1: 10 users, 3 projects
    for i in 1..=10 {
        db.execute(&format!(
            "CREATE (u:TenantUser {{id: {}, org_id: 1, name: 'Org1_User{}', role: '{}'}})",
            i,
            i,
            if i <= 2 { "admin" } else { "member" }
        ))
        .unwrap();
    }
    for i in 1..=3 {
        db.execute(&format!(
            "CREATE (p:TenantProject {{id: {}, org_id: 1, name: 'Org1_Proj{}'}})",
            i, i
        ))
        .unwrap();
    }
    // All org1 users work on project 1
    for i in 1..=10 {
        db.execute(&format!(
            "MATCH (u:TenantUser), (p:TenantProject) WHERE u.id = {} AND p.id = 1 \
             CREATE (u)-[:WORKS_ON]->(p)",
            i
        ))
        .unwrap();
    }

    // Org 2: 8 users, 2 projects (IDs offset to avoid collision)
    for i in 11..=18 {
        db.execute(&format!(
            "CREATE (u:TenantUser {{id: {}, org_id: 2, name: 'Org2_User{}', role: '{}'}})",
            i,
            i - 10,
            if i <= 12 { "admin" } else { "member" }
        ))
        .unwrap();
    }
    for i in 4..=5 {
        db.execute(&format!(
            "CREATE (p:TenantProject {{id: {}, org_id: 2, name: 'Org2_Proj{}'}})",
            i,
            i - 3
        ))
        .unwrap();
    }
    for i in 11..=18 {
        db.execute(&format!(
            "MATCH (u:TenantUser), (p:TenantProject) WHERE u.id = {} AND p.id = 4 \
             CREATE (u)-[:WORKS_ON]->(p)",
            i
        ))
        .unwrap();
    }

    // Org 3: 5 users, 1 project
    for i in 19..=23 {
        db.execute(&format!(
            "CREATE (u:TenantUser {{id: {}, org_id: 3, name: 'Org3_User{}', role: 'member'}})",
            i,
            i - 18
        ))
        .unwrap();
    }
    db.execute("CREATE (p:TenantProject {id: 6, org_id: 3, name: 'Org3_Proj1'})").unwrap();
    for i in 19..=23 {
        db.execute(&format!(
            "MATCH (u:TenantUser), (p:TenantProject) WHERE u.id = {} AND p.id = 6 \
             CREATE (u)-[:WORKS_ON]->(p)",
            i
        ))
        .unwrap();
    }

    // Isolation checks

    // Org1 user query should only return org1 users
    let r = db
        .query(
            "MATCH (u:TenantUser) WHERE u.org_id = 1 \
             RETURN u.name ORDER BY u.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 10, "org1 should have 10 users");
    for row in r.rows() {
        let name = row.get_string(0).unwrap();
        assert!(
            name.starts_with("Org1_"),
            "org1 query returned non-org1 user: {}",
            name
        );
    }

    // Org2 user query
    let r = db
        .query("MATCH (u:TenantUser) WHERE u.org_id = 2 RETURN COUNT(u)")
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 8, "org2 should have 8 users");

    // Org3 user query
    let r = db
        .query("MATCH (u:TenantUser) WHERE u.org_id = 3 RETURN COUNT(u)")
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 5, "org3 should have 5 users");

    // Cross-check: org1 projects should not appear in org2 project query
    let r = db
        .query(
            "MATCH (p:TenantProject) WHERE p.org_id = 2 \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 2, "org2 should have 2 projects");
    for row in r.rows() {
        let name = row.get_string(0).unwrap();
        assert!(
            name.starts_with("Org2_"),
            "org2 project query returned non-org2 project: {}",
            name
        );
    }

    // Relationship isolation: org1 users' WORKS_ON should only connect to org1 projects
    let r = db
        .query(
            "MATCH (u:TenantUser)-[:WORKS_ON]->(p:TenantProject) \
             WHERE u.org_id = 1 \
             RETURN p.org_id, COUNT(u)",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 1, "org1 users should only work on projects of one org");
    assert_eq!(
        r.rows()[0].get_int(0).unwrap(),
        1,
        "org1 users should only work on org1 projects"
    );

    // Relationship isolation: org2 users' WORKS_ON should only connect to org2 projects
    let r = db
        .query(
            "MATCH (u:TenantUser)-[:WORKS_ON]->(p:TenantProject) \
             WHERE u.org_id = 2 \
             RETURN p.org_id, COUNT(u)",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 1, "org2 users should only work on projects of one org");
    assert_eq!(
        r.rows()[0].get_int(0).unwrap(),
        2,
        "org2 users should only work on org2 projects"
    );

    // Aggregation scoped to tenant: admin count per org
    let r = db
        .query(
            "MATCH (u:TenantUser) WHERE u.org_id = 1 AND u.role = 'admin' \
             RETURN COUNT(u)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 2, "org1 should have 2 admins");

    let r = db
        .query(
            "MATCH (u:TenantUser) WHERE u.org_id = 2 AND u.role = 'admin' \
             RETURN COUNT(u)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 2, "org2 should have 2 admins");

    // Total across all tenants (sanity check)
    let r = db.query("MATCH (u:TenantUser) RETURN COUNT(u)").unwrap();
    assert_eq!(
        r.rows()[0].get_int(0).unwrap(),
        23,
        "total users across all orgs should be 23"
    );

    let issues = db.check();
    assert!(issues.is_empty(), "integrity issues in multi-tenant setup: {:?}", issues);
}

// ============================================================
// G-12: Transaction rollback mid-batch recovery
// ============================================================

#[test]
fn g12_transaction_rollback_mid_batch() {
    let path = temp_db_path("g12_rollback");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE Item(id INT64, batch STRING, PRIMARY KEY(id))").unwrap();

        // Batch 1: committed successfully
        db.execute_script(
            "BEGIN; \
             CREATE (n:Item {id: 1, batch: 'batch1'}); \
             CREATE (n:Item {id: 2, batch: 'batch1'}); \
             CREATE (n:Item {id: 3, batch: 'batch1'}); \
             CREATE (n:Item {id: 4, batch: 'batch1'}); \
             CREATE (n:Item {id: 5, batch: 'batch1'}); \
             COMMIT;",
        )
        .unwrap();

        // Verify batch 1 committed
        let r = db.query("MATCH (n:Item) RETURN COUNT(n)").unwrap();
        assert_eq!(r.rows()[0].get_int(0).unwrap(), 5, "batch1 should have 5 items");

        // Batch 2: will be rolled back
        db.execute_script(
            "BEGIN; \
             CREATE (n:Item {id: 6, batch: 'batch2'}); \
             CREATE (n:Item {id: 7, batch: 'batch2'}); \
             CREATE (n:Item {id: 8, batch: 'batch2'}); \
             ROLLBACK;",
        )
        .unwrap();

        // Verify rollback: still only 5 items
        let r = db.query("MATCH (n:Item) RETURN COUNT(n)").unwrap();
        assert_eq!(
            r.rows()[0].get_int(0).unwrap(),
            5,
            "rolled-back batch2 should not persist"
        );

        // Verify none of batch2 items exist
        let r = db
            .query("MATCH (n:Item) WHERE n.batch = 'batch2' RETURN n.id")
            .unwrap();
        assert_eq!(r.num_rows(), 0, "batch2 items should not exist after rollback");

        // Batch 3: committed after the rollback
        db.execute_script(
            "BEGIN; \
             CREATE (n:Item {id: 9, batch: 'batch3'}); \
             CREATE (n:Item {id: 10, batch: 'batch3'}); \
             COMMIT;",
        )
        .unwrap();

        let r = db.query("MATCH (n:Item) RETURN COUNT(n)").unwrap();
        assert_eq!(
            r.rows()[0].get_int(0).unwrap(),
            7,
            "batch1 + batch3 = 7 items"
        );
    }

    // Reopen and verify persistence
    {
        let db = Database::open(&path).unwrap();

        // Total count: batch1 (5) + batch3 (2) = 7
        let r = db.query("MATCH (n:Item) RETURN COUNT(n)").unwrap();
        assert_eq!(
            r.rows()[0].get_int(0).unwrap(),
            7,
            "after reopen, should have 7 items (batch1 + batch3)"
        );

        // batch2 still absent
        let r = db
            .query("MATCH (n:Item) WHERE n.batch = 'batch2' RETURN n.id")
            .unwrap();
        assert_eq!(r.num_rows(), 0, "batch2 should not exist after reopen");

        // Verify batch1 items are all present
        let r = db
            .query("MATCH (n:Item) WHERE n.batch = 'batch1' RETURN n.id ORDER BY n.id")
            .unwrap();
        assert_eq!(r.num_rows(), 5);
        for (i, row) in r.rows().iter().enumerate() {
            assert_eq!(row.get_int(0).unwrap(), (i + 1) as i64);
        }

        // Verify batch3 items are present
        let r = db
            .query("MATCH (n:Item) WHERE n.batch = 'batch3' RETURN n.id ORDER BY n.id")
            .unwrap();
        assert_eq!(r.num_rows(), 2);
        assert_eq!(r.rows()[0].get_int(0).unwrap(), 9);
        assert_eq!(r.rows()[1].get_int(0).unwrap(), 10);

        // No polluted state — IDs 6, 7, 8 should not exist
        for id in [6, 7, 8] {
            let r = db
                .query(&format!("MATCH (n:Item) WHERE n.id = {} RETURN n.id", id))
                .unwrap();
            assert_eq!(r.num_rows(), 0, "rolled-back item id={} should not exist", id);
        }

        let issues = db.check();
        assert!(issues.is_empty(), "integrity issues after rollback recovery: {:?}", issues);
    }

    cleanup(&path);
}
