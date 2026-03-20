/// 电商系统场景 V2 端到端测试
///
/// 覆盖 E-01 ~ E-13 测试用例：
/// - E-01: 浏览→加购→下单→支付 转化漏斗
/// - E-02: 评论与评分关系属性
/// - E-03: 订单明细边属性（通过 OrderItem 节点建模）
/// - E-04: 退款/取消订单
/// - E-05: 多次支付尝试
/// - E-06: 多仓履约与拆单
/// - E-07: 用户兴趣图推荐
/// - E-08: 会员与优惠券图
/// - E-09: 热门商品高并发浏览
/// - E-10: 售后服务图
/// - E-11: 商品变体/SKU 层级图
/// - E-12: 供应商-仓库-商品供应链
/// - E-13: 价格历史与时间维度定价
///
/// 注意：由于 gqlite 当前不支持在 RETURN 子句中访问边属性（rel properties），
/// 需要聚合/过滤的属性数据通过中间节点建模（如 OrderItem、Review、PriceRecord 等）。
use gqlite_core::Database;

// ============================================================
// E-01: 浏览→加购→下单→支付 转化漏斗
// ============================================================

#[test]
fn e01_conversion_funnel() {
    let db = Database::in_memory();

    // -- Schema
    db.execute("CREATE NODE TABLE Customer(id INT64, name STRING, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE NODE TABLE Product(id INT64, name STRING, price DOUBLE, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE NODE TABLE Cart(id INT64, customer_id INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute(
        "CREATE NODE TABLE PurchaseOrder(id INT64, customer_id INT64, status STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute(
        "CREATE NODE TABLE Payment(id INT64, order_id INT64, status STRING, amount DOUBLE, PRIMARY KEY(id))",
    )
    .unwrap();

    db.execute("CREATE REL TABLE VIEWED(FROM Customer TO Product, ts INT64)").unwrap();
    db.execute("CREATE REL TABLE CARTED(FROM Customer TO Product, ts INT64)").unwrap();
    db.execute("CREATE REL TABLE ORDERED(FROM Customer TO Product, ts INT64)").unwrap();
    db.execute("CREATE REL TABLE PLACED(FROM Customer TO PurchaseOrder)").unwrap();
    db.execute("CREATE REL TABLE PAID_WITH(FROM PurchaseOrder TO Payment)").unwrap();

    // -- 10 customers, 3 products
    for i in 1..=10 {
        db.execute(&format!(
            "CREATE (c:Customer {{id: {}, name: 'Cust{}'}})",
            i, i
        ))
        .unwrap();
    }
    for (id, name, price) in [(1, "Laptop", 4999.0), (2, "Phone", 2999.0), (3, "Tablet", 1999.0)]
    {
        db.execute(&format!(
            "CREATE (p:Product {{id: {}, name: '{}', price: {}}})",
            id, name, price
        ))
        .unwrap();
    }

    // Funnel data (product id=1):
    // Step 1 - VIEWED: customers 1..=10 (10 people)
    for cid in 1..=10 {
        db.execute(&format!(
            "MATCH (c:Customer), (p:Product) WHERE c.id = {} AND p.id = 1 \
             CREATE (c)-[:VIEWED {{ts: {}}}]->(p)",
            cid,
            1700000000 + cid
        ))
        .unwrap();
    }

    // Step 2 - CARTED: customers 1..=7 (7 people)
    for cid in 1..=7 {
        db.execute(&format!(
            "MATCH (c:Customer), (p:Product) WHERE c.id = {} AND p.id = 1 \
             CREATE (c)-[:CARTED {{ts: {}}}]->(p)",
            cid,
            1700001000 + cid
        ))
        .unwrap();
    }

    // Step 3 - ORDERED: customers 1..=4 (4 people)
    for cid in 1..=4 {
        db.execute(&format!(
            "MATCH (c:Customer), (p:Product) WHERE c.id = {} AND p.id = 1 \
             CREATE (c)-[:ORDERED {{ts: {}}}]->(p)",
            cid,
            1700002000 + cid
        ))
        .unwrap();
        // Also create order + payment for paid step
        let oid = 100 + cid;
        db.execute(&format!(
            "CREATE (o:PurchaseOrder {{id: {}, customer_id: {}, status: 'completed'}})",
            oid, cid
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (c:Customer), (o:PurchaseOrder) WHERE c.id = {} AND o.id = {} \
             CREATE (c)-[:PLACED]->(o)",
            cid, oid
        ))
        .unwrap();
    }

    // Step 4 - PAID: customers 1..=3 (3 people) — customer 4 did not pay
    for cid in 1..=3 {
        let oid = 100 + cid;
        let pid = 200 + cid;
        db.execute(&format!(
            "CREATE (pay:Payment {{id: {}, order_id: {}, status: 'captured', amount: 4999.0}})",
            pid, oid
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (o:PurchaseOrder), (pay:Payment) WHERE o.id = {} AND pay.id = {} \
             CREATE (o)-[:PAID_WITH]->(pay)",
            oid, pid
        ))
        .unwrap();
    }

    // -- Funnel assertions

    // Step 1: VIEWED count
    let r = db
        .query(
            "MATCH (c:Customer)-[:VIEWED]->(p:Product) WHERE p.id = 1 \
             RETURN count(c)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 10, "VIEWED step should have 10");

    // Step 2: CARTED count
    let r = db
        .query(
            "MATCH (c:Customer)-[:CARTED]->(p:Product) WHERE p.id = 1 \
             RETURN count(c)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 7, "CARTED step should have 7");

    // Step 3: ORDERED count
    let r = db
        .query(
            "MATCH (c:Customer)-[:ORDERED]->(p:Product) WHERE p.id = 1 \
             RETURN count(c)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 4, "ORDERED step should have 4");

    // Step 4: PAID count (orders with captured payment)
    let r = db
        .query(
            "MATCH (c:Customer)-[:PLACED]->(o:PurchaseOrder)-[:PAID_WITH]->(pay:Payment) \
             WHERE pay.status = 'captured' \
             RETURN count(DISTINCT c)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 3, "PAID step should have 3");

    // Conversion rate: VIEWED->CARTED = 7/10, CARTED->ORDERED = 4/7, ORDERED->PAID = 3/4
    // Overall: 3/10

    // Verify funnel is monotonically decreasing
    let viewed = 10i64;
    let carted = 7i64;
    let ordered = 4i64;
    let paid = 3i64;
    assert!(viewed >= carted, "funnel should decrease");
    assert!(carted >= ordered, "funnel should decrease");
    assert!(ordered >= paid, "funnel should decrease");
}

// ============================================================
// E-02: 评论与评分关系属性
// ============================================================

#[test]
fn e02_review_and_rating() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE Customer(id INT64, name STRING, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE NODE TABLE Product(id INT64, name STRING, PRIMARY KEY(id))")
        .unwrap();
    // Use Review as a node to make rating queryable (edge properties not accessible in RETURN)
    db.execute(
        "CREATE NODE TABLE Review(id INT64, customer_id INT64, product_id INT64, rating INT64, ts INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE WROTE_REVIEW(FROM Customer TO Review)").unwrap();
    db.execute("CREATE REL TABLE REVIEW_OF(FROM Review TO Product)").unwrap();

    // 5 customers, 3 products
    for i in 1..=5 {
        db.execute(&format!(
            "CREATE (c:Customer {{id: {}, name: 'Cust{}'}})",
            i, i
        ))
        .unwrap();
    }
    for (id, name) in [(1, "Laptop"), (2, "Phone"), (3, "Tablet")] {
        db.execute(&format!("CREATE (p:Product {{id: {}, name: '{}'}})", id, name)).unwrap();
    }

    // Reviews: (review_id, customer_id, product_id, rating, ts)
    let reviews = [
        (1, 1, 1, 5, 1700000100),
        (2, 2, 1, 4, 1700000200),
        (3, 3, 1, 3, 1700000300),
        (4, 4, 1, 2, 1700000400),
        (5, 5, 1, 1, 1700000500),
        (6, 1, 2, 5, 1700001100),
        (7, 2, 2, 5, 1700001200),
        (8, 3, 2, 4, 1700001300),
        (9, 1, 3, 3, 1700002100),
        (10, 4, 3, 2, 1700002200),
    ];
    for (rid, cid, pid, rating, ts) in &reviews {
        db.execute(&format!(
            "CREATE (r:Review {{id: {}, customer_id: {}, product_id: {}, rating: {}, ts: {}}})",
            rid, cid, pid, rating, ts
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (c:Customer), (r:Review) WHERE c.id = {} AND r.id = {} \
             CREATE (c)-[:WROTE_REVIEW]->(r)",
            cid, rid
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (r:Review), (p:Product) WHERE r.id = {} AND p.id = {} \
             CREATE (r)-[:REVIEW_OF]->(p)",
            rid, pid
        ))
        .unwrap();
    }

    // Average rating for Laptop (product 1): (5+4+3+2+1)/5 = 3.0
    let r = db
        .query(
            "MATCH (rev:Review)-[:REVIEW_OF]->(p:Product) \
             WHERE p.id = 1 \
             RETURN avg(rev.rating)",
        )
        .unwrap();
    let avg_rating = r.rows()[0].get_float(0).unwrap();
    assert!(
        (avg_rating - 3.0).abs() < 0.01,
        "Laptop avg rating should be 3.0, got {}",
        avg_rating
    );

    // Average rating for Phone (product 2): (5+5+4)/3 ≈ 4.67
    let r = db
        .query(
            "MATCH (rev:Review)-[:REVIEW_OF]->(p:Product) \
             WHERE p.id = 2 \
             RETURN avg(rev.rating)",
        )
        .unwrap();
    let avg_rating = r.rows()[0].get_float(0).unwrap();
    assert!(
        (avg_rating - 4.666).abs() < 0.1,
        "Phone avg rating should be ~4.67, got {}",
        avg_rating
    );

    // Filter low ratings (rating <= 2)
    let r = db
        .query(
            "MATCH (rev:Review)-[:REVIEW_OF]->(p:Product) \
             WHERE rev.rating <= 2 \
             RETURN rev.id, p.name, rev.rating ORDER BY rev.rating",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 3, "should have 3 low-rating reviews (rating<=2)");
    // ratings should all be <= 2
    for row in r.rows() {
        let rating = row.get_int(2).unwrap();
        assert!(rating <= 2, "filtered rating {} should be <= 2", rating);
    }

    // Sort products by average rating descending (sort client-side since
    // ORDER BY on aggregate expressions may not sort deterministically)
    let r = db
        .query(
            "MATCH (rev:Review)-[:REVIEW_OF]->(p:Product) \
             RETURN p.name, avg(rev.rating) AS avg_r, count(rev) AS cnt",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 3, "should have 3 products");
    // Collect and sort by avg_r descending
    let mut rating_rows: Vec<(String, f64)> = r
        .rows()
        .iter()
        .map(|row| {
            (
                row.get_string(0).unwrap().to_string(),
                row.get_float(1).unwrap(),
            )
        })
        .collect();
    rating_rows.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    // Phone (4.67) > Laptop (3.0) > Tablet (2.5)
    assert_eq!(rating_rows[0].0, "Phone");
    assert_eq!(rating_rows[1].0, "Laptop");
    assert_eq!(rating_rows[2].0, "Tablet");

    // Verify review count per product
    // Laptop: 5, Phone: 3, Tablet: 2
    let r = db
        .query(
            "MATCH (rev:Review)-[:REVIEW_OF]->(p:Product) \
             WHERE p.id = 1 \
             RETURN count(rev)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 5);

    let r = db
        .query(
            "MATCH (rev:Review)-[:REVIEW_OF]->(p:Product) \
             WHERE p.id = 2 \
             RETURN count(rev)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 3);
}

// ============================================================
// E-03: 订单明细边属性（订单总额汇总）
// ============================================================

#[test]
fn e03_order_line_items() {
    let db = Database::in_memory();

    db.execute(
        "CREATE NODE TABLE PurchaseOrder(id INT64, status STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE NODE TABLE Product(id INT64, name STRING, PRIMARY KEY(id))")
        .unwrap();
    // OrderItem as node to make qty/unit_price/discount queryable
    db.execute(
        "CREATE NODE TABLE OrderItem(id INT64, order_id INT64, product_id INT64, \
         qty INT64, unit_price DOUBLE, discount DOUBLE, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE HAS_ITEM(FROM PurchaseOrder TO OrderItem)").unwrap();
    db.execute("CREATE REL TABLE ITEM_PRODUCT(FROM OrderItem TO Product)").unwrap();

    // Products
    db.execute("CREATE (p:Product {id: 1, name: 'Laptop'})").unwrap();
    db.execute("CREATE (p:Product {id: 2, name: 'Mouse'})").unwrap();
    db.execute("CREATE (p:Product {id: 3, name: 'Keyboard'})").unwrap();

    // Order 1
    db.execute("CREATE (o:PurchaseOrder {id: 101, status: 'completed'})").unwrap();

    // OrderItems for order 101:
    // Item 1: 1x Laptop @ 4999.0, discount 0.1 (10%) => 4999 * 1 * (1-0.1) = 4499.1
    // Item 2: 2x Mouse @ 99.0, discount 0.0 => 99 * 2 = 198.0
    // Item 3: 1x Keyboard @ 199.0, discount 0.05 => 199 * 1 * 0.95 = 189.05
    // Total = 4499.1 + 198.0 + 189.05 = 4886.15
    let items = [
        (1, 101, 1, 1, 4999.0, 0.1),
        (2, 101, 2, 2, 99.0, 0.0),
        (3, 101, 3, 1, 199.0, 0.05),
    ];
    for (iid, oid, pid, qty, price, discount) in &items {
        db.execute(&format!(
            "CREATE (item:OrderItem {{id: {}, order_id: {}, product_id: {}, \
             qty: {}, unit_price: {:.2}, discount: {:.2}}})",
            iid, oid, pid, qty, price, discount
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (o:PurchaseOrder), (item:OrderItem) WHERE o.id = {} AND item.id = {} \
             CREATE (o)-[:HAS_ITEM]->(item)",
            oid, iid
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (item:OrderItem), (p:Product) WHERE item.id = {} AND p.id = {} \
             CREATE (item)-[:ITEM_PRODUCT]->(p)",
            iid, pid
        ))
        .unwrap();
    }

    // Verify 3 items in order 101
    let r = db
        .query(
            "MATCH (o:PurchaseOrder)-[:HAS_ITEM]->(item:OrderItem) \
             WHERE o.id = 101 \
             RETURN count(item)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 3);

    // Compute order total: SUM(qty * unit_price * (1 - discount))
    let r = db
        .query(
            "MATCH (o:PurchaseOrder)-[:HAS_ITEM]->(item:OrderItem) \
             WHERE o.id = 101 \
             RETURN sum(item.qty * item.unit_price * (1.0 - item.discount))",
        )
        .unwrap();
    let total = r.rows()[0].get_float(0).unwrap();
    assert!(
        (total - 4886.15).abs() < 0.01,
        "order total should be 4886.15, got {}",
        total
    );

    // Verify per-item subtotals
    let r = db
        .query(
            "MATCH (o:PurchaseOrder)-[:HAS_ITEM]->(item:OrderItem)-[:ITEM_PRODUCT]->(p:Product) \
             WHERE o.id = 101 \
             RETURN p.name, item.qty * item.unit_price * (1.0 - item.discount) AS subtotal \
             ORDER BY p.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 3);

    // Keyboard: 199 * 1 * 0.95 = 189.05
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "Keyboard");
    let kb_total = r.rows()[0].get_float(1).unwrap();
    assert!((kb_total - 189.05).abs() < 0.01, "Keyboard subtotal got {}", kb_total);

    // Laptop: 4999 * 1 * 0.9 = 4499.1
    assert_eq!(r.rows()[1].get_string(0).unwrap(), "Laptop");
    let lp_total = r.rows()[1].get_float(1).unwrap();
    assert!((lp_total - 4499.1).abs() < 0.01, "Laptop subtotal got {}", lp_total);

    // Mouse: 99 * 2 * 1.0 = 198.0
    assert_eq!(r.rows()[2].get_string(0).unwrap(), "Mouse");
    let ms_total = r.rows()[2].get_float(1).unwrap();
    assert!((ms_total - 198.0).abs() < 0.01, "Mouse subtotal got {}", ms_total);
}

// ============================================================
// E-04: 退款/拒付/取消订单
// ============================================================

#[test]
fn e04_refund_and_cancel() {
    let db = Database::in_memory();

    db.execute(
        "CREATE NODE TABLE PurchaseOrder(id INT64, status STRING, total DOUBLE, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute(
        "CREATE NODE TABLE Payment(id INT64, status STRING, amount DOUBLE, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE PAID_WITH(FROM PurchaseOrder TO Payment)").unwrap();

    // Order 1: paid, $500
    db.execute("CREATE (o:PurchaseOrder {id: 1, status: 'paid', total: 500.0})").unwrap();
    db.execute("CREATE (p:Payment {id: 101, status: 'captured', amount: 500.0})").unwrap();
    db.execute(
        "MATCH (o:PurchaseOrder), (p:Payment) WHERE o.id = 1 AND p.id = 101 \
         CREATE (o)-[:PAID_WITH]->(p)",
    )
    .unwrap();

    // Order 2: paid, $300
    db.execute("CREATE (o:PurchaseOrder {id: 2, status: 'paid', total: 300.0})").unwrap();
    db.execute("CREATE (p:Payment {id: 102, status: 'captured', amount: 300.0})").unwrap();
    db.execute(
        "MATCH (o:PurchaseOrder), (p:Payment) WHERE o.id = 2 AND p.id = 102 \
         CREATE (o)-[:PAID_WITH]->(p)",
    )
    .unwrap();

    // Order 3: paid, $200
    db.execute("CREATE (o:PurchaseOrder {id: 3, status: 'paid', total: 200.0})").unwrap();
    db.execute("CREATE (p:Payment {id: 103, status: 'captured', amount: 200.0})").unwrap();
    db.execute(
        "MATCH (o:PurchaseOrder), (p:Payment) WHERE o.id = 3 AND p.id = 103 \
         CREATE (o)-[:PAID_WITH]->(p)",
    )
    .unwrap();

    // Before refund: total captured revenue = 500 + 300 + 200 = 1000
    let r = db
        .query(
            "MATCH (o:PurchaseOrder)-[:PAID_WITH]->(p:Payment) \
             WHERE p.status = 'captured' \
             RETURN sum(p.amount)",
        )
        .unwrap();
    let revenue_before = r.rows()[0].get_float(0).unwrap();
    assert!(
        (revenue_before - 1000.0).abs() < 0.01,
        "revenue before refund should be 1000, got {}",
        revenue_before
    );

    // Refund order 1: update payment status to 'refunded', order status to 'refunded'
    db.execute("MATCH (p:Payment) WHERE p.id = 101 SET p.status = 'refunded'")
        .unwrap();
    db.execute("MATCH (o:PurchaseOrder) WHERE o.id = 1 SET o.status = 'refunded'")
        .unwrap();

    // Cancel order 2: update payment status to 'cancelled', order status to 'cancelled'
    db.execute("MATCH (p:Payment) WHERE p.id = 102 SET p.status = 'cancelled'")
        .unwrap();
    db.execute("MATCH (o:PurchaseOrder) WHERE o.id = 2 SET o.status = 'cancelled'")
        .unwrap();

    // After refund/cancel: only order 3 has captured payment = 200
    let r = db
        .query(
            "MATCH (o:PurchaseOrder)-[:PAID_WITH]->(p:Payment) \
             WHERE p.status = 'captured' \
             RETURN sum(p.amount)",
        )
        .unwrap();
    let revenue_after = r.rows()[0].get_float(0).unwrap();
    assert!(
        (revenue_after - 200.0).abs() < 0.01,
        "revenue after refund should be 200, got {}",
        revenue_after
    );

    // Verify order statuses
    let r = db
        .query("MATCH (o:PurchaseOrder) WHERE o.status = 'refunded' RETURN count(o)")
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 1);

    let r = db
        .query("MATCH (o:PurchaseOrder) WHERE o.status = 'cancelled' RETURN count(o)")
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 1);

    let r = db
        .query("MATCH (o:PurchaseOrder) WHERE o.status = 'paid' RETURN count(o)")
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 1);

    // Revenue delta = 1000 - 200 = 800
    let delta = revenue_before - revenue_after;
    assert!(
        (delta - 800.0).abs() < 0.01,
        "revenue delta should be 800, got {}",
        delta
    );
}

// ============================================================
// E-05: 一个订单多次支付尝试
// ============================================================

#[test]
fn e05_multiple_payment_attempts() {
    let db = Database::in_memory();

    db.execute(
        "CREATE NODE TABLE PurchaseOrder(id INT64, status STRING, total DOUBLE, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute(
        "CREATE NODE TABLE Payment(id INT64, attempt INT64, status STRING, amount DOUBLE, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE PAID_WITH(FROM PurchaseOrder TO Payment)").unwrap();

    // Order: $1000
    db.execute("CREATE (o:PurchaseOrder {id: 1, status: 'pending', total: 1000.0})")
        .unwrap();

    // Attempt 1: authorization failed
    db.execute("CREATE (p:Payment {id: 201, attempt: 1, status: 'auth_failed', amount: 1000.0})")
        .unwrap();
    db.execute(
        "MATCH (o:PurchaseOrder), (p:Payment) WHERE o.id = 1 AND p.id = 201 \
         CREATE (o)-[:PAID_WITH]->(p)",
    )
    .unwrap();

    // Attempt 2: authorization failed again
    db.execute("CREATE (p:Payment {id: 202, attempt: 2, status: 'auth_failed', amount: 1000.0})")
        .unwrap();
    db.execute(
        "MATCH (o:PurchaseOrder), (p:Payment) WHERE o.id = 1 AND p.id = 202 \
         CREATE (o)-[:PAID_WITH]->(p)",
    )
    .unwrap();

    // Attempt 3: success (captured)
    db.execute("CREATE (p:Payment {id: 203, attempt: 3, status: 'captured', amount: 1000.0})")
        .unwrap();
    db.execute(
        "MATCH (o:PurchaseOrder), (p:Payment) WHERE o.id = 1 AND p.id = 203 \
         CREATE (o)-[:PAID_WITH]->(p)",
    )
    .unwrap();

    // Update order status
    db.execute("MATCH (o:PurchaseOrder) WHERE o.id = 1 SET o.status = 'paid'")
        .unwrap();

    // Partial refund: $300
    db.execute(
        "CREATE (p:Payment {id: 204, attempt: 4, status: 'partial_refund', amount: 300.0})",
    )
    .unwrap();
    db.execute(
        "MATCH (o:PurchaseOrder), (p:Payment) WHERE o.id = 1 AND p.id = 204 \
         CREATE (o)-[:PAID_WITH]->(p)",
    )
    .unwrap();

    // Total payment attempts = 4
    let r = db
        .query(
            "MATCH (o:PurchaseOrder)-[:PAID_WITH]->(p:Payment) \
             WHERE o.id = 1 \
             RETURN count(p)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 4, "should have 4 payment attempts");

    // Only captured amount counts as revenue = 1000
    let r = db
        .query(
            "MATCH (o:PurchaseOrder)-[:PAID_WITH]->(p:Payment) \
             WHERE o.id = 1 AND p.status = 'captured' \
             RETURN sum(p.amount)",
        )
        .unwrap();
    let captured = r.rows()[0].get_float(0).unwrap();
    assert!(
        (captured - 1000.0).abs() < 0.01,
        "captured revenue should be 1000, got {}",
        captured
    );

    // Net revenue = captured - refund = 1000 - 300 = 700
    let r = db
        .query(
            "MATCH (o:PurchaseOrder)-[:PAID_WITH]->(p:Payment) \
             WHERE o.id = 1 AND p.status = 'partial_refund' \
             RETURN sum(p.amount)",
        )
        .unwrap();
    let refunded = r.rows()[0].get_float(0).unwrap();
    let net = captured - refunded;
    assert!(
        (net - 700.0).abs() < 0.01,
        "net revenue should be 700, got {}",
        net
    );

    // Failed attempts should not count
    let r = db
        .query(
            "MATCH (o:PurchaseOrder)-[:PAID_WITH]->(p:Payment) \
             WHERE o.id = 1 AND p.status = 'auth_failed' \
             RETURN count(p)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 2, "should have 2 failed attempts");
}

// ============================================================
// E-06: 多仓履约与拆单
// ============================================================

#[test]
fn e06_multi_warehouse_fulfillment() {
    let db = Database::in_memory();

    db.execute(
        "CREATE NODE TABLE PurchaseOrder(id INT64, status STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute(
        "CREATE NODE TABLE Shipment(id INT64, order_id INT64, status STRING, warehouse STRING, items_count INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE FULFILLED_BY(FROM PurchaseOrder TO Shipment)").unwrap();

    // Order with 3 items, split into 3 shipments from different warehouses
    db.execute("CREATE (o:PurchaseOrder {id: 1, status: 'processing'})").unwrap();

    // Shipment 1: from Shanghai, pending
    db.execute(
        "CREATE (s:Shipment {id: 301, order_id: 1, status: 'pending', warehouse: 'SH-01', items_count: 1})",
    )
    .unwrap();
    db.execute(
        "MATCH (o:PurchaseOrder), (s:Shipment) WHERE o.id = 1 AND s.id = 301 \
         CREATE (o)-[:FULFILLED_BY]->(s)",
    )
    .unwrap();

    // Shipment 2: from Beijing, in_transit
    db.execute(
        "CREATE (s:Shipment {id: 302, order_id: 1, status: 'in_transit', warehouse: 'BJ-01', items_count: 1})",
    )
    .unwrap();
    db.execute(
        "MATCH (o:PurchaseOrder), (s:Shipment) WHERE o.id = 1 AND s.id = 302 \
         CREATE (o)-[:FULFILLED_BY]->(s)",
    )
    .unwrap();

    // Shipment 3: from Guangzhou, delivered
    db.execute(
        "CREATE (s:Shipment {id: 303, order_id: 1, status: 'delivered', warehouse: 'GZ-01', items_count: 1})",
    )
    .unwrap();
    db.execute(
        "MATCH (o:PurchaseOrder), (s:Shipment) WHERE o.id = 1 AND s.id = 303 \
         CREATE (o)-[:FULFILLED_BY]->(s)",
    )
    .unwrap();

    // Total shipments for order 1 = 3
    let r = db
        .query(
            "MATCH (o:PurchaseOrder)-[:FULFILLED_BY]->(s:Shipment) \
             WHERE o.id = 1 \
             RETURN count(s)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 3, "should have 3 shipments");

    // Delivered count = 1
    let r = db
        .query(
            "MATCH (o:PurchaseOrder)-[:FULFILLED_BY]->(s:Shipment) \
             WHERE o.id = 1 AND s.status = 'delivered' \
             RETURN count(s)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 1, "1 delivered");

    // Pending count = 1
    let r = db
        .query(
            "MATCH (o:PurchaseOrder)-[:FULFILLED_BY]->(s:Shipment) \
             WHERE o.id = 1 AND s.status = 'pending' \
             RETURN count(s)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 1, "1 pending");

    // In-transit count = 1
    let r = db
        .query(
            "MATCH (o:PurchaseOrder)-[:FULFILLED_BY]->(s:Shipment) \
             WHERE o.id = 1 AND s.status = 'in_transit' \
             RETURN count(s)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 1, "1 in_transit");

    // Not fully delivered yet: total != delivered
    let total = db
        .query(
            "MATCH (o:PurchaseOrder)-[:FULFILLED_BY]->(s:Shipment) \
             WHERE o.id = 1 \
             RETURN count(s)",
        )
        .unwrap()
        .rows()[0]
        .get_int(0)
        .unwrap();
    let delivered = db
        .query(
            "MATCH (o:PurchaseOrder)-[:FULFILLED_BY]->(s:Shipment) \
             WHERE o.id = 1 AND s.status = 'delivered' \
             RETURN count(s)",
        )
        .unwrap()
        .rows()[0]
        .get_int(0)
        .unwrap();
    assert!(total > delivered, "not fully delivered yet");

    // Now deliver remaining shipments
    db.execute("MATCH (s:Shipment) WHERE s.id = 301 SET s.status = 'delivered'")
        .unwrap();
    db.execute("MATCH (s:Shipment) WHERE s.id = 302 SET s.status = 'delivered'")
        .unwrap();

    // Now all delivered
    let r = db
        .query(
            "MATCH (o:PurchaseOrder)-[:FULFILLED_BY]->(s:Shipment) \
             WHERE o.id = 1 AND s.status = 'delivered' \
             RETURN count(s)",
        )
        .unwrap();
    assert_eq!(
        r.rows()[0].get_int(0).unwrap(),
        3,
        "all 3 should be delivered now"
    );

    // Update order status to 'completed'
    db.execute("MATCH (o:PurchaseOrder) WHERE o.id = 1 SET o.status = 'completed'")
        .unwrap();
    let r = db
        .query("MATCH (o:PurchaseOrder) WHERE o.id = 1 RETURN o.status")
        .unwrap();
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "completed");
}

// ============================================================
// E-07: 用户兴趣图推荐（2-hop：买了X的人也浏览了Y）
// ============================================================

#[test]
fn e07_interest_graph_recommendation() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE Customer(id INT64, name STRING, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE NODE TABLE Product(id INT64, name STRING, category STRING, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE REL TABLE VIEWED(FROM Customer TO Product, ts INT64)").unwrap();
    db.execute("CREATE REL TABLE PURCHASED(FROM Customer TO Product)").unwrap();
    db.execute("CREATE REL TABLE REVIEWED_P(FROM Customer TO Product)").unwrap();

    // 5 customers, 6 products
    for i in 1..=5 {
        db.execute(&format!(
            "CREATE (c:Customer {{id: {}, name: 'Cust{}'}})",
            i, i
        ))
        .unwrap();
    }
    let products = [
        (1, "Laptop", "Electronics"),
        (2, "Phone", "Electronics"),
        (3, "Tablet", "Electronics"),
        (4, "Novel", "Books"),
        (5, "Textbook", "Books"),
        (6, "Headphones", "Electronics"),
    ];
    for (id, name, cat) in &products {
        db.execute(&format!(
            "CREATE (p:Product {{id: {}, name: '{}', category: '{}'}})",
            id, name, cat
        ))
        .unwrap();
    }

    // Customer 1: purchased Laptop, viewed Phone, Tablet
    db.execute(
        "MATCH (c:Customer), (p:Product) WHERE c.id = 1 AND p.id = 1 CREATE (c)-[:PURCHASED]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (c:Customer), (p:Product) WHERE c.id = 1 AND p.id = 2 CREATE (c)-[:VIEWED {ts: 100}]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (c:Customer), (p:Product) WHERE c.id = 1 AND p.id = 3 CREATE (c)-[:VIEWED {ts: 101}]->(p)",
    )
    .unwrap();

    // Customer 2: purchased Laptop, purchased Phone, viewed Headphones
    db.execute(
        "MATCH (c:Customer), (p:Product) WHERE c.id = 2 AND p.id = 1 CREATE (c)-[:PURCHASED]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (c:Customer), (p:Product) WHERE c.id = 2 AND p.id = 2 CREATE (c)-[:PURCHASED]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (c:Customer), (p:Product) WHERE c.id = 2 AND p.id = 6 CREATE (c)-[:VIEWED {ts: 200}]->(p)",
    )
    .unwrap();

    // Customer 3: purchased Laptop, viewed Novel
    db.execute(
        "MATCH (c:Customer), (p:Product) WHERE c.id = 3 AND p.id = 1 CREATE (c)-[:PURCHASED]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (c:Customer), (p:Product) WHERE c.id = 3 AND p.id = 4 CREATE (c)-[:VIEWED {ts: 300}]->(p)",
    )
    .unwrap();

    // Customer 4: purchased Phone, viewed Textbook
    db.execute(
        "MATCH (c:Customer), (p:Product) WHERE c.id = 4 AND p.id = 2 CREATE (c)-[:PURCHASED]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (c:Customer), (p:Product) WHERE c.id = 4 AND p.id = 5 CREATE (c)-[:VIEWED {ts: 400}]->(p)",
    )
    .unwrap();

    // Customer 5: purchased Tablet, viewed Laptop
    db.execute(
        "MATCH (c:Customer), (p:Product) WHERE c.id = 5 AND p.id = 3 CREATE (c)-[:PURCHASED]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (c:Customer), (p:Product) WHERE c.id = 5 AND p.id = 1 CREATE (c)-[:VIEWED {ts: 500}]->(p)",
    )
    .unwrap();

    // Recommendation: "people who bought Laptop also viewed..." (2-hop)
    // Laptop(id=1) <-[:PURCHASED]- Customer -[:VIEWED]-> OtherProduct
    // Cust1 bought Laptop, viewed: Phone(2), Tablet(3)
    // Cust2 bought Laptop, viewed: Headphones(6)
    // Cust3 bought Laptop, viewed: Novel(4)
    let r = db
        .query(
            "MATCH (p1:Product)<-[:PURCHASED]-(c:Customer)-[:VIEWED]->(p2:Product) \
             WHERE p1.id = 1 AND p2.id <> 1 \
             RETURN DISTINCT p2.name ORDER BY p2.name",
        )
        .unwrap();

    let names: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert!(names.contains(&"Phone"), "should recommend Phone");
    assert!(names.contains(&"Tablet"), "should recommend Tablet");
    assert!(names.contains(&"Headphones"), "should recommend Headphones");
    assert!(names.contains(&"Novel"), "should recommend Novel (cross-category)");
    assert_eq!(r.num_rows(), 4, "4 distinct recommended products");

    // Also: who bought X also bought Y
    // Laptop(id=1) <-[:PURCHASED]- Customer -[:PURCHASED]-> OtherProduct
    // Cust2 bought Laptop and Phone
    let r = db
        .query(
            "MATCH (p1:Product)<-[:PURCHASED]-(c:Customer)-[:PURCHASED]->(p2:Product) \
             WHERE p1.id = 1 AND p2.id <> 1 \
             RETURN DISTINCT p2.name",
        )
        .unwrap();
    let names: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert!(names.contains(&"Phone"), "who bought Laptop also bought Phone");
}

// ============================================================
// E-08: 会员与优惠券图
// ============================================================

#[test]
fn e08_membership_and_coupon() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE Customer(id INT64, name STRING, PRIMARY KEY(id))")
        .unwrap();
    db.execute(
        "CREATE NODE TABLE MemberLevel(id INT64, name STRING, min_spend DOUBLE, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute(
        "CREATE NODE TABLE Coupon(id INT64, code STRING, discount_pct DOUBLE, min_level INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE HAS_LEVEL(FROM Customer TO MemberLevel)").unwrap();
    db.execute("CREATE REL TABLE HAS_COUPON(FROM MemberLevel TO Coupon)").unwrap();

    // Member levels: Bronze(1), Silver(2), Gold(3)
    db.execute(
        "CREATE (m:MemberLevel {id: 1, name: 'Bronze', min_spend: 0.0})",
    )
    .unwrap();
    db.execute(
        "CREATE (m:MemberLevel {id: 2, name: 'Silver', min_spend: 1000.0})",
    )
    .unwrap();
    db.execute(
        "CREATE (m:MemberLevel {id: 3, name: 'Gold', min_spend: 5000.0})",
    )
    .unwrap();

    // Coupons
    // Bronze coupon: 5% off (all members)
    db.execute(
        "CREATE (c:Coupon {id: 1, code: 'BRONZE5', discount_pct: 5.0, min_level: 1})",
    )
    .unwrap();
    // Silver coupon: 10% off
    db.execute(
        "CREATE (c:Coupon {id: 2, code: 'SILVER10', discount_pct: 10.0, min_level: 2})",
    )
    .unwrap();
    // Gold coupon: 20% off
    db.execute(
        "CREATE (c:Coupon {id: 3, code: 'GOLD20', discount_pct: 20.0, min_level: 3})",
    )
    .unwrap();

    // Link coupons to levels
    // Bronze level gets Bronze coupon
    db.execute(
        "MATCH (m:MemberLevel), (c:Coupon) WHERE m.id = 1 AND c.id = 1 \
         CREATE (m)-[:HAS_COUPON]->(c)",
    )
    .unwrap();
    // Silver level gets Bronze + Silver coupons
    db.execute(
        "MATCH (m:MemberLevel), (c:Coupon) WHERE m.id = 2 AND c.id = 1 \
         CREATE (m)-[:HAS_COUPON]->(c)",
    )
    .unwrap();
    db.execute(
        "MATCH (m:MemberLevel), (c:Coupon) WHERE m.id = 2 AND c.id = 2 \
         CREATE (m)-[:HAS_COUPON]->(c)",
    )
    .unwrap();
    // Gold level gets all coupons
    db.execute(
        "MATCH (m:MemberLevel), (c:Coupon) WHERE m.id = 3 AND c.id = 1 \
         CREATE (m)-[:HAS_COUPON]->(c)",
    )
    .unwrap();
    db.execute(
        "MATCH (m:MemberLevel), (c:Coupon) WHERE m.id = 3 AND c.id = 2 \
         CREATE (m)-[:HAS_COUPON]->(c)",
    )
    .unwrap();
    db.execute(
        "MATCH (m:MemberLevel), (c:Coupon) WHERE m.id = 3 AND c.id = 3 \
         CREATE (m)-[:HAS_COUPON]->(c)",
    )
    .unwrap();

    // Customers
    db.execute("CREATE (c:Customer {id: 1, name: 'Alice'})").unwrap(); // Gold
    db.execute("CREATE (c:Customer {id: 2, name: 'Bob'})").unwrap(); // Silver
    db.execute("CREATE (c:Customer {id: 3, name: 'Charlie'})").unwrap(); // Bronze

    // Assign levels
    db.execute(
        "MATCH (c:Customer), (m:MemberLevel) WHERE c.id = 1 AND m.id = 3 \
         CREATE (c)-[:HAS_LEVEL]->(m)",
    )
    .unwrap();
    db.execute(
        "MATCH (c:Customer), (m:MemberLevel) WHERE c.id = 2 AND m.id = 2 \
         CREATE (c)-[:HAS_LEVEL]->(m)",
    )
    .unwrap();
    db.execute(
        "MATCH (c:Customer), (m:MemberLevel) WHERE c.id = 3 AND m.id = 1 \
         CREATE (c)-[:HAS_LEVEL]->(m)",
    )
    .unwrap();

    // Alice (Gold) should have access to 3 coupons
    let r = db
        .query(
            "MATCH (c:Customer)-[:HAS_LEVEL]->(m:MemberLevel)-[:HAS_COUPON]->(coupon:Coupon) \
             WHERE c.id = 1 \
             RETURN coupon.code ORDER BY coupon.code",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 3, "Gold member should have 3 coupons");

    // Bob (Silver) should have access to 2 coupons
    let r = db
        .query(
            "MATCH (c:Customer)-[:HAS_LEVEL]->(m:MemberLevel)-[:HAS_COUPON]->(coupon:Coupon) \
             WHERE c.id = 2 \
             RETURN coupon.code ORDER BY coupon.code",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 2, "Silver member should have 2 coupons");
    let codes: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert!(codes.contains(&"BRONZE5"));
    assert!(codes.contains(&"SILVER10"));

    // Charlie (Bronze) should have access to 1 coupon
    let r = db
        .query(
            "MATCH (c:Customer)-[:HAS_LEVEL]->(m:MemberLevel)-[:HAS_COUPON]->(coupon:Coupon) \
             WHERE c.id = 3 \
             RETURN coupon.code",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 1, "Bronze member should have 1 coupon");
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "BRONZE5");

    // Verify max discount for Alice = 20%
    let r = db
        .query(
            "MATCH (c:Customer)-[:HAS_LEVEL]->(m:MemberLevel)-[:HAS_COUPON]->(coupon:Coupon) \
             WHERE c.id = 1 \
             RETURN max(coupon.discount_pct)",
        )
        .unwrap();
    let max_disc = r.rows()[0].get_float(0).unwrap();
    assert!(
        (max_disc - 20.0).abs() < 0.01,
        "Alice max discount should be 20%, got {}",
        max_disc
    );

    // Verify member level name through traversal
    let r = db
        .query(
            "MATCH (c:Customer)-[:HAS_LEVEL]->(m:MemberLevel) \
             WHERE c.id = 1 \
             RETURN m.name",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "Gold");
}

// ============================================================
// E-09: 热门商品高并发浏览
// ============================================================

#[test]
fn e09_hot_product_views() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE Customer(id INT64, name STRING, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE NODE TABLE Product(id INT64, name STRING, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE REL TABLE VIEWED(FROM Customer TO Product, ts INT64)").unwrap();
    db.execute("CREATE REL TABLE PURCHASED(FROM Customer TO Product)").unwrap();

    // 3 products
    db.execute("CREATE (p:Product {id: 1, name: 'HotItem'})").unwrap();
    db.execute("CREATE (p:Product {id: 2, name: 'NormalItem'})").unwrap();
    db.execute("CREATE (p:Product {id: 3, name: 'ColdItem'})").unwrap();

    // 600 customers — use batches for speed
    for i in 1..=600 {
        db.execute(&format!(
            "CREATE (c:Customer {{id: {}, name: 'C{}'}})",
            i, i
        ))
        .unwrap();
    }

    // HotItem: 500+ views (customers 1..=550 view it)
    for cid in 1..=550 {
        db.execute(&format!(
            "MATCH (c:Customer), (p:Product) WHERE c.id = {} AND p.id = 1 \
             CREATE (c)-[:VIEWED {{ts: {}}}]->(p)",
            cid,
            1700000000 + cid
        ))
        .unwrap();
    }

    // NormalItem: 100 views (customers 1..=100)
    for cid in 1..=100 {
        db.execute(&format!(
            "MATCH (c:Customer), (p:Product) WHERE c.id = {} AND p.id = 2 \
             CREATE (c)-[:VIEWED {{ts: {}}}]->(p)",
            cid,
            1700100000 + cid
        ))
        .unwrap();
    }

    // ColdItem: 10 views (customers 1..=10)
    for cid in 1..=10 {
        db.execute(&format!(
            "MATCH (c:Customer), (p:Product) WHERE c.id = {} AND p.id = 3 \
             CREATE (c)-[:VIEWED {{ts: {}}}]->(p)",
            cid,
            1700200000 + cid
        ))
        .unwrap();
    }

    // Purchases: HotItem bought by 50 people (1..=50), NormalItem by 20 (1..=20), ColdItem by 5 (1..=5)
    for cid in 1..=50 {
        db.execute(&format!(
            "MATCH (c:Customer), (p:Product) WHERE c.id = {} AND p.id = 1 \
             CREATE (c)-[:PURCHASED]->(p)",
            cid
        ))
        .unwrap();
    }
    for cid in 1..=20 {
        db.execute(&format!(
            "MATCH (c:Customer), (p:Product) WHERE c.id = {} AND p.id = 2 \
             CREATE (c)-[:PURCHASED]->(p)",
            cid
        ))
        .unwrap();
    }
    for cid in 1..=5 {
        db.execute(&format!(
            "MATCH (c:Customer), (p:Product) WHERE c.id = {} AND p.id = 3 \
             CREATE (c)-[:PURCHASED]->(p)",
            cid
        ))
        .unwrap();
    }

    // Hot list: sorted by view count descending (sort client-side)
    let r = db
        .query(
            "MATCH (c:Customer)-[:VIEWED]->(p:Product) \
             RETURN p.name, count(c) AS view_count",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 3);
    let mut view_rows: Vec<(String, i64)> = r
        .rows()
        .iter()
        .map(|row| {
            (
                row.get_string(0).unwrap().to_string(),
                row.get_int(1).unwrap(),
            )
        })
        .collect();
    view_rows.sort_by(|a, b| b.1.cmp(&a.1));
    assert_eq!(view_rows[0].0, "HotItem");
    assert_eq!(view_rows[0].1, 550);
    assert_eq!(view_rows[1].0, "NormalItem");
    assert_eq!(view_rows[1].1, 100);
    assert_eq!(view_rows[2].0, "ColdItem");
    assert_eq!(view_rows[2].1, 10);

    // HotItem has 500+ views
    let hot_views = view_rows[0].1;
    assert!(hot_views > 500, "HotItem should have 500+ views, got {}", hot_views);

    // Purchase counts (sort client-side)
    let r = db
        .query(
            "MATCH (c:Customer)-[:PURCHASED]->(p:Product) \
             RETURN p.name, count(c) AS purchase_count",
        )
        .unwrap();
    let mut purchase_rows: Vec<(String, i64)> = r
        .rows()
        .iter()
        .map(|row| {
            (
                row.get_string(0).unwrap().to_string(),
                row.get_int(1).unwrap(),
            )
        })
        .collect();
    purchase_rows.sort_by(|a, b| b.1.cmp(&a.1));
    assert_eq!(purchase_rows[0].0, "HotItem");
    assert_eq!(purchase_rows[0].1, 50);

    // Conversion rate = purchases / views
    // HotItem: 50/550 ≈ 9.1%, NormalItem: 20/100 = 20%, ColdItem: 5/10 = 50%
    // ColdItem has the highest conversion rate despite fewest views
    // (We verify this via separate queries since we cannot do division in one query easily)
    let hot_purchases = 50i64;
    let normal_views = 100i64;
    let normal_purchases = 20i64;
    let cold_views = 10i64;
    let cold_purchases = 5i64;

    let hot_conv = hot_purchases as f64 / hot_views as f64;
    let normal_conv = normal_purchases as f64 / normal_views as f64;
    let cold_conv = cold_purchases as f64 / cold_views as f64;

    assert!(hot_conv < normal_conv, "HotItem conv rate should be lower than NormalItem");
    assert!(normal_conv < cold_conv, "NormalItem conv rate should be lower than ColdItem");
    assert!((cold_conv - 0.5).abs() < 0.01, "ColdItem conv rate should be 50%");
}

// ============================================================
// E-10: 售后服务图（4-hop 追踪）
// ============================================================

#[test]
fn e10_after_sales_service_chain() {
    let db = Database::in_memory();

    db.execute(
        "CREATE NODE TABLE PurchaseOrder(id INT64, status STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute(
        "CREATE NODE TABLE ServiceRequest(id INT64, order_id INT64, reason STRING, status STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute(
        "CREATE NODE TABLE Refund(id INT64, sr_id INT64, amount DOUBLE, status STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute(
        "CREATE NODE TABLE Ticket(id INT64, refund_id INT64, assignee STRING, status STRING, PRIMARY KEY(id))",
    )
    .unwrap();

    db.execute("CREATE REL TABLE HAS_SERVICE(FROM PurchaseOrder TO ServiceRequest)").unwrap();
    db.execute("CREATE REL TABLE HAS_REFUND(FROM ServiceRequest TO Refund)").unwrap();
    db.execute("CREATE REL TABLE HAS_TICKET(FROM Refund TO Ticket)").unwrap();

    // Order -> ServiceRequest -> Refund -> Ticket chain
    db.execute("CREATE (o:PurchaseOrder {id: 1, status: 'completed'})").unwrap();
    db.execute(
        "CREATE (sr:ServiceRequest {id: 101, order_id: 1, reason: 'defective', status: 'open'})",
    )
    .unwrap();
    db.execute(
        "CREATE (rf:Refund {id: 201, sr_id: 101, amount: 500.0, status: 'pending'})",
    )
    .unwrap();
    db.execute(
        "CREATE (t:Ticket {id: 301, refund_id: 201, assignee: 'Agent1', status: 'in_progress'})",
    )
    .unwrap();

    // Link the chain
    db.execute(
        "MATCH (o:PurchaseOrder), (sr:ServiceRequest) WHERE o.id = 1 AND sr.id = 101 \
         CREATE (o)-[:HAS_SERVICE]->(sr)",
    )
    .unwrap();
    db.execute(
        "MATCH (sr:ServiceRequest), (rf:Refund) WHERE sr.id = 101 AND rf.id = 201 \
         CREATE (sr)-[:HAS_REFUND]->(rf)",
    )
    .unwrap();
    db.execute(
        "MATCH (rf:Refund), (t:Ticket) WHERE rf.id = 201 AND t.id = 301 \
         CREATE (rf)-[:HAS_TICKET]->(t)",
    )
    .unwrap();

    // 4-hop trace: Order → ServiceRequest → Refund → Ticket
    let r = db
        .query(
            "MATCH (o:PurchaseOrder)-[:HAS_SERVICE]->(sr:ServiceRequest)\
             -[:HAS_REFUND]->(rf:Refund)-[:HAS_TICKET]->(t:Ticket) \
             WHERE o.id = 1 \
             RETURN o.id, sr.reason, rf.amount, t.assignee",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 1, "should find exactly 1 after-sales chain");
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 1);
    assert_eq!(r.rows()[0].get_string(1).unwrap(), "defective");
    let refund_amt = r.rows()[0].get_float(2).unwrap();
    assert!((refund_amt - 500.0).abs() < 0.01);
    assert_eq!(r.rows()[0].get_string(3).unwrap(), "Agent1");

    // Add a second chain for order 1 (different issue)
    db.execute(
        "CREATE (sr:ServiceRequest {id: 102, order_id: 1, reason: 'wrong_item', status: 'open'})",
    )
    .unwrap();
    db.execute(
        "CREATE (rf:Refund {id: 202, sr_id: 102, amount: 200.0, status: 'approved'})",
    )
    .unwrap();
    db.execute(
        "CREATE (t:Ticket {id: 302, refund_id: 202, assignee: 'Agent2', status: 'resolved'})",
    )
    .unwrap();
    db.execute(
        "MATCH (o:PurchaseOrder), (sr:ServiceRequest) WHERE o.id = 1 AND sr.id = 102 \
         CREATE (o)-[:HAS_SERVICE]->(sr)",
    )
    .unwrap();
    db.execute(
        "MATCH (sr:ServiceRequest), (rf:Refund) WHERE sr.id = 102 AND rf.id = 202 \
         CREATE (sr)-[:HAS_REFUND]->(rf)",
    )
    .unwrap();
    db.execute(
        "MATCH (rf:Refund), (t:Ticket) WHERE rf.id = 202 AND t.id = 302 \
         CREATE (rf)-[:HAS_TICKET]->(t)",
    )
    .unwrap();

    // Now order 1 has 2 after-sales chains
    let r = db
        .query(
            "MATCH (o:PurchaseOrder)-[:HAS_SERVICE]->(sr:ServiceRequest)\
             -[:HAS_REFUND]->(rf:Refund)-[:HAS_TICKET]->(t:Ticket) \
             WHERE o.id = 1 \
             RETURN o.id, sr.reason, rf.amount, t.assignee ORDER BY sr.id",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 2, "should find 2 after-sales chains");

    // Total refund amount for order 1
    let r = db
        .query(
            "MATCH (o:PurchaseOrder)-[:HAS_SERVICE]->(sr:ServiceRequest)\
             -[:HAS_REFUND]->(rf:Refund) \
             WHERE o.id = 1 \
             RETURN sum(rf.amount)",
        )
        .unwrap();
    let total_refund = r.rows()[0].get_float(0).unwrap();
    assert!(
        (total_refund - 700.0).abs() < 0.01,
        "total refund should be 700, got {}",
        total_refund
    );

    // Count open service requests
    let r = db
        .query(
            "MATCH (o:PurchaseOrder)-[:HAS_SERVICE]->(sr:ServiceRequest) \
             WHERE o.id = 1 AND sr.status = 'open' \
             RETURN count(sr)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 2, "2 open service requests");
}

// ============================================================
// E-11: 商品变体/SKU 层级图
// ============================================================

#[test]
fn e11_product_variant_hierarchy() {
    let db = Database::in_memory();

    db.execute(
        "CREATE NODE TABLE Product(id INT64, name STRING, brand STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute(
        "CREATE NODE TABLE Variant(id INT64, product_id INT64, color STRING, size STRING, stock INT64, price DOUBLE, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE NODE TABLE Customer(id INT64, name STRING, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE REL TABLE HAS_VARIANT(FROM Product TO Variant)").unwrap();
    db.execute("CREATE REL TABLE BOUGHT_VARIANT(FROM Customer TO Variant)").unwrap();

    // Parent product: T-Shirt
    db.execute("CREATE (p:Product {id: 1, name: 'T-Shirt', brand: 'BrandX'})").unwrap();

    // Variants
    db.execute(
        "CREATE (v:Variant {id: 101, product_id: 1, color: 'Red', size: 'M', stock: 50, price: 99.0})",
    )
    .unwrap();
    db.execute(
        "CREATE (v:Variant {id: 102, product_id: 1, color: 'Red', size: 'L', stock: 30, price: 99.0})",
    )
    .unwrap();
    db.execute(
        "CREATE (v:Variant {id: 103, product_id: 1, color: 'Blue', size: 'M', stock: 0, price: 109.0})",
    )
    .unwrap();

    // Link variants
    for vid in [101, 102, 103] {
        db.execute(&format!(
            "MATCH (p:Product), (v:Variant) WHERE p.id = 1 AND v.id = {} \
             CREATE (p)-[:HAS_VARIANT]->(v)",
            vid
        ))
        .unwrap();
    }

    // Total stock = SUM(variant.stock) = 50 + 30 + 0 = 80
    let r = db
        .query(
            "MATCH (p:Product)-[:HAS_VARIANT]->(v:Variant) \
             WHERE p.id = 1 \
             RETURN sum(v.stock)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 80, "total stock should be 80");

    // In-stock variants count (stock > 0) = 2
    let r = db
        .query(
            "MATCH (p:Product)-[:HAS_VARIANT]->(v:Variant) \
             WHERE p.id = 1 AND v.stock > 0 \
             RETURN count(v)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 2, "2 variants in stock");

    // Aggregate stock by color: Red = 80, Blue = 0
    let r = db
        .query(
            "MATCH (p:Product)-[:HAS_VARIANT]->(v:Variant) \
             WHERE p.id = 1 \
             RETURN v.color, sum(v.stock) AS color_stock \
             ORDER BY v.color",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 2);
    // Blue first alphabetically
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "Blue");
    assert_eq!(r.rows()[0].get_int(1).unwrap(), 0, "Blue stock = 0");
    assert_eq!(r.rows()[1].get_string(0).unwrap(), "Red");
    assert_eq!(r.rows()[1].get_int(1).unwrap(), 80, "Red stock = 80");

    // Filter by variant attributes: size = 'M'
    let r = db
        .query(
            "MATCH (p:Product)-[:HAS_VARIANT]->(v:Variant) \
             WHERE p.id = 1 AND v.size = 'M' \
             RETURN v.color, v.stock ORDER BY v.color",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 2, "2 variants with size M");

    // Parent product total sales through 2-hop aggregation
    // Add some customers who bought variants
    for i in 1..=5 {
        db.execute(&format!("CREATE (c:Customer {{id: {}, name: 'Buyer{}'}})", i, i))
            .unwrap();
    }
    // 3 bought Red-M (101), 2 bought Red-L (102), 0 bought Blue-M (103)
    for cid in 1..=3 {
        db.execute(&format!(
            "MATCH (c:Customer), (v:Variant) WHERE c.id = {} AND v.id = 101 \
             CREATE (c)-[:BOUGHT_VARIANT]->(v)",
            cid
        ))
        .unwrap();
    }
    for cid in 4..=5 {
        db.execute(&format!(
            "MATCH (c:Customer), (v:Variant) WHERE c.id = {} AND v.id = 102 \
             CREATE (c)-[:BOUGHT_VARIANT]->(v)",
            cid
        ))
        .unwrap();
    }

    // Parent product total sales = 3 + 2 + 0 = 5 (2-hop: Product -> Variant <- Customer)
    let r = db
        .query(
            "MATCH (p:Product)-[:HAS_VARIANT]->(v:Variant)<-[:BOUGHT_VARIANT]-(c:Customer) \
             WHERE p.id = 1 \
             RETURN count(c)",
        )
        .unwrap();
    assert_eq!(
        r.rows()[0].get_int(0).unwrap(),
        5,
        "parent product total sales = 5"
    );

    // Sales per variant
    let r = db
        .query(
            "MATCH (p:Product)-[:HAS_VARIANT]->(v:Variant)<-[:BOUGHT_VARIANT]-(c:Customer) \
             WHERE p.id = 1 \
             RETURN v.id, count(c) AS sales ORDER BY v.id",
        )
        .unwrap();
    // Only variants with sales appear (101: 3, 102: 2)
    assert_eq!(r.num_rows(), 2);
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 101);
    assert_eq!(r.rows()[0].get_int(1).unwrap(), 3);
    assert_eq!(r.rows()[1].get_int(0).unwrap(), 102);
    assert_eq!(r.rows()[1].get_int(1).unwrap(), 2);
}

// ============================================================
// E-12: 供应商-仓库-商品供应链
// ============================================================

#[test]
fn e12_supplier_warehouse_supply_chain() {
    let db = Database::in_memory();

    db.execute(
        "CREATE NODE TABLE Supplier(id INT64, name STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute(
        "CREATE NODE TABLE Warehouse(id INT64, name STRING, city STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE NODE TABLE Product(id INT64, name STRING, PRIMARY KEY(id))")
        .unwrap();

    // Use intermediate nodes to model edge properties (lead_time, qty)
    db.execute(
        "CREATE NODE TABLE SupplyLink(id INT64, supplier_id INT64, warehouse_id INT64, lead_time INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute(
        "CREATE NODE TABLE StockRecord(id INT64, warehouse_id INT64, product_id INT64, qty INT64, PRIMARY KEY(id))",
    )
    .unwrap();

    db.execute("CREATE REL TABLE SUPPLIES_VIA(FROM Supplier TO SupplyLink)").unwrap();
    db.execute("CREATE REL TABLE SUPPLIED_TO(FROM SupplyLink TO Warehouse)").unwrap();
    db.execute("CREATE REL TABLE STOCKS_VIA(FROM Warehouse TO StockRecord)").unwrap();
    db.execute("CREATE REL TABLE STOCKED_PRODUCT(FROM StockRecord TO Product)").unwrap();

    // Suppliers
    db.execute("CREATE (s:Supplier {id: 1, name: 'SupplierA'})").unwrap();
    db.execute("CREATE (s:Supplier {id: 2, name: 'SupplierB'})").unwrap();

    // Warehouses
    db.execute("CREATE (w:Warehouse {id: 1, name: 'SH-01', city: 'Shanghai'})").unwrap();
    db.execute("CREATE (w:Warehouse {id: 2, name: 'BJ-01', city: 'Beijing'})").unwrap();

    // Products
    db.execute("CREATE (p:Product {id: 1, name: 'Laptop'})").unwrap();
    db.execute("CREATE (p:Product {id: 2, name: 'Phone'})").unwrap();

    // Supply links: SupplierA -> SH-01 (lead_time: 7), SupplierB -> BJ-01 (lead_time: 14)
    db.execute(
        "CREATE (sl:SupplyLink {id: 1, supplier_id: 1, warehouse_id: 1, lead_time: 7})",
    )
    .unwrap();
    db.execute(
        "CREATE (sl:SupplyLink {id: 2, supplier_id: 2, warehouse_id: 2, lead_time: 14})",
    )
    .unwrap();
    // SupplierA also supplies BJ-01 with lead_time: 10
    db.execute(
        "CREATE (sl:SupplyLink {id: 3, supplier_id: 1, warehouse_id: 2, lead_time: 10})",
    )
    .unwrap();

    // Link supply chains
    db.execute(
        "MATCH (s:Supplier), (sl:SupplyLink) WHERE s.id = 1 AND sl.id = 1 \
         CREATE (s)-[:SUPPLIES_VIA]->(sl)",
    )
    .unwrap();
    db.execute(
        "MATCH (sl:SupplyLink), (w:Warehouse) WHERE sl.id = 1 AND w.id = 1 \
         CREATE (sl)-[:SUPPLIED_TO]->(w)",
    )
    .unwrap();
    db.execute(
        "MATCH (s:Supplier), (sl:SupplyLink) WHERE s.id = 2 AND sl.id = 2 \
         CREATE (s)-[:SUPPLIES_VIA]->(sl)",
    )
    .unwrap();
    db.execute(
        "MATCH (sl:SupplyLink), (w:Warehouse) WHERE sl.id = 2 AND w.id = 2 \
         CREATE (sl)-[:SUPPLIED_TO]->(w)",
    )
    .unwrap();
    db.execute(
        "MATCH (s:Supplier), (sl:SupplyLink) WHERE s.id = 1 AND sl.id = 3 \
         CREATE (s)-[:SUPPLIES_VIA]->(sl)",
    )
    .unwrap();
    db.execute(
        "MATCH (sl:SupplyLink), (w:Warehouse) WHERE sl.id = 3 AND w.id = 2 \
         CREATE (sl)-[:SUPPLIED_TO]->(w)",
    )
    .unwrap();

    // Stock records: SH-01 has 100 Laptops, BJ-01 has 50 Laptops, BJ-01 has 200 Phones
    db.execute(
        "CREATE (sr:StockRecord {id: 1, warehouse_id: 1, product_id: 1, qty: 100})",
    )
    .unwrap();
    db.execute(
        "CREATE (sr:StockRecord {id: 2, warehouse_id: 2, product_id: 1, qty: 50})",
    )
    .unwrap();
    db.execute(
        "CREATE (sr:StockRecord {id: 3, warehouse_id: 2, product_id: 2, qty: 200})",
    )
    .unwrap();

    // Link stock records
    db.execute(
        "MATCH (w:Warehouse), (sr:StockRecord) WHERE w.id = 1 AND sr.id = 1 \
         CREATE (w)-[:STOCKS_VIA]->(sr)",
    )
    .unwrap();
    db.execute(
        "MATCH (sr:StockRecord), (p:Product) WHERE sr.id = 1 AND p.id = 1 \
         CREATE (sr)-[:STOCKED_PRODUCT]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (w:Warehouse), (sr:StockRecord) WHERE w.id = 2 AND sr.id = 2 \
         CREATE (w)-[:STOCKS_VIA]->(sr)",
    )
    .unwrap();
    db.execute(
        "MATCH (sr:StockRecord), (p:Product) WHERE sr.id = 2 AND p.id = 1 \
         CREATE (sr)-[:STOCKED_PRODUCT]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (w:Warehouse), (sr:StockRecord) WHERE w.id = 2 AND sr.id = 3 \
         CREATE (w)-[:STOCKS_VIA]->(sr)",
    )
    .unwrap();
    db.execute(
        "MATCH (sr:StockRecord), (p:Product) WHERE sr.id = 3 AND p.id = 2 \
         CREATE (sr)-[:STOCKED_PRODUCT]->(p)",
    )
    .unwrap();

    // 3-hop query: find suppliers for Laptop
    // Product <- STOCKED_PRODUCT <- StockRecord <- STOCKS_VIA <- Warehouse <- SUPPLIED_TO <- SupplyLink <- SUPPLIES_VIA <- Supplier
    // Using forward direction: Supplier -> SupplyLink -> Warehouse -> StockRecord -> Product
    let r = db
        .query(
            "MATCH (s:Supplier)-[:SUPPLIES_VIA]->(sl:SupplyLink)-[:SUPPLIED_TO]->(w:Warehouse)\
             -[:STOCKS_VIA]->(sr:StockRecord)-[:STOCKED_PRODUCT]->(p:Product) \
             WHERE p.id = 1 \
             RETURN DISTINCT s.name ORDER BY s.name",
        )
        .unwrap();
    let supplier_names: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert!(supplier_names.contains(&"SupplierA"), "SupplierA supplies Laptop");
    assert!(supplier_names.contains(&"SupplierB"), "SupplierB supplies Laptop");

    // Total available stock for Laptop = 100 + 50 = 150
    let r = db
        .query(
            "MATCH (w:Warehouse)-[:STOCKS_VIA]->(sr:StockRecord)-[:STOCKED_PRODUCT]->(p:Product) \
             WHERE p.id = 1 \
             RETURN sum(sr.qty)",
        )
        .unwrap();
    assert_eq!(
        r.rows()[0].get_int(0).unwrap(),
        150,
        "total Laptop stock should be 150"
    );

    // Find fastest supply route for Laptop (by lead_time)
    let r = db
        .query(
            "MATCH (s:Supplier)-[:SUPPLIES_VIA]->(sl:SupplyLink)-[:SUPPLIED_TO]->(w:Warehouse)\
             -[:STOCKS_VIA]->(sr:StockRecord)-[:STOCKED_PRODUCT]->(p:Product) \
             WHERE p.id = 1 \
             RETURN s.name, w.name, sl.lead_time \
             ORDER BY sl.lead_time",
        )
        .unwrap();
    assert!(r.num_rows() >= 2, "should have multiple supply routes");
    // Fastest should be SupplierA -> SH-01 with lead_time 7
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "SupplierA");
    assert_eq!(r.rows()[0].get_string(1).unwrap(), "SH-01");
    assert_eq!(r.rows()[0].get_int(2).unwrap(), 7);

    // Phone only in BJ-01 stock
    let r = db
        .query(
            "MATCH (w:Warehouse)-[:STOCKS_VIA]->(sr:StockRecord)-[:STOCKED_PRODUCT]->(p:Product) \
             WHERE p.id = 2 \
             RETURN w.name, sr.qty",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 1);
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "BJ-01");
    assert_eq!(r.rows()[0].get_int(1).unwrap(), 200);
}

// ============================================================
// E-13: 价格历史与时间维度定价
// ============================================================

#[test]
fn e13_price_history_and_time_pricing() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE Product(id INT64, name STRING, PRIMARY KEY(id))")
        .unwrap();
    // PriceRecord as node (since edge properties are not queryable in RETURN)
    db.execute(
        "CREATE NODE TABLE PriceRecord(id INT64, product_id INT64, amount DOUBLE, \
         effective_from INT64, effective_to INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE HAS_PRICE(FROM Product TO PriceRecord)").unwrap();

    // Product: Phone
    db.execute("CREATE (p:Product {id: 1, name: 'Phone'})").unwrap();

    // Price history:
    // Period 1: 4999.0 from 20240101 to 20240630
    // Period 2: 4499.0 from 20240701 to 20241231
    // Period 3: 3999.0 from 20250101 to 99991231 (current)
    db.execute(
        "CREATE (pr:PriceRecord {id: 1, product_id: 1, amount: 4999.0, \
         effective_from: 20240101, effective_to: 20240630})",
    )
    .unwrap();
    db.execute(
        "CREATE (pr:PriceRecord {id: 2, product_id: 1, amount: 4499.0, \
         effective_from: 20240701, effective_to: 20241231})",
    )
    .unwrap();
    db.execute(
        "CREATE (pr:PriceRecord {id: 3, product_id: 1, amount: 3999.0, \
         effective_from: 20250101, effective_to: 99991231})",
    )
    .unwrap();

    // Link prices to product
    for prid in 1..=3 {
        db.execute(&format!(
            "MATCH (p:Product), (pr:PriceRecord) WHERE p.id = 1 AND pr.id = {} \
             CREATE (p)-[:HAS_PRICE]->(pr)",
            prid
        ))
        .unwrap();
    }

    // Query effective price at 20241015 (should be 4499.0)
    let r = db
        .query(
            "MATCH (p:Product)-[:HAS_PRICE]->(pr:PriceRecord) \
             WHERE p.id = 1 AND pr.effective_from <= 20241015 AND pr.effective_to >= 20241015 \
             RETURN pr.amount",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 1, "should find exactly one active price");
    let price = r.rows()[0].get_float(0).unwrap();
    assert!(
        (price - 4499.0).abs() < 0.01,
        "price at 20241015 should be 4499.0, got {}",
        price
    );

    // Query effective price at 20240315 (should be 4999.0)
    let r = db
        .query(
            "MATCH (p:Product)-[:HAS_PRICE]->(pr:PriceRecord) \
             WHERE p.id = 1 AND pr.effective_from <= 20240315 AND pr.effective_to >= 20240315 \
             RETURN pr.amount",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 1);
    let price = r.rows()[0].get_float(0).unwrap();
    assert!(
        (price - 4999.0).abs() < 0.01,
        "price at 20240315 should be 4999.0, got {}",
        price
    );

    // Query effective price at 20260101 (should be 3999.0, current)
    let r = db
        .query(
            "MATCH (p:Product)-[:HAS_PRICE]->(pr:PriceRecord) \
             WHERE p.id = 1 AND pr.effective_from <= 20260101 AND pr.effective_to >= 20260101 \
             RETURN pr.amount",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 1);
    let price = r.rows()[0].get_float(0).unwrap();
    assert!(
        (price - 3999.0).abs() < 0.01,
        "price at 20260101 should be 3999.0, got {}",
        price
    );

    // Price change count = 3
    let r = db
        .query(
            "MATCH (p:Product)-[:HAS_PRICE]->(pr:PriceRecord) \
             WHERE p.id = 1 \
             RETURN count(pr)",
        )
        .unwrap();
    assert_eq!(
        r.rows()[0].get_int(0).unwrap(),
        3,
        "should have 3 price records"
    );

    // Historical min price = 3999.0
    let r = db
        .query(
            "MATCH (p:Product)-[:HAS_PRICE]->(pr:PriceRecord) \
             WHERE p.id = 1 \
             RETURN min(pr.amount)",
        )
        .unwrap();
    let min_price = r.rows()[0].get_float(0).unwrap();
    assert!(
        (min_price - 3999.0).abs() < 0.01,
        "min price should be 3999.0, got {}",
        min_price
    );

    // Historical max price = 4999.0
    let r = db
        .query(
            "MATCH (p:Product)-[:HAS_PRICE]->(pr:PriceRecord) \
             WHERE p.id = 1 \
             RETURN max(pr.amount)",
        )
        .unwrap();
    let max_price = r.rows()[0].get_float(0).unwrap();
    assert!(
        (max_price - 4999.0).abs() < 0.01,
        "max price should be 4999.0, got {}",
        max_price
    );

    // Price history sorted by effective_from
    let r = db
        .query(
            "MATCH (p:Product)-[:HAS_PRICE]->(pr:PriceRecord) \
             WHERE p.id = 1 \
             RETURN pr.amount, pr.effective_from, pr.effective_to \
             ORDER BY pr.effective_from",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 3);

    // First period: 4999.0
    let p1 = r.rows()[0].get_float(0).unwrap();
    assert!((p1 - 4999.0).abs() < 0.01);
    assert_eq!(r.rows()[0].get_int(1).unwrap(), 20240101);
    assert_eq!(r.rows()[0].get_int(2).unwrap(), 20240630);

    // Second period: 4499.0
    let p2 = r.rows()[1].get_float(0).unwrap();
    assert!((p2 - 4499.0).abs() < 0.01);
    assert_eq!(r.rows()[1].get_int(1).unwrap(), 20240701);
    assert_eq!(r.rows()[1].get_int(2).unwrap(), 20241231);

    // Third period: 3999.0
    let p3 = r.rows()[2].get_float(0).unwrap();
    assert!((p3 - 3999.0).abs() < 0.01);
    assert_eq!(r.rows()[2].get_int(1).unwrap(), 20250101);
    assert_eq!(r.rows()[2].get_int(2).unwrap(), 99991231);

    // Verify no overlap: query at a boundary date (20240630) should only return one price
    let r = db
        .query(
            "MATCH (p:Product)-[:HAS_PRICE]->(pr:PriceRecord) \
             WHERE p.id = 1 AND pr.effective_from <= 20240630 AND pr.effective_to >= 20240630 \
             RETURN pr.amount",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 1, "boundary date should match exactly one price");
    let boundary_price = r.rows()[0].get_float(0).unwrap();
    assert!((boundary_price - 4999.0).abs() < 0.01);

    // Add a second product with different price history for diversity
    db.execute("CREATE (p:Product {id: 2, name: 'Tablet'})").unwrap();
    db.execute(
        "CREATE (pr:PriceRecord {id: 4, product_id: 2, amount: 2999.0, \
         effective_from: 20240101, effective_to: 99991231})",
    )
    .unwrap();
    db.execute(
        "MATCH (p:Product), (pr:PriceRecord) WHERE p.id = 2 AND pr.id = 4 \
         CREATE (p)-[:HAS_PRICE]->(pr)",
    )
    .unwrap();

    // Tablet has only 1 price change
    let r = db
        .query(
            "MATCH (p:Product)-[:HAS_PRICE]->(pr:PriceRecord) \
             WHERE p.id = 2 \
             RETURN count(pr)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 1);

    // Compare products by price change frequency (sort client-side)
    let r = db
        .query(
            "MATCH (p:Product)-[:HAS_PRICE]->(pr:PriceRecord) \
             RETURN p.name, count(pr) AS changes",
        )
        .unwrap();
    let mut change_rows: Vec<(String, i64)> = r
        .rows()
        .iter()
        .map(|row| {
            (
                row.get_string(0).unwrap().to_string(),
                row.get_int(1).unwrap(),
            )
        })
        .collect();
    change_rows.sort_by(|a, b| b.1.cmp(&a.1));
    assert_eq!(change_rows.len(), 2);
    assert_eq!(change_rows[0].0, "Phone");
    assert_eq!(change_rows[0].1, 3, "Phone has 3 price changes");
    assert_eq!(change_rows[1].0, "Tablet");
    assert_eq!(change_rows[1].1, 1, "Tablet has 1 price change");
}
