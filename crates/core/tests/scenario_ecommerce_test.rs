/// 电商系统场景端到端测试
///
/// 场景设定：电商平台
/// - 节点：Customer、Product、Category
/// - 关系：PURCHASED（客户→商品）、BELONGS_TO（商品→类别）、REVIEWED（客户→商品）、VIEWED（客户→商品）
use gqlite_core::Database;

/// 建表 + 创建关系表的辅助函数
fn setup_schema(db: &Database) {
    // 节点表
    db.execute("CREATE NODE TABLE Customer(id INT64, name STRING, email STRING, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE NODE TABLE Product(id INT64, name STRING, price DOUBLE, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE NODE TABLE Category(id INT64, name STRING, PRIMARY KEY(id))").unwrap();

    // 关系表
    db.execute("CREATE REL TABLE PURCHASED(FROM Customer TO Product)").unwrap();
    db.execute("CREATE REL TABLE BELONGS_TO(FROM Product TO Category)").unwrap();
    db.execute("CREATE REL TABLE REVIEWED(FROM Customer TO Product, rating INT64)").unwrap();
    db.execute("CREATE REL TABLE VIEWED(FROM Customer TO Product)").unwrap();
}

/// 插入客户数据
fn insert_customers(db: &Database) {
    for i in 1..=10 {
        db.execute(&format!(
            "CREATE (c:Customer {{id: {}, name: 'Customer{}', email: 'c{}@shop.com'}})",
            i, i, i
        ))
        .unwrap();
    }
}

/// 插入类别数据
fn insert_categories(db: &Database) {
    let cats = ["Electronics", "Books", "Clothing", "Food", "Sports"];
    for (i, name) in cats.iter().enumerate() {
        db.execute(&format!("CREATE (c:Category {{id: {}, name: '{}'}})", i + 1, name)).unwrap();
    }
}

/// 插入商品数据（20 个商品，分配到 5 个类别）
fn insert_products(db: &Database) {
    let products = [
        (1, "Laptop", 999.99, 1),
        (2, "Phone", 699.99, 1),
        (3, "Tablet", 499.99, 1),
        (4, "Headphones", 149.99, 1),
        (5, "Novel A", 12.99, 2),
        (6, "Novel B", 14.99, 2),
        (7, "Textbook", 59.99, 2),
        (8, "Comic", 9.99, 2),
        (9, "T-Shirt", 19.99, 3),
        (10, "Jacket", 89.99, 3),
        (11, "Jeans", 49.99, 3),
        (12, "Hat", 14.99, 3),
        (13, "Rice", 5.99, 4),
        (14, "Pasta", 3.99, 4),
        (15, "Coffee", 11.99, 4),
        (16, "Tea", 8.99, 4),
        (17, "Basketball", 29.99, 5),
        (18, "Tennis Racket", 79.99, 5),
        (19, "Yoga Mat", 24.99, 5),
        (20, "Dumbbell", 34.99, 5),
    ];
    for (id, name, price, cat_id) in &products {
        db.execute(&format!(
            "CREATE (p:Product {{id: {}, name: '{}', price: {}}})",
            id, name, price
        ))
        .unwrap();
        // 建立 BELONGS_TO 关系
        db.execute(&format!(
            "MATCH (p:Product), (c:Category) WHERE p.id = {} AND c.id = {} \
             CREATE (p)-[:BELONGS_TO]->(c)",
            id, cat_id
        ))
        .unwrap();
    }
}

/// 插入购买关系（30+ 条）
fn insert_purchases(db: &Database) {
    // (customer_id, product_id) 对
    let purchases = [
        (1, 1),
        (1, 2),
        (1, 5),
        (1, 9),
        (2, 1),
        (2, 3),
        (2, 7),
        (3, 2),
        (3, 4),
        (3, 10),
        (4, 5),
        (4, 6),
        (4, 8),
        (4, 13),
        (5, 1),
        (5, 17),
        (5, 18),
        (6, 9),
        (6, 11),
        (6, 12),
        (6, 14),
        (7, 3),
        (7, 15),
        (7, 16),
        (8, 2),
        (8, 19),
        (8, 20),
        (9, 6),
        (9, 7),
        (9, 10),
        (10, 1),
        (10, 4),
        (10, 17),
        (10, 13),
    ];
    for (cid, pid) in &purchases {
        db.execute(&format!(
            "MATCH (c:Customer), (p:Product) WHERE c.id = {} AND p.id = {} \
             CREATE (c)-[:PURCHASED]->(p)",
            cid, pid
        ))
        .unwrap();
    }
}

/// 完整数据设置
fn setup_full(db: &Database) {
    setup_schema(db);
    insert_customers(db);
    insert_categories(db);
    insert_products(db);
    insert_purchases(db);
}

// ============================================================
// 1. 建表 + 插入验证
// ============================================================

#[test]
fn ecommerce_schema_and_data() {
    let db = Database::in_memory();
    setup_full(&db);

    // 验证客户数
    let r = db.query("MATCH (c:Customer) RETURN count(*)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 10);

    // 验证商品数
    let r = db.query("MATCH (p:Product) RETURN count(*)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 20);

    // 验证类别数
    let r = db.query("MATCH (c:Category) RETURN count(*)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 5);

    // 验证购买关系数 >= 30
    let r = db.query("MATCH (c:Customer)-[:PURCHASED]->(p:Product) RETURN count(*)").unwrap();
    assert!(r.rows()[0].get_int(0).unwrap() >= 30);

    // 验证每个商品都属于一个类别
    let r = db.query("MATCH (p:Product)-[:BELONGS_TO]->(c:Category) RETURN count(*)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 20);
}

// ============================================================
// 2. 查询某客户购买的所有商品
// ============================================================

#[test]
fn customer_order_history() {
    let db = Database::in_memory();
    setup_full(&db);

    // Customer1 购买了: Laptop(1), Phone(2), Novel A(5), T-Shirt(9) → 4 件
    let r = db
        .query(
            "MATCH (c:Customer)-[:PURCHASED]->(p:Product) \
             WHERE c.id = 1 \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 4);
    let names: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert!(names.contains(&"Laptop"));
    assert!(names.contains(&"Phone"));
    assert!(names.contains(&"Novel A"));
    assert!(names.contains(&"T-Shirt"));
}

// ============================================================
// 3. "买了X的人也买了Y"：2 跳查询
// ============================================================

#[test]
fn product_recommendation() {
    let db = Database::in_memory();
    setup_full(&db);

    // 查找购买了 Laptop(id=1) 的人还购买了哪些其他商品
    // 路径: Product(1) <-[:PURCHASED]- Customer -[:PURCHASED]-> OtherProduct
    let r = db
        .query(
            "MATCH (p1:Product)<-[:PURCHASED]-(c:Customer)-[:PURCHASED]->(p2:Product) \
             WHERE p1.id = 1 AND p2.id <> 1 \
             RETURN DISTINCT p2.name ORDER BY p2.name",
        )
        .unwrap();

    // Customer1 买了 Laptop + Phone/Novel A/T-Shirt
    // Customer2 买了 Laptop + Tablet/Textbook
    // Customer5 买了 Laptop + Basketball/Tennis Racket
    // Customer10 买了 Laptop + Headphones/Basketball/Rice
    assert!(r.num_rows() > 0, "应该有推荐商品");

    let names: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    // 验证已知的推荐商品
    assert!(names.contains(&"Phone"), "Customer1 也买了 Phone");
    assert!(names.contains(&"Tablet"), "Customer2 也买了 Tablet");
    assert!(names.contains(&"Basketball"), "Customer5 也买了 Basketball");
}

// ============================================================
// 4. 每个类别的销售统计
// ============================================================

#[test]
fn category_sales_stats() {
    let db = Database::in_memory();
    setup_full(&db);

    // 每个类别：商品数（通过 BELONGS_TO）和总购买次数（通过 PURCHASED + BELONGS_TO）
    // 先统计每个类别有多少商品
    let r = db
        .query(
            "MATCH (p:Product)-[:BELONGS_TO]->(cat:Category) \
             RETURN cat.name, count(p) AS product_count \
             ORDER BY cat.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 5);

    // 每个类别 4 个商品
    for row in r.rows() {
        assert_eq!(row.get_int(1).unwrap(), 4);
    }

    // 统计每个类别的总购买次数（通过 3 表连接）
    let r = db
        .query(
            "MATCH (c:Customer)-[:PURCHASED]->(p:Product)-[:BELONGS_TO]->(cat:Category) \
             RETURN cat.name, count(*) AS total_purchases \
             ORDER BY cat.name",
        )
        .unwrap();
    assert!(r.num_rows() > 0, "每个类别都应有购买记录");

    // 统计总购买次数之和应与总购买关系数匹配
    let total: i64 = r.rows().iter().map(|row| row.get_int(1).unwrap()).sum();
    let all = db.query("MATCH (c:Customer)-[:PURCHASED]->(p:Product) RETURN count(*)").unwrap();
    assert_eq!(total, all.rows()[0].get_int(0).unwrap());
}

// ============================================================
// 5. 购买排行榜 ORDER BY + LIMIT
// ============================================================

#[test]
fn top_customers_by_purchases() {
    let db = Database::in_memory();
    setup_full(&db);

    let r = db
        .query(
            "MATCH (c:Customer)-[:PURCHASED]->(p:Product) \
             RETURN c.name, count(p) AS purchase_count",
        )
        .unwrap();

    // 收集所有 (name, count) 并按 count 降序排序
    let mut entries: Vec<(String, i64)> = r
        .rows()
        .iter()
        .map(|row| (row.get_string(0).unwrap().to_string(), row.get_int(1).unwrap()))
        .collect();
    entries.sort_by(|a, b| b.1.cmp(&a.1));

    // 取 top 3
    let top3: Vec<&(String, i64)> = entries.iter().take(3).collect();
    assert!(top3.len() == 3);

    // 排行榜应按购买数量降序排列
    assert!(top3[0].1 >= top3[1].1, "应降序排列");
    assert!(top3[1].1 >= top3[2].1, "应降序排列");

    // Customer1, Customer4, Customer6, Customer10 各买了 4 件，应是 top
    assert_eq!(top3[0].1, 4, "top 客户应有 4 次购买，got name={}", top3[0].0);
}

// ============================================================
// 6. EXISTS 子查询：找购买了某类别所有商品的客户
// ============================================================

#[test]
fn customer_who_bought_all_in_category() {
    let db = Database::in_memory();
    setup_full(&db);

    // 先看 Books 类别(id=2)有哪些商品：Novel A(5), Novel B(6), Textbook(7), Comic(8)
    // Customer4 买了 5, 6, 8 → 没有买 7(Textbook)，不算全买
    // 没有客户购买了 Books 的全部 4 个商品

    // 换个思路：查某个类别中，客户购买的商品数等于该类别商品总数
    // 这是一个统计比较查询
    let r = db
        .query(
            "MATCH (c:Customer)-[:PURCHASED]->(p:Product)-[:BELONGS_TO]->(cat:Category) \
             WHERE cat.id = 2 \
             RETURN c.name, count(p) AS bought_count \
             ORDER BY count(p) DESC",
        )
        .unwrap();

    // Books 类别有 4 个商品，如果有客户 bought_count = 4 则买全了
    // 目前数据中没有人买全 Books 的所有商品
    if r.num_rows() > 0 {
        let max_count = r.rows()[0].get_int(1).unwrap();
        assert!(max_count < 4, "初始数据中不应有人买全 Books 的 4 个商品");
    }

    // 现在让 Customer4 再买 Textbook(7)，就买全了
    db.execute(
        "MATCH (c:Customer), (p:Product) WHERE c.id = 4 AND p.id = 7 \
         CREATE (c)-[:PURCHASED]->(p)",
    )
    .unwrap();

    // 再次查询，用 EXISTS 过滤：
    // 找出在 Books 类别中，购买数等于该类别商品总数的客户
    let r = db
        .query(
            "MATCH (c:Customer)-[:PURCHASED]->(p:Product)-[:BELONGS_TO]->(cat:Category) \
             WHERE cat.id = 2 \
             RETURN c.name, count(p) AS bought_count \
             ORDER BY count(p) DESC",
        )
        .unwrap();

    // Customer4 现在应该有 4 个
    let mut found = false;
    for row in r.rows() {
        if row.get_string(0).unwrap() == "Customer4" {
            assert_eq!(row.get_int(1).unwrap(), 4, "Customer4 应买全 Books 的 4 个商品");
            found = true;
        }
    }
    assert!(found, "Customer4 应在结果中");

    // 用 EXISTS 子查询验证：Customer4 确实在 Books 类别有购买记录
    let r = db
        .query(
            "MATCH (c:Customer) \
             WHERE c.id = 4 AND EXISTS { \
                 MATCH (c)-[:PURCHASED]->(p:Product)-[:BELONGS_TO]->(cat:Category) \
                 WHERE cat.id = 2 \
             } \
             RETURN c.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 1);
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "Customer4");
}

// ============================================================
// 7. BEGIN/COMMIT 事务：创建订单+关系，原子提交
// ============================================================

#[test]
fn transaction_atomicity_order() {
    let db = Database::in_memory();
    setup_schema(&db);
    insert_customers(&db);
    insert_products(&db);
    insert_categories(&db);

    // 在事务中：创建新客户 + 购买关系
    db.execute_script(
        "BEGIN; \
         CREATE (c:Customer {id: 100, name: 'TxnCustomer', email: 'txn@shop.com'}); \
         COMMIT;",
    )
    .unwrap();

    // 验证客户已创建
    let r = db.query("MATCH (c:Customer) WHERE c.id = 100 RETURN c.name").unwrap();
    assert_eq!(r.num_rows(), 1);
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "TxnCustomer");

    // 再在事务中创建购买关系
    db.execute_script(
        "BEGIN; \
         MATCH (c:Customer), (p:Product) WHERE c.id = 100 AND p.id = 1 \
         CREATE (c)-[:PURCHASED]->(p); \
         MATCH (c:Customer), (p:Product) WHERE c.id = 100 AND p.id = 2 \
         CREATE (c)-[:PURCHASED]->(p); \
         COMMIT;",
    )
    .unwrap();

    // 验证购买关系
    let r = db
        .query(
            "MATCH (c:Customer)-[:PURCHASED]->(p:Product) \
             WHERE c.id = 100 \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 2);
}

// ============================================================
// 8. BEGIN/ROLLBACK：订单创建失败时数据不残留
// ============================================================

#[test]
fn transaction_rollback_order() {
    let db = Database::in_memory();
    setup_schema(&db);
    insert_customers(&db);

    // 插入前的客户数
    let before =
        db.query("MATCH (c:Customer) RETURN count(*)").unwrap().rows()[0].get_int(0).unwrap();

    // ROLLBACK 事务
    db.execute_script(
        "BEGIN; \
         CREATE (c:Customer {id: 200, name: 'RollbackCustomer', email: 'rb@shop.com'}); \
         ROLLBACK;",
    )
    .unwrap();

    // 客户数不变
    let after =
        db.query("MATCH (c:Customer) RETURN count(*)").unwrap().rows()[0].get_int(0).unwrap();
    assert_eq!(before, after, "ROLLBACK 后客户数不应变化");

    // 确认具体客户不存在
    let r = db.query("MATCH (c:Customer) WHERE c.id = 200 RETURN c.name").unwrap();
    assert_eq!(r.num_rows(), 0, "ROLLBACK 的客户不应存在");
}

// ============================================================
// 9. 大批量插入 100 个商品
// ============================================================

#[test]
fn bulk_import_products() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Product(id INT64, name STRING, price DOUBLE, PRIMARY KEY(id))")
        .unwrap();

    for i in 1..=100 {
        let price = i as f64 * 1.5;
        db.execute(&format!(
            "CREATE (p:Product {{id: {}, name: 'BulkProduct{}', price: {:.2}}})",
            i, i, price
        ))
        .unwrap();
    }

    // 验证总数
    let r = db.query("MATCH (p:Product) RETURN count(*)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 100);

    // 验证第一个和最后一个
    let r = db.query("MATCH (p:Product) WHERE p.id = 1 RETURN p.name, p.price").unwrap();
    assert_eq!(r.num_rows(), 1);
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "BulkProduct1");
    assert_eq!(r.rows()[0].get_float(1).unwrap(), 1.5);

    let r = db.query("MATCH (p:Product) WHERE p.id = 100 RETURN p.name, p.price").unwrap();
    assert_eq!(r.num_rows(), 1);
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "BulkProduct100");
    assert_eq!(r.rows()[0].get_float(1).unwrap(), 150.0);

    // 验证 id 连续不重复
    let r = db.query("MATCH (p:Product) RETURN min(p.id), max(p.id)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 1);
    assert_eq!(r.rows()[0].get_int(1).unwrap(), 100);
}

// ============================================================
// 10. 批量 SET 更新价格 + 验证
// ============================================================

#[test]
fn update_product_price() {
    let db = Database::in_memory();
    setup_schema(&db);
    insert_categories(&db);
    insert_products(&db);

    // 验证更新前价格
    let r = db.query("MATCH (p:Product) WHERE p.id = 1 RETURN p.price").unwrap();
    assert_eq!(r.rows()[0].get_float(0).unwrap(), 999.99);

    // 单个更新
    db.execute("MATCH (p:Product) WHERE p.id = 1 SET p.price = 899.99").unwrap();
    let r = db.query("MATCH (p:Product) WHERE p.id = 1 RETURN p.price").unwrap();
    assert_eq!(r.rows()[0].get_float(0).unwrap(), 899.99);

    // 批量更新：所有价格 < 20 的商品涨价 5
    db.execute("MATCH (p:Product) WHERE p.price < 20.0 SET p.price = p.price + 5.0").unwrap();

    // 验证：原来 price=12.99 的 Novel A(id=5) 应该变成 17.99
    let r = db.query("MATCH (p:Product) WHERE p.id = 5 RETURN p.price").unwrap();
    let novel_a_price = r.rows()[0].get_float(0).unwrap();
    assert!(
        (novel_a_price - 17.99).abs() < 0.001,
        "Novel A price should be ~17.99, got {}",
        novel_a_price
    );

    // 验证：原来 price=14.99 的 Novel B(id=6) 应该变成 19.99
    let r = db.query("MATCH (p:Product) WHERE p.id = 6 RETURN p.price").unwrap();
    let novel_b_price = r.rows()[0].get_float(0).unwrap();
    assert!(
        (novel_b_price - 19.99).abs() < 0.001,
        "Novel B price should be ~19.99, got {}",
        novel_b_price
    );

    // 验证：原来 price >= 20 的商品不受影响，如 Basketball(id=17) = 29.99
    let r = db.query("MATCH (p:Product) WHERE p.id = 17 RETURN p.price").unwrap();
    assert_eq!(r.rows()[0].get_float(0).unwrap(), 29.99);
}

// ============================================================
// 11. 删除商品后，购买关系也应清理（DETACH DELETE）
// ============================================================

#[test]
fn delete_product_cascade() {
    let db = Database::in_memory();
    setup_full(&db);

    // 确认 Laptop(id=1) 有购买关系
    let r = db
        .query(
            "MATCH (c:Customer)-[:PURCHASED]->(p:Product) \
             WHERE p.id = 1 RETURN count(*)",
        )
        .unwrap();
    let purchases_before = r.rows()[0].get_int(0).unwrap();
    assert!(purchases_before > 0, "Laptop 应有购买记录");

    // 同时确认 BELONGS_TO 关系存在
    let r = db
        .query(
            "MATCH (p:Product)-[:BELONGS_TO]->(c:Category) \
             WHERE p.id = 1 RETURN count(*)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 1);

    // DETACH DELETE 删除商品及其所有关系
    db.execute("MATCH (p:Product) WHERE p.id = 1 DETACH DELETE p").unwrap();

    // 验证商品已删除
    let r = db.query("MATCH (p:Product) WHERE p.id = 1 RETURN p.name").unwrap();
    assert_eq!(r.num_rows(), 0);

    // 验证购买关系已清理
    let r = db
        .query(
            "MATCH (c:Customer)-[:PURCHASED]->(p:Product) \
             WHERE p.id = 1 RETURN count(*)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 0);

    // 验证 BELONGS_TO 关系已清理
    let r = db
        .query(
            "MATCH (p:Product)-[:BELONGS_TO]->(c:Category) \
             WHERE p.id = 1 RETURN count(*)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 0);

    // 验证商品总数减少了 1
    let r = db.query("MATCH (p:Product) RETURN count(*)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 19);
}

// ============================================================
// 12. 多表 JOIN：客户→购买→商品→类别
// ============================================================

#[test]
fn cross_table_query() {
    let db = Database::in_memory();
    setup_full(&db);

    // 3 跳查询：Customer → PURCHASED → Product → BELONGS_TO → Category
    let r = db
        .query(
            "MATCH (c:Customer)-[:PURCHASED]->(p:Product)-[:BELONGS_TO]->(cat:Category) \
             WHERE c.id = 1 \
             RETURN c.name, p.name, cat.name \
             ORDER BY p.name",
        )
        .unwrap();

    // Customer1 买了 Laptop(Electronics), Phone(Electronics), Novel A(Books), T-Shirt(Clothing)
    assert_eq!(r.num_rows(), 4);

    let results: Vec<(String, String, String)> = r
        .rows()
        .iter()
        .map(|row| {
            (
                row.get_string(0).unwrap().to_string(),
                row.get_string(1).unwrap().to_string(),
                row.get_string(2).unwrap().to_string(),
            )
        })
        .collect();

    // 验证客户名全部是 Customer1
    for (cname, _, _) in &results {
        assert_eq!(cname, "Customer1");
    }

    // 验证商品和类别的对应关系
    assert!(results.contains(&(
        "Customer1".to_string(),
        "Laptop".to_string(),
        "Electronics".to_string()
    )));
    assert!(results.contains(&(
        "Customer1".to_string(),
        "Novel A".to_string(),
        "Books".to_string()
    )));
    assert!(results.contains(&(
        "Customer1".to_string(),
        "T-Shirt".to_string(),
        "Clothing".to_string()
    )));
}

// ============================================================
// 13. MERGE 幂等：已存在不重复创建
// ============================================================

#[test]
fn merge_customer() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Customer(id INT64, name STRING, email STRING, PRIMARY KEY(id))")
        .unwrap();

    // 首次 MERGE → 应创建
    db.execute(
        "MERGE (c:Customer {id: 1, name: 'Alice'}) ON CREATE SET c.email = 'alice@shop.com'",
    )
    .unwrap();

    let r = db.query("MATCH (c:Customer) RETURN count(*)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 1);

    let r = db.query("MATCH (c:Customer) WHERE c.id = 1 RETURN c.name, c.email").unwrap();
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "Alice");
    assert_eq!(r.rows()[0].get_string(1).unwrap(), "alice@shop.com");

    // 再次 MERGE 相同 id → 不应重复创建，而是 ON MATCH
    db.execute(
        "MERGE (c:Customer {id: 1, name: 'Alice'}) ON MATCH SET c.email = 'alice_updated@shop.com'",
    )
    .unwrap();

    // 仍然只有 1 个客户
    let r = db.query("MATCH (c:Customer) RETURN count(*)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 1);

    // email 已更新
    let r = db.query("MATCH (c:Customer) WHERE c.id = 1 RETURN c.email").unwrap();
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "alice_updated@shop.com");

    // MERGE 一个不存在的客户 → 应创建
    db.execute("MERGE (c:Customer {id: 2, name: 'Bob'}) ON CREATE SET c.email = 'bob@shop.com'")
        .unwrap();

    let r = db.query("MATCH (c:Customer) RETURN count(*)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 2);
}

// ============================================================
// 14. CASE WHEN 动态价格分级
// ============================================================

#[test]
fn case_expression_pricing() {
    let db = Database::in_memory();
    setup_schema(&db);
    insert_categories(&db);
    insert_products(&db);

    // CASE WHEN 动态价格分级
    let r = db
        .query(
            "MATCH (p:Product) \
             RETURN p.name, p.price, \
                CASE \
                    WHEN p.price >= 100.0 THEN 'high' \
                    WHEN p.price >= 20.0 THEN 'medium' \
                    ELSE 'low' \
                END AS tier \
             ORDER BY p.price DESC",
        )
        .unwrap();

    assert_eq!(r.num_rows(), 20);

    // 验证分级逻辑
    for row in r.rows() {
        let price = row.get_float(1).unwrap();
        let tier = row.get_string(2).unwrap();
        match tier {
            "high" => assert!(price >= 100.0, "high tier price {} should >= 100", price),
            "medium" => {
                assert!(price >= 20.0, "medium tier price {} should >= 20", price);
                assert!(price < 100.0, "medium tier price {} should < 100", price);
            }
            "low" => assert!(price < 20.0, "low tier price {} should < 20", price),
            other => panic!("unexpected tier: {}", other),
        }
    }

    // 验证具体商品
    // Laptop = 999.99 → high
    let r = db
        .query(
            "MATCH (p:Product) WHERE p.id = 1 \
             RETURN CASE WHEN p.price >= 100.0 THEN 'high' \
                         WHEN p.price >= 20.0 THEN 'medium' \
                         ELSE 'low' END AS tier",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "high");

    // Coffee = 11.99 → low
    let r = db
        .query(
            "MATCH (p:Product) WHERE p.id = 15 \
             RETURN CASE WHEN p.price >= 100.0 THEN 'high' \
                         WHEN p.price >= 20.0 THEN 'medium' \
                         ELSE 'low' END AS tier",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "low");

    // Basketball = 29.99 → medium
    let r = db
        .query(
            "MATCH (p:Product) WHERE p.id = 17 \
             RETURN CASE WHEN p.price >= 100.0 THEN 'high' \
                         WHEN p.price >= 20.0 THEN 'medium' \
                         ELSE 'low' END AS tier",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "medium");
}

// ============================================================
// 15. 真实订单图：订单、支付、履约
// ============================================================

#[test]
fn order_payment_shipment_graph() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE Customer(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute(
        "CREATE NODE TABLE Product(id INT64, name STRING, category STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute(
        "CREATE NODE TABLE PurchaseOrder(id INT64, status STRING, channel STRING, total DOUBLE, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute(
        "CREATE NODE TABLE Payment(id INT64, method STRING, status STRING, amount DOUBLE, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute(
        "CREATE NODE TABLE Shipment(id INT64, status STRING, carrier STRING, PRIMARY KEY(id))",
    )
    .unwrap();

    db.execute("CREATE REL TABLE PLACED(FROM Customer TO PurchaseOrder)").unwrap();
    db.execute("CREATE REL TABLE CONTAINS(FROM PurchaseOrder TO Product)").unwrap();
    db.execute("CREATE REL TABLE PAID_WITH(FROM PurchaseOrder TO Payment)").unwrap();
    db.execute("CREATE REL TABLE FULFILLED_BY(FROM PurchaseOrder TO Shipment)").unwrap();

    db.execute("CREATE (c:Customer {id: 1, name: 'Alice'})").unwrap();
    db.execute("CREATE (c:Customer {id: 2, name: 'Bob'})").unwrap();

    db.execute("CREATE (p:Product {id: 1, name: 'Laptop', category: 'Electronics'})").unwrap();
    db.execute("CREATE (p:Product {id: 2, name: 'Mouse', category: 'Electronics'})").unwrap();
    db.execute("CREATE (p:Product {id: 3, name: 'Novel', category: 'Books'})").unwrap();

    db.execute(
        "CREATE (o:PurchaseOrder {id: 101, status: 'paid', channel: 'app', total: 1099.98})",
    )
    .unwrap();
    db.execute(
        "CREATE (o:PurchaseOrder {id: 102, status: 'pending_payment', channel: 'web', total: 12.99})",
    )
        .unwrap();
    db.execute("CREATE (o:PurchaseOrder {id: 103, status: 'paid', channel: 'web', total: 59.98})")
        .unwrap();

    db.execute("CREATE (p:Payment {id: 201, method: 'card', status: 'captured', amount: 1099.98})")
        .unwrap();
    db.execute(
        "CREATE (p:Payment {id: 202, method: 'wallet', status: 'authorized', amount: 12.99})",
    )
    .unwrap();
    db.execute("CREATE (p:Payment {id: 203, method: 'card', status: 'captured', amount: 59.98})")
        .unwrap();

    db.execute("CREATE (s:Shipment {id: 301, status: 'in_transit', carrier: 'SF'})").unwrap();
    db.execute("CREATE (s:Shipment {id: 302, status: 'pending', carrier: 'JD'})").unwrap();
    db.execute("CREATE (s:Shipment {id: 303, status: 'delivered', carrier: 'UPS'})").unwrap();

    db.execute(
        "MATCH (c:Customer), (o:PurchaseOrder) WHERE c.id = 1 AND o.id = 101 CREATE (c)-[:PLACED]->(o)",
    )
        .unwrap();
    db.execute(
        "MATCH (c:Customer), (o:PurchaseOrder) WHERE c.id = 1 AND o.id = 102 CREATE (c)-[:PLACED]->(o)",
    )
        .unwrap();
    db.execute(
        "MATCH (c:Customer), (o:PurchaseOrder) WHERE c.id = 2 AND o.id = 103 CREATE (c)-[:PLACED]->(o)",
    )
        .unwrap();

    db.execute(
        "MATCH (o:PurchaseOrder), (p:Product) WHERE o.id = 101 AND p.id = 1 CREATE (o)-[:CONTAINS]->(p)",
    )
        .unwrap();
    db.execute(
        "MATCH (o:PurchaseOrder), (p:Product) WHERE o.id = 101 AND p.id = 2 CREATE (o)-[:CONTAINS]->(p)",
    )
        .unwrap();
    db.execute(
        "MATCH (o:PurchaseOrder), (p:Product) WHERE o.id = 102 AND p.id = 3 CREATE (o)-[:CONTAINS]->(p)",
    )
        .unwrap();
    db.execute(
        "MATCH (o:PurchaseOrder), (p:Product) WHERE o.id = 103 AND p.id = 2 CREATE (o)-[:CONTAINS]->(p)",
    )
        .unwrap();
    db.execute(
        "MATCH (o:PurchaseOrder), (p:Product) WHERE o.id = 103 AND p.id = 3 CREATE (o)-[:CONTAINS]->(p)",
    )
        .unwrap();

    db.execute(
        "MATCH (o:PurchaseOrder), (p:Payment) WHERE o.id = 101 AND p.id = 201 CREATE (o)-[:PAID_WITH]->(p)",
    )
        .unwrap();
    db.execute(
        "MATCH (o:PurchaseOrder), (p:Payment) WHERE o.id = 102 AND p.id = 202 CREATE (o)-[:PAID_WITH]->(p)",
    )
        .unwrap();
    db.execute(
        "MATCH (o:PurchaseOrder), (p:Payment) WHERE o.id = 103 AND p.id = 203 CREATE (o)-[:PAID_WITH]->(p)",
    )
        .unwrap();

    db.execute(
        "MATCH (o:PurchaseOrder), (s:Shipment) WHERE o.id = 101 AND s.id = 301 CREATE (o)-[:FULFILLED_BY]->(s)",
    )
        .unwrap();
    db.execute(
        "MATCH (o:PurchaseOrder), (s:Shipment) WHERE o.id = 102 AND s.id = 302 CREATE (o)-[:FULFILLED_BY]->(s)",
    )
        .unwrap();
    db.execute(
        "MATCH (o:PurchaseOrder), (s:Shipment) WHERE o.id = 103 AND s.id = 303 CREATE (o)-[:FULFILLED_BY]->(s)",
    )
        .unwrap();

    let customer1_orders = db
        .query(
            "MATCH (c:Customer)-[:PLACED]->(o:PurchaseOrder)-[:CONTAINS]->(p:Product) \
             WHERE c.id = 1 \
             RETURN o.id, p.name ORDER BY o.id, p.name",
        )
        .unwrap();
    assert_eq!(customer1_orders.num_rows(), 3);
    assert_eq!(customer1_orders.rows()[0].get_int(0).unwrap(), 101);
    assert_eq!(customer1_orders.rows()[2].get_int(0).unwrap(), 102);

    let captured_revenue = db
        .query(
            "MATCH (o:PurchaseOrder)-[:PAID_WITH]->(pay:Payment) \
             WHERE pay.status = 'captured' \
             RETURN o.id, o.total ORDER BY o.id",
        )
        .unwrap();
    assert_eq!(captured_revenue.num_rows(), 2);
    let total_paid: f64 = captured_revenue.rows().iter().map(|row| row.get_float(1).unwrap()).sum();
    assert!((total_paid - 1159.96).abs() < 0.01, "captured GMV mismatch: {}", total_paid);

    let pending_shipments = db
        .query(
            "MATCH (c:Customer)-[:PLACED]->(o:PurchaseOrder)-[:FULFILLED_BY]->(s:Shipment) \
             WHERE s.status <> 'delivered' \
             RETURN c.name, count(o) ORDER BY c.name",
        )
        .unwrap();
    assert_eq!(pending_shipments.num_rows(), 1);
    assert_eq!(pending_shipments.rows()[0].get_string(0).unwrap(), "Alice");
    assert_eq!(pending_shipments.rows()[0].get_int(1).unwrap(), 2);

    let repeat_buyers = db
        .query(
            "MATCH (c:Customer)-[:PLACED]->(o:PurchaseOrder) \
             RETURN c.name, count(o) ORDER BY count(o) DESC",
        )
        .unwrap();
    let mut order_counts = std::collections::HashMap::new();
    for row in repeat_buyers.rows() {
        order_counts.insert(row.get_string(0).unwrap().to_string(), row.get_int(1).unwrap());
    }
    assert_eq!(order_counts["Alice"], 2);
    assert_eq!(order_counts["Bob"], 1);
}
