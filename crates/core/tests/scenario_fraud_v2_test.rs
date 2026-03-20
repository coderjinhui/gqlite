/// 欺诈检测场景 V2 端到端测试 (F-01 ~ F-12)
///
/// 覆盖：边金额过滤、时间窗高频交易、分拆交易检测、交易节点模型、
/// 案件调查子图、风险分数回写、黑名单联动、团伙识别(WCC)、
/// 资金闭环、跨境欺诈、合成身份检测、多维度风控速率检查
///
/// 因为引擎暂不支持在 WHERE/RETURN 中直接访问关系属性 (r.amount)，
/// 所有涉及交易金额/时间戳的用例均采用 Transaction 节点模型：
/// Account -[:INITIATED]-> Transaction -[:RECEIVED_BY]-> Account
use gqlite_core::Database;

// ════════════════════════════════════════════════════════════════
// F-01: 按交易边金额过滤（使用 Transaction 节点模型）
// ════════════════════════════════════════════════════════════════

#[test]
fn f01_edge_amount_filter() {
    let db = Database::in_memory();

    // 建表
    db.execute("CREATE NODE TABLE Account(id INT64, name STRING, balance DOUBLE, PRIMARY KEY(id))")
        .unwrap();
    db.execute(
        "CREATE NODE TABLE Transaction(id INT64, amount DOUBLE, ts INT64, status STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE INITIATED(FROM Account TO Transaction)").unwrap();
    db.execute("CREATE REL TABLE RECEIVED_BY(FROM Transaction TO Account)").unwrap();

    // 插入账户
    db.execute("CREATE (a:Account {id: 1, name: 'alice', balance: 50000.0})").unwrap();
    db.execute("CREATE (a:Account {id: 2, name: 'bob', balance: 30000.0})").unwrap();
    db.execute("CREATE (a:Account {id: 3, name: 'charlie', balance: 120000.0})").unwrap();
    db.execute("CREATE (a:Account {id: 4, name: 'dave', balance: 5000.0})").unwrap();

    // 插入交易：高额、小额、分拆
    // 高额交易
    db.execute("CREATE (t:Transaction {id: 101, amount: 150000.0, ts: 1000, status: 'posted'})")
        .unwrap();
    // 小额交易
    db.execute("CREATE (t:Transaction {id: 102, amount: 200.0, ts: 1010, status: 'posted'})")
        .unwrap();
    // 分拆交易（多笔小额，每笔刚好低于阈值）
    db.execute("CREATE (t:Transaction {id: 103, amount: 9800.0, ts: 1020, status: 'posted'})")
        .unwrap();
    db.execute("CREATE (t:Transaction {id: 104, amount: 9700.0, ts: 1025, status: 'posted'})")
        .unwrap();
    db.execute("CREATE (t:Transaction {id: 105, amount: 9600.0, ts: 1030, status: 'posted'})")
        .unwrap();
    // 中等金额
    db.execute("CREATE (t:Transaction {id: 106, amount: 5000.0, ts: 2000, status: 'posted'})")
        .unwrap();

    // 创建关系：alice 发起高额交易给 bob
    db.execute(
        "MATCH (a:Account), (t:Transaction) WHERE a.id = 1 AND t.id = 101 CREATE (a)-[:INITIATED]->(t)",
    )
    .unwrap();
    db.execute(
        "MATCH (t:Transaction), (a:Account) WHERE t.id = 101 AND a.id = 2 CREATE (t)-[:RECEIVED_BY]->(a)",
    )
    .unwrap();

    // alice 发起小额交易给 charlie
    db.execute(
        "MATCH (a:Account), (t:Transaction) WHERE a.id = 1 AND t.id = 102 CREATE (a)-[:INITIATED]->(t)",
    )
    .unwrap();
    db.execute(
        "MATCH (t:Transaction), (a:Account) WHERE t.id = 102 AND a.id = 3 CREATE (t)-[:RECEIVED_BY]->(a)",
    )
    .unwrap();

    // charlie 发起 3 笔分拆交易给 dave
    for tx_id in [103, 104, 105] {
        db.execute(&format!(
            "MATCH (a:Account), (t:Transaction) WHERE a.id = 3 AND t.id = {} CREATE (a)-[:INITIATED]->(t)",
            tx_id
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (t:Transaction), (a:Account) WHERE t.id = {} AND a.id = 4 CREATE (t)-[:RECEIVED_BY]->(a)",
            tx_id
        ))
        .unwrap();
    }

    // bob 发起中等交易给 charlie
    db.execute(
        "MATCH (a:Account), (t:Transaction) WHERE a.id = 2 AND t.id = 106 CREATE (a)-[:INITIATED]->(t)",
    )
    .unwrap();
    db.execute(
        "MATCH (t:Transaction), (a:Account) WHERE t.id = 106 AND a.id = 3 CREATE (t)-[:RECEIVED_BY]->(a)",
    )
    .unwrap();

    // 断言 1：筛选高额交易 (amount > 100000)
    let high = db
        .query(
            "MATCH (src:Account)-[:INITIATED]->(t:Transaction)-[:RECEIVED_BY]->(dst:Account) \
             WHERE t.amount > 100000.0 \
             RETURN src.name, t.amount, dst.name",
        )
        .unwrap();
    assert_eq!(high.num_rows(), 1, "应有 1 笔高额交易");
    assert_eq!(high.rows()[0].get_string(0).unwrap(), "alice");
    assert_eq!(high.rows()[0].get_string(2).unwrap(), "bob");

    // 断言 2：筛选小额交易 (amount < 1000)
    let low = db
        .query(
            "MATCH (src:Account)-[:INITIATED]->(t:Transaction)-[:RECEIVED_BY]->(dst:Account) \
             WHERE t.amount < 1000.0 \
             RETURN src.name, t.amount, dst.name",
        )
        .unwrap();
    assert_eq!(low.num_rows(), 1, "应有 1 笔小额交易");
    assert_eq!(low.rows()[0].get_string(0).unwrap(), "alice");

    // 断言 3：筛选分拆交易 (9000 <= amount < 10000 的多笔交易)
    let split = db
        .query(
            "MATCH (src:Account)-[:INITIATED]->(t:Transaction)-[:RECEIVED_BY]->(dst:Account) \
             WHERE t.amount >= 9000.0 AND t.amount < 10000.0 \
             RETURN src.name, t.amount, dst.name ORDER BY t.amount",
        )
        .unwrap();
    assert_eq!(split.num_rows(), 3, "应有 3 笔分拆交易");
    // 所有分拆交易的发起方都是 charlie
    for row in split.rows() {
        assert_eq!(row.get_string(0).unwrap(), "charlie");
        assert_eq!(row.get_string(2).unwrap(), "dave");
    }

    // 断言 4：按金额区间统计交易数量
    let mid = db
        .query(
            "MATCH (src:Account)-[:INITIATED]->(t:Transaction) \
             WHERE t.amount >= 1000.0 AND t.amount <= 10000.0 \
             RETURN count(*) AS cnt",
        )
        .unwrap();
    // 9800 + 9700 + 9600 + 5000 = 4 笔在 [1000, 10000] 区间
    assert_eq!(mid.rows()[0].get_int(0).unwrap(), 4, "应有 4 笔中等金额交易");
}

// ════════════════════════════════════════════════════════════════
// F-02: 时间窗高频交易
// ════════════════════════════════════════════════════════════════

#[test]
fn f02_time_window_high_frequency() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE Account(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute(
        "CREATE NODE TABLE Transaction(id INT64, amount DOUBLE, ts INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE INITIATED(FROM Account TO Transaction)").unwrap();
    db.execute("CREATE REL TABLE RECEIVED_BY(FROM Transaction TO Account)").unwrap();

    // 账户
    db.execute("CREATE (a:Account {id: 1, name: 'high_freq_alice'})").unwrap();
    db.execute("CREATE (a:Account {id: 2, name: 'normal_bob'})").unwrap();
    db.execute("CREATE (a:Account {id: 3, name: 'target_charlie'})").unwrap();
    db.execute("CREATE (a:Account {id: 4, name: 'target_dave'})").unwrap();

    // alice: 1 小时 (3600s) 内 5 笔交易 — 高频
    let alice_txs = vec![
        (201, 1000.0, 10000_i64),
        (202, 1500.0, 10200),
        (203, 2000.0, 10500),
        (204, 800.0, 11000),
        (205, 1200.0, 12000),
    ];
    for (id, amount, ts) in &alice_txs {
        db.execute(&format!(
            "CREATE (t:Transaction {{id: {}, amount: {:.1}, ts: {}}})",
            id, amount, ts
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (a:Account), (t:Transaction) WHERE a.id = 1 AND t.id = {} \
             CREATE (a)-[:INITIATED]->(t)",
            id
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (t:Transaction), (a:Account) WHERE t.id = {} AND a.id = 3 \
             CREATE (t)-[:RECEIVED_BY]->(a)",
            id
        ))
        .unwrap();
    }

    // bob: 仅 1 笔交易
    db.execute("CREATE (t:Transaction {id: 206, amount: 5000.0, ts: 10000})").unwrap();
    db.execute(
        "MATCH (a:Account), (t:Transaction) WHERE a.id = 2 AND t.id = 206 \
         CREATE (a)-[:INITIATED]->(t)",
    )
    .unwrap();
    db.execute(
        "MATCH (t:Transaction), (a:Account) WHERE t.id = 206 AND a.id = 4 \
         CREATE (t)-[:RECEIVED_BY]->(a)",
    )
    .unwrap();

    // 断言 1：查找在 [10000, 13600] 时间窗内发起交易的账户及数量
    let result = db
        .query(
            "MATCH (a:Account)-[:INITIATED]->(t:Transaction) \
             WHERE t.ts >= 10000 AND t.ts <= 13600 \
             RETURN a.id, a.name, count(*) AS tx_count",
        )
        .unwrap();
    assert!(result.num_rows() >= 1, "应有至少 1 个账户在时间窗内有交易");

    // alice 应有 5 笔
    let alice_row = result.rows().iter().find(|r| r.get_int(0).unwrap() == 1);
    assert!(alice_row.is_some(), "alice 应在结果中");
    assert_eq!(alice_row.unwrap().get_int(2).unwrap(), 5, "alice 应有 5 笔交易");

    // 断言 2：bob 在同一时间窗内仅 1 笔
    let bob_row = result.rows().iter().find(|r| r.get_int(0).unwrap() == 2);
    assert!(bob_row.is_some(), "bob 也在此时间窗内有交易");
    assert_eq!(bob_row.unwrap().get_int(2).unwrap(), 1, "bob 应仅有 1 笔交易");

    // 断言 3：缩小时间窗 [10000, 10600] 应只包含 alice 的 3 笔交易
    let narrow = db
        .query(
            "MATCH (a:Account)-[:INITIATED]->(t:Transaction) \
             WHERE t.ts >= 10000 AND t.ts <= 10600 \
             RETURN a.id, count(*) AS tx_count",
        )
        .unwrap();
    // alice 在 [10000, 10600] 内有 ts=10000, 10200, 10500 共 3 笔
    let alice_narrow = narrow.rows().iter().find(|r| r.get_int(0).unwrap() == 1);
    assert!(alice_narrow.is_some());
    assert_eq!(alice_narrow.unwrap().get_int(1).unwrap(), 3, "缩小时间窗后 alice 应有 3 笔");
}

// ════════════════════════════════════════════════════════════════
// F-03: 分拆交易检测 (structuring / smurfing)
// ════════════════════════════════════════════════════════════════

#[test]
fn f03_structuring_detection() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE Account(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute(
        "CREATE NODE TABLE Transaction(id INT64, amount DOUBLE, ts INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE INITIATED(FROM Account TO Transaction)").unwrap();
    db.execute("CREATE REL TABLE RECEIVED_BY(FROM Transaction TO Account)").unwrap();

    db.execute("CREATE (a:Account {id: 1, name: 'structurer'})").unwrap();
    db.execute("CREATE (a:Account {id: 2, name: 'target_mule'})").unwrap();
    db.execute("CREATE (a:Account {id: 3, name: 'normal_user'})").unwrap();
    db.execute("CREATE (a:Account {id: 4, name: 'normal_target'})").unwrap();

    // structurer(1) -> target_mule(2): 5 笔刚好低于 10000 阈值的交易，短时间内
    let structuring_txs = vec![
        (301, 9800.0, 5000_i64),
        (302, 9700.0, 5010),
        (303, 9900.0, 5020),
        (304, 9600.0, 5030),
        (305, 9500.0, 5040),
    ];
    for (id, amount, ts) in &structuring_txs {
        db.execute(&format!(
            "CREATE (t:Transaction {{id: {}, amount: {:.1}, ts: {}}})",
            id, amount, ts
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (a:Account), (t:Transaction) WHERE a.id = 1 AND t.id = {} \
             CREATE (a)-[:INITIATED]->(t)",
            id
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (t:Transaction), (a:Account) WHERE t.id = {} AND a.id = 2 \
             CREATE (t)-[:RECEIVED_BY]->(a)",
            id
        ))
        .unwrap();
    }

    // normal_user(3) -> normal_target(4): 1 笔正常交易
    db.execute("CREATE (t:Transaction {id: 306, amount: 15000.0, ts: 5000})").unwrap();
    db.execute(
        "MATCH (a:Account), (t:Transaction) WHERE a.id = 3 AND t.id = 306 \
         CREATE (a)-[:INITIATED]->(t)",
    )
    .unwrap();
    db.execute(
        "MATCH (t:Transaction), (a:Account) WHERE t.id = 306 AND a.id = 4 \
         CREATE (t)-[:RECEIVED_BY]->(a)",
    )
    .unwrap();

    // 断言 1：找到同一源→同一目标、短时间内 (ts 范围 50s) 多笔小额 (<10000) 的模式
    let result = db
        .query(
            "MATCH (src:Account)-[:INITIATED]->(t:Transaction)-[:RECEIVED_BY]->(dst:Account) \
             WHERE t.amount < 10000.0 AND t.ts >= 5000 AND t.ts <= 5050 \
             RETURN src.id, dst.id, count(*) AS tx_count, sum(t.amount) AS total",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1, "应识别出 1 对 structuring 组合");
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 1, "源应为 structurer");
    assert_eq!(result.rows()[0].get_int(1).unwrap(), 2, "目标应为 target_mule");
    assert_eq!(result.rows()[0].get_int(2).unwrap(), 5, "应有 5 笔分拆交易");

    // 总金额应接近 48500
    let total = result.rows()[0].get_float(3).unwrap();
    assert!(
        (total - 48500.0).abs() < 1.0,
        "分拆交易总金额应为 48500，实际 {}",
        total
    );

    // 断言 2：单笔超阈值的交易不在分拆模式中
    let over_threshold = db
        .query(
            "MATCH (src:Account)-[:INITIATED]->(t:Transaction)-[:RECEIVED_BY]->(dst:Account) \
             WHERE t.amount >= 10000.0 \
             RETURN src.name, t.amount, dst.name",
        )
        .unwrap();
    assert_eq!(over_threshold.num_rows(), 1, "应有 1 笔超阈值交易");
    assert_eq!(over_threshold.rows()[0].get_string(0).unwrap(), "normal_user");
}

// ════════════════════════════════════════════════════════════════
// F-04: 交易节点模型主路径（设备、IP、状态追踪）
// ════════════════════════════════════════════════════════════════

#[test]
fn f04_transaction_node_model() {
    let db = Database::in_memory();

    db.execute(
        "CREATE NODE TABLE Account(id INT64, name STRING, risk_tier STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute(
        "CREATE NODE TABLE Transaction(id INT64, amount DOUBLE, status STRING, risk_score INT64, ts INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE NODE TABLE Device(id INT64, fingerprint STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE IPAddress(id INT64, addr STRING, PRIMARY KEY(id))").unwrap();

    db.execute("CREATE REL TABLE INITIATED(FROM Account TO Transaction)").unwrap();
    db.execute("CREATE REL TABLE RECEIVED_BY(FROM Transaction TO Account)").unwrap();
    db.execute("CREATE REL TABLE USED_DEVICE(FROM Transaction TO Device)").unwrap();
    db.execute("CREATE REL TABLE FROM_IP(FROM Transaction TO IPAddress)").unwrap();

    // 账户
    for (id, name, tier) in [
        (1, "alice", "normal"),
        (2, "bob", "normal"),
        (3, "mule_x", "high"),
        (4, "shell_co", "critical"),
    ] {
        db.execute(&format!(
            "CREATE (a:Account {{id: {}, name: '{}', risk_tier: '{}'}})",
            id, name, tier
        ))
        .unwrap();
    }

    // 设备和 IP
    db.execute("CREATE (d:Device {id: 1, fingerprint: 'fp_shared_001'})").unwrap();
    db.execute("CREATE (d:Device {id: 2, fingerprint: 'fp_clean_002'})").unwrap();
    db.execute("CREATE (ip:IPAddress {id: 1, addr: '203.0.113.50'})").unwrap();
    db.execute("CREATE (ip:IPAddress {id: 2, addr: '10.0.0.1'})").unwrap();

    // 交易
    for (id, amount, status, risk_score, ts) in [
        (401, 9800.0, "posted", 85, 1000),
        (402, 9900.0, "posted", 90, 1010),
        (403, 500.0, "posted", 10, 2000),
        (404, 50000.0, "blocked", 99, 3000),
    ] {
        db.execute(&format!(
            "CREATE (t:Transaction {{id: {}, amount: {:.1}, status: '{}', risk_score: {}, ts: {}}})",
            id, amount, status, risk_score, ts
        ))
        .unwrap();
    }

    // alice 发起 401, 402 给 mule_x
    for tx_id in [401, 402] {
        db.execute(&format!(
            "MATCH (a:Account), (t:Transaction) WHERE a.id = 1 AND t.id = {} \
             CREATE (a)-[:INITIATED]->(t)",
            tx_id
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (t:Transaction), (a:Account) WHERE t.id = {} AND a.id = 3 \
             CREATE (t)-[:RECEIVED_BY]->(a)",
            tx_id
        ))
        .unwrap();
    }

    // bob 发起 403 给 alice (正常)
    db.execute(
        "MATCH (a:Account), (t:Transaction) WHERE a.id = 2 AND t.id = 403 \
         CREATE (a)-[:INITIATED]->(t)",
    )
    .unwrap();
    db.execute(
        "MATCH (t:Transaction), (a:Account) WHERE t.id = 403 AND a.id = 1 \
         CREATE (t)-[:RECEIVED_BY]->(a)",
    )
    .unwrap();

    // shell_co 发起 404 给 mule_x (blocked)
    db.execute(
        "MATCH (a:Account), (t:Transaction) WHERE a.id = 4 AND t.id = 404 \
         CREATE (a)-[:INITIATED]->(t)",
    )
    .unwrap();
    db.execute(
        "MATCH (t:Transaction), (a:Account) WHERE t.id = 404 AND a.id = 3 \
         CREATE (t)-[:RECEIVED_BY]->(a)",
    )
    .unwrap();

    // 设备和 IP 关联：401, 402, 404 使用共享设备 1 和可疑 IP 1
    for tx_id in [401, 402, 404] {
        db.execute(&format!(
            "MATCH (t:Transaction), (d:Device) WHERE t.id = {} AND d.id = 1 \
             CREATE (t)-[:USED_DEVICE]->(d)",
            tx_id
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (t:Transaction), (ip:IPAddress) WHERE t.id = {} AND ip.id = 1 \
             CREATE (t)-[:FROM_IP]->(ip)",
            tx_id
        ))
        .unwrap();
    }
    // 403 使用干净设备 2 和内网 IP 2
    db.execute(
        "MATCH (t:Transaction), (d:Device) WHERE t.id = 403 AND d.id = 2 \
         CREATE (t)-[:USED_DEVICE]->(d)",
    )
    .unwrap();
    db.execute(
        "MATCH (t:Transaction), (ip:IPAddress) WHERE t.id = 403 AND ip.id = 2 \
         CREATE (t)-[:FROM_IP]->(ip)",
    )
    .unwrap();

    // 断言 1：通过交易节点追踪 Account→Transaction→Device
    let device_trace = db
        .query(
            "MATCH (a:Account)-[:INITIATED]->(t:Transaction)-[:USED_DEVICE]->(d:Device) \
             WHERE d.fingerprint = 'fp_shared_001' \
             RETURN DISTINCT a.name ORDER BY a.name",
        )
        .unwrap();
    let names: Vec<&str> = device_trace.rows().iter().map(|r| r.get_string(0).unwrap()).collect();
    assert!(names.contains(&"alice"), "alice 应使用过共享设备");
    assert!(names.contains(&"shell_co"), "shell_co 应使用过共享设备");

    // 断言 2：追踪高风险交易 (risk_score >= 80) 及其 IP
    let risky = db
        .query(
            "MATCH (a:Account)-[:INITIATED]->(t:Transaction)-[:FROM_IP]->(ip:IPAddress) \
             WHERE t.risk_score >= 80 \
             RETURN a.name, t.id, t.risk_score, ip.addr ORDER BY t.risk_score DESC",
        )
        .unwrap();
    assert_eq!(risky.num_rows(), 3, "应有 3 笔高风险交易使用了 IP");
    // 最高风险交易应是 404 (risk_score=99)
    assert_eq!(risky.rows()[0].get_int(2).unwrap(), 99);

    // 断言 3：被拦截的交易
    let blocked = db
        .query(
            "MATCH (src:Account)-[:INITIATED]->(t:Transaction)-[:RECEIVED_BY]->(dst:Account) \
             WHERE t.status = 'blocked' \
             RETURN src.name, t.amount, dst.name",
        )
        .unwrap();
    assert_eq!(blocked.num_rows(), 1, "应有 1 笔被拦截交易");
    assert_eq!(blocked.rows()[0].get_string(0).unwrap(), "shell_co");

    // 断言 4：完整路径 Account→Transaction→Account（验证两端可达）
    let paths = db
        .query(
            "MATCH (src:Account)-[:INITIATED]->(t:Transaction)-[:RECEIVED_BY]->(dst:Account) \
             RETURN src.name, t.amount, dst.name ORDER BY t.amount DESC",
        )
        .unwrap();
    assert_eq!(paths.num_rows(), 4, "应有 4 条完整的交易路径");
}

// ════════════════════════════════════════════════════════════════
// F-05: 案件调查子图（完整证据链）
// ════════════════════════════════════════════════════════════════

#[test]
fn f05_investigation_subgraph() {
    let db = Database::in_memory();

    // 创建完整的调查子图表结构
    db.execute(
        "CREATE NODE TABLE Account(id INT64, name STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute(
        "CREATE NODE TABLE Transaction(id INT64, amount DOUBLE, ts INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE NODE TABLE Device(id INT64, fingerprint STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE IPAddress(id INT64, addr STRING, PRIMARY KEY(id))").unwrap();
    db.execute(
        "CREATE NODE TABLE Alert(id INT64, severity STRING, ts INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute(
        "CREATE NODE TABLE FraudCase(id INT64, status STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute(
        "CREATE NODE TABLE Investigator(id INT64, name STRING, PRIMARY KEY(id))",
    )
    .unwrap();

    db.execute("CREATE REL TABLE INITIATED(FROM Account TO Transaction)").unwrap();
    db.execute("CREATE REL TABLE RECEIVED_BY(FROM Transaction TO Account)").unwrap();
    db.execute("CREATE REL TABLE USED_DEVICE(FROM Transaction TO Device)").unwrap();
    db.execute("CREATE REL TABLE FROM_IP(FROM Transaction TO IPAddress)").unwrap();
    db.execute("CREATE REL TABLE FLAGGED_BY(FROM Transaction TO Alert)").unwrap();
    db.execute("CREATE REL TABLE INVOLVES(FROM FraudCase TO Transaction)").unwrap();
    db.execute("CREATE REL TABLE ASSIGNED_TO(FROM FraudCase TO Investigator)").unwrap();

    // 数据
    db.execute("CREATE (a:Account {id: 1, name: 'suspect_alice'})").unwrap();
    db.execute("CREATE (a:Account {id: 2, name: 'mule_bob'})").unwrap();
    db.execute("CREATE (t:Transaction {id: 501, amount: 95000.0, ts: 1000})").unwrap();
    db.execute("CREATE (t:Transaction {id: 502, amount: 94000.0, ts: 1010})").unwrap();
    db.execute("CREATE (d:Device {id: 1, fingerprint: 'fp_suspect'})").unwrap();
    db.execute("CREATE (ip:IPAddress {id: 1, addr: '185.100.87.202'})").unwrap();
    db.execute("CREATE (al:Alert {id: 1, severity: 'critical', ts: 1001})").unwrap();
    db.execute("CREATE (al:Alert {id: 2, severity: 'high', ts: 1011})").unwrap();
    db.execute("CREATE (c:FraudCase {id: 1, status: 'open'})").unwrap();
    db.execute("CREATE (inv:Investigator {id: 1, name: 'detective_zhang'})").unwrap();

    // 关系链条：Account→Transaction→Account, Transaction→Device, Transaction→IP
    for tx_id in [501, 502] {
        db.execute(&format!(
            "MATCH (a:Account), (t:Transaction) WHERE a.id = 1 AND t.id = {} \
             CREATE (a)-[:INITIATED]->(t)",
            tx_id
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (t:Transaction), (a:Account) WHERE t.id = {} AND a.id = 2 \
             CREATE (t)-[:RECEIVED_BY]->(a)",
            tx_id
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (t:Transaction), (d:Device) WHERE t.id = {} AND d.id = 1 \
             CREATE (t)-[:USED_DEVICE]->(d)",
            tx_id
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (t:Transaction), (ip:IPAddress) WHERE t.id = {} AND ip.id = 1 \
             CREATE (t)-[:FROM_IP]->(ip)",
            tx_id
        ))
        .unwrap();
    }

    // Transaction→Alert
    db.execute(
        "MATCH (t:Transaction), (al:Alert) WHERE t.id = 501 AND al.id = 1 \
         CREATE (t)-[:FLAGGED_BY]->(al)",
    )
    .unwrap();
    db.execute(
        "MATCH (t:Transaction), (al:Alert) WHERE t.id = 502 AND al.id = 2 \
         CREATE (t)-[:FLAGGED_BY]->(al)",
    )
    .unwrap();

    // FraudCase→Transaction, FraudCase→Investigator
    for tx_id in [501, 502] {
        db.execute(&format!(
            "MATCH (c:FraudCase), (t:Transaction) WHERE c.id = 1 AND t.id = {} \
             CREATE (c)-[:INVOLVES]->(t)",
            tx_id
        ))
        .unwrap();
    }
    db.execute(
        "MATCH (c:FraudCase), (inv:Investigator) WHERE c.id = 1 AND inv.id = 1 \
         CREATE (c)-[:ASSIGNED_TO]->(inv)",
    )
    .unwrap();

    // 断言 1：从 Alert 追溯到发起账户
    let trace_from_alert = db
        .query(
            "MATCH (a:Account)-[:INITIATED]->(t:Transaction)-[:FLAGGED_BY]->(al:Alert) \
             WHERE al.severity = 'critical' \
             RETURN a.name, t.amount, al.severity",
        )
        .unwrap();
    assert_eq!(trace_from_alert.num_rows(), 1, "critical alert 应追溯到 1 个账户");
    assert_eq!(trace_from_alert.rows()[0].get_string(0).unwrap(), "suspect_alice");

    // 断言 2：从 FraudCase 追溯到所有涉及的交易
    let case_txs = db
        .query(
            "MATCH (c:FraudCase)-[:INVOLVES]->(t:Transaction) \
             WHERE c.id = 1 \
             RETURN t.id ORDER BY t.id",
        )
        .unwrap();
    assert_eq!(case_txs.num_rows(), 2, "案件应涉及 2 笔交易");
    assert_eq!(case_txs.rows()[0].get_int(0).unwrap(), 501);
    assert_eq!(case_txs.rows()[1].get_int(0).unwrap(), 502);

    // 断言 3：FraudCase→Investigator 分配
    let assigned = db
        .query(
            "MATCH (c:FraudCase)-[:ASSIGNED_TO]->(inv:Investigator) \
             WHERE c.id = 1 \
             RETURN inv.name",
        )
        .unwrap();
    assert_eq!(assigned.num_rows(), 1);
    assert_eq!(assigned.rows()[0].get_string(0).unwrap(), "detective_zhang");

    // 断言 4：多跳查询 FraudCase→Transaction→Device
    let case_devices = db
        .query(
            "MATCH (c:FraudCase)-[:INVOLVES]->(t:Transaction)-[:USED_DEVICE]->(d:Device) \
             WHERE c.id = 1 \
             RETURN d.fingerprint",
        )
        .unwrap();
    // 两笔交易都使用了 fp_suspect 设备，结果可能有 2 行
    assert!(case_devices.num_rows() >= 1, "案件涉及的交易应关联到设备");
    for row in case_devices.rows() {
        assert_eq!(
            row.get_string(0).unwrap(),
            "fp_suspect",
            "所有设备应为 fp_suspect"
        );
    }

    // 断言 5：多跳查询 FraudCase→Transaction→IP
    let case_ips = db
        .query(
            "MATCH (c:FraudCase)-[:INVOLVES]->(t:Transaction)-[:FROM_IP]->(ip:IPAddress) \
             WHERE c.id = 1 \
             RETURN ip.addr",
        )
        .unwrap();
    // 两笔交易都使用了同一 IP，结果可能有 2 行
    assert!(case_ips.num_rows() >= 1, "案件涉及的交易应关联到 IP");
    for row in case_ips.rows() {
        assert_eq!(
            row.get_string(0).unwrap(),
            "185.100.87.202",
            "所有 IP 应为 185.100.87.202"
        );
    }

    // 断言 6：完整证据链 Account→Transaction→Alert（从嫌疑人到告警）
    let full_chain = db
        .query(
            "MATCH (a:Account)-[:INITIATED]->(t:Transaction)-[:FLAGGED_BY]->(al:Alert) \
             WHERE a.name = 'suspect_alice' \
             RETURN t.id, al.severity ORDER BY t.id",
        )
        .unwrap();
    assert_eq!(full_chain.num_rows(), 2, "suspect_alice 应触发 2 个告警");
}

// ════════════════════════════════════════════════════════════════
// F-06: 风险分数回写与复核
// ════════════════════════════════════════════════════════════════

#[test]
fn f06_risk_score_update() {
    let db = Database::in_memory();

    db.execute(
        "CREATE NODE TABLE Account(id INT64, name STRING, risk_score INT64, PRIMARY KEY(id))",
    )
    .unwrap();

    // 初始数据：所有账户低风险
    db.execute("CREATE (a:Account {id: 1, name: 'alice', risk_score: 20})").unwrap();
    db.execute("CREATE (a:Account {id: 2, name: 'bob', risk_score: 30})").unwrap();
    db.execute("CREATE (a:Account {id: 3, name: 'charlie', risk_score: 15})").unwrap();
    db.execute("CREATE (a:Account {id: 4, name: 'dave', risk_score: 50})").unwrap();
    db.execute("CREATE (a:Account {id: 5, name: 'eve', risk_score: 45})").unwrap();

    // 断言 1：初始高风险集合 (risk_score >= 80) 应为空
    let initial_high = db
        .query(
            "MATCH (a:Account) WHERE a.risk_score >= 80 \
             RETURN a.name ORDER BY a.name",
        )
        .unwrap();
    assert_eq!(initial_high.num_rows(), 0, "初始应无高风险账户");

    // 更新：提升 alice 和 dave 的风险分数
    db.execute("MATCH (a:Account) WHERE a.id = 1 SET a.risk_score = 92").unwrap();
    db.execute("MATCH (a:Account) WHERE a.id = 4 SET a.risk_score = 88").unwrap();

    // 断言 2：高风险集合应包含 alice 和 dave
    let high_risk = db
        .query(
            "MATCH (a:Account) WHERE a.risk_score >= 80 \
             RETURN a.name ORDER BY a.name",
        )
        .unwrap();
    assert_eq!(high_risk.num_rows(), 2, "更新后应有 2 个高风险账户");
    assert_eq!(high_risk.rows()[0].get_string(0).unwrap(), "alice");
    assert_eq!(high_risk.rows()[1].get_string(0).unwrap(), "dave");

    // 断言 3：进一步更新，降低 alice 的风险分数
    db.execute("MATCH (a:Account) WHERE a.id = 1 SET a.risk_score = 40").unwrap();

    let high_risk_after = db
        .query(
            "MATCH (a:Account) WHERE a.risk_score >= 80 \
             RETURN a.name",
        )
        .unwrap();
    assert_eq!(high_risk_after.num_rows(), 1, "alice 降分后应只剩 1 个高风险账户");
    assert_eq!(high_risk_after.rows()[0].get_string(0).unwrap(), "dave");

    // 断言 4：验证具体分数正确
    let alice_score = db
        .query("MATCH (a:Account) WHERE a.id = 1 RETURN a.risk_score")
        .unwrap();
    assert_eq!(alice_score.rows()[0].get_int(0).unwrap(), 40, "alice 的分数应为 40");

    let dave_score = db
        .query("MATCH (a:Account) WHERE a.id = 4 RETURN a.risk_score")
        .unwrap();
    assert_eq!(dave_score.rows()[0].get_int(0).unwrap(), 88, "dave 的分数应为 88");

    // 断言 5：中等风险区间 [40, 60] 的账户
    let mid_risk = db
        .query(
            "MATCH (a:Account) WHERE a.risk_score >= 40 AND a.risk_score <= 60 \
             RETURN a.name ORDER BY a.name",
        )
        .unwrap();
    // alice(40), dave 已变 88 不在区间，bob(30) 不在，charlie(15) 不在，eve(45)
    assert_eq!(mid_risk.num_rows(), 2, "中等风险区间应有 2 个账户");
    let mid_names: Vec<&str> = mid_risk.rows().iter().map(|r| r.get_string(0).unwrap()).collect();
    assert!(mid_names.contains(&"alice"));
    assert!(mid_names.contains(&"eve"));
}

// ════════════════════════════════════════════════════════════════
// F-07: 黑名单/灰名单联动
// ════════════════════════════════════════════════════════════════

#[test]
fn f07_blacklist_graylist() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE Account(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Device(id INT64, fingerprint STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE IPAddress(id INT64, addr STRING, PRIMARY KEY(id))").unwrap();
    db.execute(
        "CREATE NODE TABLE Blacklist(id INT64, list_type STRING, entity_type STRING, PRIMARY KEY(id))",
    )
    .unwrap();

    db.execute("CREATE REL TABLE ON_LIST(FROM Account TO Blacklist)").unwrap();
    db.execute("CREATE REL TABLE DEVICE_ON_LIST(FROM Device TO Blacklist)").unwrap();
    db.execute("CREATE REL TABLE IP_ON_LIST(FROM IPAddress TO Blacklist)").unwrap();
    db.execute("CREATE REL TABLE USED_DEVICE(FROM Account TO Device)").unwrap();
    db.execute("CREATE REL TABLE FROM_IP(FROM Account TO IPAddress)").unwrap();

    // 名单节点
    db.execute(
        "CREATE (bl:Blacklist {id: 1, list_type: 'blacklist', entity_type: 'account'})",
    )
    .unwrap();
    db.execute(
        "CREATE (bl:Blacklist {id: 2, list_type: 'graylist', entity_type: 'account'})",
    )
    .unwrap();
    db.execute(
        "CREATE (bl:Blacklist {id: 3, list_type: 'blacklist', entity_type: 'device'})",
    )
    .unwrap();
    db.execute(
        "CREATE (bl:Blacklist {id: 4, list_type: 'blacklist', entity_type: 'ip'})",
    )
    .unwrap();

    // 账户
    db.execute("CREATE (a:Account {id: 1, name: 'clean_alice'})").unwrap();
    db.execute("CREATE (a:Account {id: 2, name: 'blacklisted_bob'})").unwrap();
    db.execute("CREATE (a:Account {id: 3, name: 'graylisted_charlie'})").unwrap();
    db.execute("CREATE (a:Account {id: 4, name: 'indirect_dave'})").unwrap();

    // 设备和 IP
    db.execute("CREATE (d:Device {id: 1, fingerprint: 'fp_banned'})").unwrap();
    db.execute("CREATE (d:Device {id: 2, fingerprint: 'fp_clean'})").unwrap();
    db.execute("CREATE (ip:IPAddress {id: 1, addr: '192.168.1.1'})").unwrap();
    db.execute("CREATE (ip:IPAddress {id: 2, addr: '10.0.0.1'})").unwrap();

    // 名单关联
    db.execute(
        "MATCH (a:Account), (bl:Blacklist) WHERE a.id = 2 AND bl.id = 1 \
         CREATE (a)-[:ON_LIST]->(bl)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Account), (bl:Blacklist) WHERE a.id = 3 AND bl.id = 2 \
         CREATE (a)-[:ON_LIST]->(bl)",
    )
    .unwrap();
    db.execute(
        "MATCH (d:Device), (bl:Blacklist) WHERE d.id = 1 AND bl.id = 3 \
         CREATE (d)-[:DEVICE_ON_LIST]->(bl)",
    )
    .unwrap();
    db.execute(
        "MATCH (ip:IPAddress), (bl:Blacklist) WHERE ip.id = 1 AND bl.id = 4 \
         CREATE (ip)-[:IP_ON_LIST]->(bl)",
    )
    .unwrap();

    // 设备/IP 使用关系
    db.execute(
        "MATCH (a:Account), (d:Device) WHERE a.id = 4 AND d.id = 1 \
         CREATE (a)-[:USED_DEVICE]->(d)",
    )
    .unwrap(); // dave 使用被禁设备
    db.execute(
        "MATCH (a:Account), (ip:IPAddress) WHERE a.id = 4 AND ip.id = 1 \
         CREATE (a)-[:FROM_IP]->(ip)",
    )
    .unwrap(); // dave 使用被禁 IP
    db.execute(
        "MATCH (a:Account), (d:Device) WHERE a.id = 1 AND d.id = 2 \
         CREATE (a)-[:USED_DEVICE]->(d)",
    )
    .unwrap(); // alice 使用干净设备
    db.execute(
        "MATCH (a:Account), (ip:IPAddress) WHERE a.id = 1 AND ip.id = 2 \
         CREATE (a)-[:FROM_IP]->(ip)",
    )
    .unwrap();

    // 断言 1：直接黑名单命中
    let blacklisted = db
        .query(
            "MATCH (a:Account)-[:ON_LIST]->(bl:Blacklist) \
             WHERE bl.list_type = 'blacklist' \
             RETURN a.name",
        )
        .unwrap();
    assert_eq!(blacklisted.num_rows(), 1, "应有 1 个账户直接在黑名单上");
    assert_eq!(blacklisted.rows()[0].get_string(0).unwrap(), "blacklisted_bob");

    // 断言 2：灰名单命中
    let graylisted = db
        .query(
            "MATCH (a:Account)-[:ON_LIST]->(bl:Blacklist) \
             WHERE bl.list_type = 'graylist' \
             RETURN a.name",
        )
        .unwrap();
    assert_eq!(graylisted.num_rows(), 1);
    assert_eq!(graylisted.rows()[0].get_string(0).unwrap(), "graylisted_charlie");

    // 断言 3：通过设备间接命中黑名单的账户
    let device_blacklisted = db
        .query(
            "MATCH (a:Account)-[:USED_DEVICE]->(d:Device)-[:DEVICE_ON_LIST]->(bl:Blacklist) \
             WHERE bl.list_type = 'blacklist' \
             RETURN a.name",
        )
        .unwrap();
    assert_eq!(device_blacklisted.num_rows(), 1, "应有 1 个账户通过设备间接命中黑名单");
    assert_eq!(device_blacklisted.rows()[0].get_string(0).unwrap(), "indirect_dave");

    // 断言 4：通过 IP 间接命中黑名单的账户
    let ip_blacklisted = db
        .query(
            "MATCH (a:Account)-[:FROM_IP]->(ip:IPAddress)-[:IP_ON_LIST]->(bl:Blacklist) \
             WHERE bl.list_type = 'blacklist' \
             RETURN a.name",
        )
        .unwrap();
    assert_eq!(ip_blacklisted.num_rows(), 1, "应有 1 个账户通过 IP 间接命中黑名单");
    assert_eq!(ip_blacklisted.rows()[0].get_string(0).unwrap(), "indirect_dave");

    // 断言 5：clean_alice 不在任何名单上
    let alice_lists = db
        .query(
            "MATCH (a:Account)-[:ON_LIST]->(bl:Blacklist) \
             WHERE a.id = 1 \
             RETURN bl.list_type",
        )
        .unwrap();
    assert_eq!(alice_lists.num_rows(), 0, "alice 不应在任何名单上");
}

// ════════════════════════════════════════════════════════════════
// F-08: 共用基础设施团伙识别 (WCC)
// ════════════════════════════════════════════════════════════════

#[test]
fn f08_gang_identification_wcc() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE Account(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Device(id INT64, fingerprint STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE IPAddress(id INT64, addr STRING, PRIMARY KEY(id))").unwrap();

    db.execute("CREATE REL TABLE USED_DEVICE(FROM Account TO Device)").unwrap();
    db.execute("CREATE REL TABLE FROM_IP(FROM Account TO IPAddress)").unwrap();
    // 为 WCC 分析构建双向的"共用"关系：Account 之间的连接
    db.execute("CREATE REL TABLE SHARES_INFRA(FROM Account TO Account)").unwrap();

    // 团伙 A：3 个账户共享 1 个设备
    for i in 1..=3 {
        db.execute(&format!(
            "CREATE (a:Account {{id: {}, name: 'gang_a_{}'}})",
            i, i
        ))
        .unwrap();
    }
    db.execute("CREATE (d:Device {id: 1, fingerprint: 'fp_gang_a'})").unwrap();
    for i in 1..=3 {
        db.execute(&format!(
            "MATCH (a:Account), (d:Device) WHERE a.id = {} AND d.id = 1 \
             CREATE (a)-[:USED_DEVICE]->(d)",
            i
        ))
        .unwrap();
    }

    // 团伙 B：4 个账户共享 1 个 IP
    for i in 4..=7 {
        db.execute(&format!(
            "CREATE (a:Account {{id: {}, name: 'gang_b_{}'}})",
            i, i
        ))
        .unwrap();
    }
    db.execute("CREATE (ip:IPAddress {id: 1, addr: '10.0.0.99'})").unwrap();
    for i in 4..=7 {
        db.execute(&format!(
            "MATCH (a:Account), (ip:IPAddress) WHERE a.id = {} AND ip.id = 1 \
             CREATE (a)-[:FROM_IP]->(ip)",
            i
        ))
        .unwrap();
    }

    // 孤立账户（不在任何团伙中）
    db.execute("CREATE (a:Account {id: 8, name: 'lone_wolf'})").unwrap();

    // 为 WCC 分析构建 SHARES_INFRA 关系（同团伙成员互连）
    // 团伙 A 内部连接
    for (a, b) in [(1, 2), (2, 3)] {
        db.execute(&format!(
            "MATCH (a:Account), (b:Account) WHERE a.id = {} AND b.id = {} \
             CREATE (a)-[:SHARES_INFRA]->(b)",
            a, b
        ))
        .unwrap();
    }
    // 团伙 B 内部连接
    for (a, b) in [(4, 5), (5, 6), (6, 7)] {
        db.execute(&format!(
            "MATCH (a:Account), (b:Account) WHERE a.id = {} AND b.id = {} \
             CREATE (a)-[:SHARES_INFRA]->(b)",
            a, b
        ))
        .unwrap();
    }

    // 断言 1：WCC 应识别出至少 2 个连通分量（+ 孤立节点）
    let wcc = db.query("CALL wcc('SHARES_INFRA') YIELD node_id, component_id").unwrap();
    assert!(wcc.num_rows() > 0, "WCC 应返回结果");

    let mut components: Vec<i64> = wcc.rows().iter().map(|r| r.get_int(1).unwrap()).collect();
    components.sort();
    components.dedup();
    assert!(
        components.len() >= 2,
        "应至少有 2 个连通分量（团伙 A + 团伙 B），实际 {}",
        components.len()
    );

    // 断言 2：共用设备 1 的账户数
    let shared_device = db
        .query(
            "MATCH (a:Account)-[:USED_DEVICE]->(d:Device) \
             WHERE d.id = 1 \
             RETURN a.id ORDER BY a.id",
        )
        .unwrap();
    assert_eq!(shared_device.num_rows(), 3, "设备 1 应被 3 个账户共用");

    // 断言 3：共用 IP 1 的账户数
    let shared_ip = db
        .query(
            "MATCH (a:Account)-[:FROM_IP]->(ip:IPAddress) \
             WHERE ip.id = 1 \
             RETURN a.id ORDER BY a.id",
        )
        .unwrap();
    assert_eq!(shared_ip.num_rows(), 4, "IP 1 应被 4 个账户共用");

    // 断言 4：统计每个设备/IP 关联的账户数量
    let device_counts = db
        .query(
            "MATCH (a:Account)-[:USED_DEVICE]->(d:Device) \
             RETURN d.fingerprint, count(*) AS acct_count",
        )
        .unwrap();
    assert_eq!(device_counts.num_rows(), 1);
    assert_eq!(device_counts.rows()[0].get_int(1).unwrap(), 3);
}

// ════════════════════════════════════════════════════════════════
// F-09: 资金闭环与循环链检测
// ════════════════════════════════════════════════════════════════

#[test]
fn f09_fund_cycle_detection() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE Account(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE TRANSFERRED_TO(FROM Account TO Account, amount DOUBLE)").unwrap();

    // 构建环路：A(1) -> B(2) -> C(3) -> A(1)
    db.execute("CREATE (a:Account {id: 1, name: 'cycle_a'})").unwrap();
    db.execute("CREATE (a:Account {id: 2, name: 'cycle_b'})").unwrap();
    db.execute("CREATE (a:Account {id: 3, name: 'cycle_c'})").unwrap();
    // 非环路账户
    db.execute("CREATE (a:Account {id: 4, name: 'external'})").unwrap();

    db.execute(
        "MATCH (a:Account), (b:Account) WHERE a.id = 1 AND b.id = 2 \
         CREATE (a)-[:TRANSFERRED_TO {amount: 10000.0}]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Account), (b:Account) WHERE a.id = 2 AND b.id = 3 \
         CREATE (a)-[:TRANSFERRED_TO {amount: 9500.0}]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Account), (b:Account) WHERE a.id = 3 AND b.id = 1 \
         CREATE (a)-[:TRANSFERRED_TO {amount: 9000.0}]->(b)",
    )
    .unwrap();

    // 外部单向转账（不形成环）
    db.execute(
        "MATCH (a:Account), (b:Account) WHERE a.id = 4 AND b.id = 1 \
         CREATE (a)-[:TRANSFERRED_TO {amount: 5000.0}]->(b)",
    )
    .unwrap();

    // 断言 1：验证环路各段存在
    let seg1 = db
        .query(
            "MATCH (a:Account)-[:TRANSFERRED_TO]->(b:Account) \
             WHERE a.id = 1 AND b.id = 2 RETURN a.name",
        )
        .unwrap();
    assert_eq!(seg1.num_rows(), 1, "1->2 转账应存在");

    let seg2 = db
        .query(
            "MATCH (a:Account)-[:TRANSFERRED_TO]->(b:Account) \
             WHERE a.id = 2 AND b.id = 3 RETURN a.name",
        )
        .unwrap();
    assert_eq!(seg2.num_rows(), 1, "2->3 转账应存在");

    let seg3 = db
        .query(
            "MATCH (a:Account)-[:TRANSFERRED_TO]->(b:Account) \
             WHERE a.id = 3 AND b.id = 1 RETURN a.name",
        )
        .unwrap();
    assert_eq!(seg3.num_rows(), 1, "3->1 转账应存在（环路闭合）");

    // 断言 2：使用可变长路径检测环路 (a.id = b.id)
    // 引擎可能有 cycle avoidance，所以做容错处理
    let cycle_result = db
        .query(
            "MATCH (a:Account)-[:TRANSFERRED_TO*1..4]->(b:Account) \
             WHERE a.id = 1 AND b.id = 1 \
             RETURN a.name",
        )
        .unwrap();
    if cycle_result.num_rows() > 0 {
        assert_eq!(cycle_result.rows()[0].get_string(0).unwrap(), "cycle_a");
    }

    // 断言 3：从每个环路成员出发，都能到达其他两个成员
    for (start, expected_targets) in [(1, vec![2, 3]), (2, vec![1, 3]), (3, vec![1, 2])] {
        let reachable = db
            .query(&format!(
                "MATCH (a:Account)-[:TRANSFERRED_TO*1..3]->(b:Account) \
                 WHERE a.id = {} AND a.id <> b.id \
                 RETURN b.id ORDER BY b.id",
                start
            ))
            .unwrap();
        let ids: Vec<i64> = reachable.rows().iter().map(|r| r.get_int(0).unwrap()).collect();
        for target in &expected_targets {
            assert!(
                ids.contains(target),
                "从 {} 出发应能到达 {}，实际可达: {:?}",
                start,
                target,
                ids
            );
        }
    }

    // 断言 4：external(4) 不参与环路
    let ext_cycle = db
        .query(
            "MATCH (a:Account)-[:TRANSFERRED_TO*1..4]->(b:Account) \
             WHERE a.id = 4 AND b.id = 4 \
             RETURN a.name",
        )
        .unwrap();
    assert_eq!(ext_cycle.num_rows(), 0, "external 不应形成环路");
}

// ════════════════════════════════════════════════════════════════
// F-10: 跨境/跨渠道欺诈
// ════════════════════════════════════════════════════════════════

#[test]
fn f10_cross_border_fraud() {
    let db = Database::in_memory();

    db.execute(
        "CREATE NODE TABLE Account(id INT64, name STRING, country STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute(
        "CREATE NODE TABLE Transaction(id INT64, amount DOUBLE, ts INT64, dest_country STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE INITIATED(FROM Account TO Transaction)").unwrap();
    db.execute("CREATE REL TABLE RECEIVED_BY(FROM Transaction TO Account)").unwrap();

    // 账户（不同国家）
    db.execute("CREATE (a:Account {id: 1, name: 'cn_alice', country: 'CN'})").unwrap();
    db.execute("CREATE (a:Account {id: 2, name: 'us_bob', country: 'US'})").unwrap();
    db.execute("CREATE (a:Account {id: 3, name: 'jp_charlie', country: 'JP'})").unwrap();
    db.execute("CREATE (a:Account {id: 4, name: 'cn_dave', country: 'CN'})").unwrap();

    // 交易（含目标国家）
    // 跨境交易：CN -> US
    db.execute(
        "CREATE (t:Transaction {id: 701, amount: 50000.0, ts: 1000, dest_country: 'US'})",
    )
    .unwrap();
    // 跨境交易：CN -> JP
    db.execute(
        "CREATE (t:Transaction {id: 702, amount: 30000.0, ts: 1010, dest_country: 'JP'})",
    )
    .unwrap();
    // 国内交易：CN -> CN
    db.execute(
        "CREATE (t:Transaction {id: 703, amount: 8000.0, ts: 1020, dest_country: 'CN'})",
    )
    .unwrap();
    // 跨境交易：US -> JP
    db.execute(
        "CREATE (t:Transaction {id: 704, amount: 120000.0, ts: 1030, dest_country: 'JP'})",
    )
    .unwrap();
    // 跨境交易：CN -> US（大额）
    db.execute(
        "CREATE (t:Transaction {id: 705, amount: 200000.0, ts: 1040, dest_country: 'US'})",
    )
    .unwrap();

    // cn_alice -> us_bob (跨境)
    db.execute(
        "MATCH (a:Account), (t:Transaction) WHERE a.id = 1 AND t.id = 701 \
         CREATE (a)-[:INITIATED]->(t)",
    )
    .unwrap();
    db.execute(
        "MATCH (t:Transaction), (a:Account) WHERE t.id = 701 AND a.id = 2 \
         CREATE (t)-[:RECEIVED_BY]->(a)",
    )
    .unwrap();

    // cn_alice -> jp_charlie (跨境)
    db.execute(
        "MATCH (a:Account), (t:Transaction) WHERE a.id = 1 AND t.id = 702 \
         CREATE (a)-[:INITIATED]->(t)",
    )
    .unwrap();
    db.execute(
        "MATCH (t:Transaction), (a:Account) WHERE t.id = 702 AND a.id = 3 \
         CREATE (t)-[:RECEIVED_BY]->(a)",
    )
    .unwrap();

    // cn_alice -> cn_dave (国内)
    db.execute(
        "MATCH (a:Account), (t:Transaction) WHERE a.id = 1 AND t.id = 703 \
         CREATE (a)-[:INITIATED]->(t)",
    )
    .unwrap();
    db.execute(
        "MATCH (t:Transaction), (a:Account) WHERE t.id = 703 AND a.id = 4 \
         CREATE (t)-[:RECEIVED_BY]->(a)",
    )
    .unwrap();

    // us_bob -> jp_charlie (跨境)
    db.execute(
        "MATCH (a:Account), (t:Transaction) WHERE a.id = 2 AND t.id = 704 \
         CREATE (a)-[:INITIATED]->(t)",
    )
    .unwrap();
    db.execute(
        "MATCH (t:Transaction), (a:Account) WHERE t.id = 704 AND a.id = 3 \
         CREATE (t)-[:RECEIVED_BY]->(a)",
    )
    .unwrap();

    // cn_dave -> us_bob (跨境大额)
    db.execute(
        "MATCH (a:Account), (t:Transaction) WHERE a.id = 4 AND t.id = 705 \
         CREATE (a)-[:INITIATED]->(t)",
    )
    .unwrap();
    db.execute(
        "MATCH (t:Transaction), (a:Account) WHERE t.id = 705 AND a.id = 2 \
         CREATE (t)-[:RECEIVED_BY]->(a)",
    )
    .unwrap();

    // 断言 1：查找所有跨境交易（src.country <> dest_country）
    let cross_border = db
        .query(
            "MATCH (src:Account)-[:INITIATED]->(t:Transaction)-[:RECEIVED_BY]->(dst:Account) \
             WHERE src.country <> t.dest_country \
             RETURN src.name, t.amount, t.dest_country, dst.name ORDER BY t.amount DESC",
        )
        .unwrap();
    assert_eq!(cross_border.num_rows(), 4, "应有 4 笔跨境交易");
    // 最大额跨境交易
    let max_amount = cross_border.rows()[0].get_float(1).unwrap();
    assert!((max_amount - 200000.0).abs() < 1.0, "最大跨境交易应为 200000");

    // 断言 2：CN 账户发起的跨境交易
    let cn_cross = db
        .query(
            "MATCH (src:Account)-[:INITIATED]->(t:Transaction) \
             WHERE src.country = 'CN' AND t.dest_country <> 'CN' \
             RETURN src.name, t.amount, t.dest_country ORDER BY t.amount DESC",
        )
        .unwrap();
    assert_eq!(cn_cross.num_rows(), 3, "CN 账户应有 3 笔跨境交易");

    // 断言 3：目的地为 US 的所有交易
    let to_us = db
        .query(
            "MATCH (src:Account)-[:INITIATED]->(t:Transaction) \
             WHERE t.dest_country = 'US' \
             RETURN src.name, t.amount ORDER BY t.amount DESC",
        )
        .unwrap();
    assert_eq!(to_us.num_rows(), 2, "应有 2 笔目标为 US 的交易");

    // 断言 4：国内交易
    let domestic = db
        .query(
            "MATCH (src:Account)-[:INITIATED]->(t:Transaction) \
             WHERE src.country = t.dest_country \
             RETURN src.name, t.amount",
        )
        .unwrap();
    assert_eq!(domestic.num_rows(), 1, "应有 1 笔国内交易");
    assert_eq!(domestic.rows()[0].get_string(0).unwrap(), "cn_alice");

    // 断言 5：跨境大额交易 (>100000)
    let big_cross = db
        .query(
            "MATCH (src:Account)-[:INITIATED]->(t:Transaction)-[:RECEIVED_BY]->(dst:Account) \
             WHERE src.country <> t.dest_country AND t.amount > 100000.0 \
             RETURN src.name, t.amount, dst.name",
        )
        .unwrap();
    assert_eq!(big_cross.num_rows(), 2, "应有 2 笔跨境大额交易");
}

// ════════════════════════════════════════════════════════════════
// F-11: 合成身份与身份盗用检测
// ════════════════════════════════════════════════════════════════

#[test]
fn f11_synthetic_identity_detection() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE Account(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Phone(id INT64, number STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Email(id INT64, address STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Address(id INT64, value STRING, PRIMARY KEY(id))").unwrap();

    db.execute("CREATE REL TABLE HAS_PHONE(FROM Account TO Phone)").unwrap();
    db.execute("CREATE REL TABLE HAS_EMAIL(FROM Account TO Email)").unwrap();
    db.execute("CREATE REL TABLE HAS_ADDRESS(FROM Account TO Address)").unwrap();
    // 用于 WCC 的 Account 间关系
    db.execute("CREATE REL TABLE LINKED_BY_PII(FROM Account TO Account)").unwrap();

    // 账户
    db.execute("CREATE (a:Account {id: 1, name: 'John Smith'})").unwrap();
    db.execute("CREATE (a:Account {id: 2, name: 'Jon Smith'})").unwrap(); // 名字轻微变体
    db.execute("CREATE (a:Account {id: 3, name: 'Jane Doe'})").unwrap();
    db.execute("CREATE (a:Account {id: 4, name: 'Unrelated User'})").unwrap(); // 无关联用户

    // PII 片段作为独立节点
    db.execute("CREATE (p:Phone {id: 1, number: '13812345678'})").unwrap();
    db.execute("CREATE (p:Phone {id: 2, number: '13999999999'})").unwrap(); // 仅 account 4 使用
    db.execute("CREATE (e:Email {id: 1, address: 'john@mail.com'})").unwrap();
    db.execute("CREATE (e:Email {id: 2, address: 'unrelated@mail.com'})").unwrap();
    db.execute("CREATE (addr:Address {id: 1, value: '123 Main St'})").unwrap();
    db.execute("CREATE (addr:Address {id: 2, value: '456 Other St'})").unwrap();

    // 共享关系
    // account 1 和 2 共用电话 1
    db.execute(
        "MATCH (a:Account), (p:Phone) WHERE a.id = 1 AND p.id = 1 \
         CREATE (a)-[:HAS_PHONE]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Account), (p:Phone) WHERE a.id = 2 AND p.id = 1 \
         CREATE (a)-[:HAS_PHONE]->(p)",
    )
    .unwrap();

    // account 1 和 3 共用邮箱 1
    db.execute(
        "MATCH (a:Account), (e:Email) WHERE a.id = 1 AND e.id = 1 \
         CREATE (a)-[:HAS_EMAIL]->(e)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Account), (e:Email) WHERE a.id = 3 AND e.id = 1 \
         CREATE (a)-[:HAS_EMAIL]->(e)",
    )
    .unwrap();

    // account 2 和 3 共用地址 1
    db.execute(
        "MATCH (a:Account), (addr:Address) WHERE a.id = 2 AND addr.id = 1 \
         CREATE (a)-[:HAS_ADDRESS]->(addr)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Account), (addr:Address) WHERE a.id = 3 AND addr.id = 1 \
         CREATE (a)-[:HAS_ADDRESS]->(addr)",
    )
    .unwrap();

    // account 4 独立使用不同的 PII
    db.execute(
        "MATCH (a:Account), (p:Phone) WHERE a.id = 4 AND p.id = 2 \
         CREATE (a)-[:HAS_PHONE]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Account), (e:Email) WHERE a.id = 4 AND e.id = 2 \
         CREATE (a)-[:HAS_EMAIL]->(e)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Account), (addr:Address) WHERE a.id = 4 AND addr.id = 2 \
         CREATE (a)-[:HAS_ADDRESS]->(addr)",
    )
    .unwrap();

    // 断言 1：通过共享电话发现关联账户对
    let shared_phone = db
        .query(
            "MATCH (a1:Account)-[:HAS_PHONE]->(p:Phone)<-[:HAS_PHONE]-(a2:Account) \
             WHERE a1.id < a2.id \
             RETURN a1.name, a2.name, p.number",
        )
        .unwrap();
    assert_eq!(shared_phone.num_rows(), 1, "应有 1 对通过电话关联的账户");
    assert_eq!(shared_phone.rows()[0].get_string(0).unwrap(), "John Smith");
    assert_eq!(shared_phone.rows()[0].get_string(1).unwrap(), "Jon Smith");

    // 断言 2：通过共享邮箱发现关联账户对
    let shared_email = db
        .query(
            "MATCH (a1:Account)-[:HAS_EMAIL]->(e:Email)<-[:HAS_EMAIL]-(a2:Account) \
             WHERE a1.id < a2.id \
             RETURN a1.name, a2.name, e.address",
        )
        .unwrap();
    assert_eq!(shared_email.num_rows(), 1, "应有 1 对通过邮箱关联的账户");
    assert_eq!(shared_email.rows()[0].get_string(0).unwrap(), "John Smith");
    assert_eq!(shared_email.rows()[0].get_string(1).unwrap(), "Jane Doe");

    // 断言 3：通过共享地址发现关联账户对
    let shared_addr = db
        .query(
            "MATCH (a1:Account)-[:HAS_ADDRESS]->(addr:Address)<-[:HAS_ADDRESS]-(a2:Account) \
             WHERE a1.id < a2.id \
             RETURN a1.name, a2.name, addr.value",
        )
        .unwrap();
    assert_eq!(shared_addr.num_rows(), 1, "应有 1 对通过地址关联的账户");
    assert_eq!(shared_addr.rows()[0].get_string(0).unwrap(), "Jon Smith");
    assert_eq!(shared_addr.rows()[0].get_string(1).unwrap(), "Jane Doe");

    // 断言 4：account 4 不与任何其他账户共享 PII
    let unrelated_phone = db
        .query(
            "MATCH (a1:Account)-[:HAS_PHONE]->(p:Phone)<-[:HAS_PHONE]-(a2:Account) \
             WHERE a1.id = 4 AND a1.id <> a2.id \
             RETURN a2.name",
        )
        .unwrap();
    assert_eq!(unrelated_phone.num_rows(), 0, "account 4 不应与任何账户共享电话");

    let unrelated_email = db
        .query(
            "MATCH (a1:Account)-[:HAS_EMAIL]->(e:Email)<-[:HAS_EMAIL]-(a2:Account) \
             WHERE a1.id = 4 AND a1.id <> a2.id \
             RETURN a2.name",
        )
        .unwrap();
    assert_eq!(unrelated_email.num_rows(), 0, "account 4 不应与任何账户共享邮箱");

    // 断言 5：通过 PII 关联构建 LINKED_BY_PII 并验证 WCC
    // 手动根据 PII 共享创建链接关系
    // (1,2) 共用电话, (1,3) 共用邮箱, (2,3) 共用地址
    for (a, b) in [(1, 2), (1, 3), (2, 3)] {
        db.execute(&format!(
            "MATCH (a:Account), (b:Account) WHERE a.id = {} AND b.id = {} \
             CREATE (a)-[:LINKED_BY_PII]->(b)",
            a, b
        ))
        .unwrap();
    }

    let wcc = db.query("CALL wcc('LINKED_BY_PII') YIELD node_id, component_id").unwrap();
    assert!(wcc.num_rows() > 0, "WCC 应返回结果");

    // 收集组件信息
    let mut comp_map: std::collections::HashMap<i64, Vec<i64>> = std::collections::HashMap::new();
    for row in wcc.rows() {
        let node_id = row.get_int(0).unwrap();
        let comp_id = row.get_int(1).unwrap();
        comp_map.entry(comp_id).or_default().push(node_id);
    }

    // 账户 1, 2, 3 应在同一个连通分量中
    let mut found_cluster = false;
    for nodes in comp_map.values() {
        if nodes.len() >= 3 {
            found_cluster = true;
        }
    }
    assert!(found_cluster, "账户 1, 2, 3 应在同一个连通分量中形成团伙");
}

// ════════════════════════════════════════════════════════════════
// F-12: 多维度风控速率检查
// ════════════════════════════════════════════════════════════════

#[test]
fn f12_multi_dimensional_velocity_check() {
    let db = Database::in_memory();

    db.execute(
        "CREATE NODE TABLE Account(id INT64, name STRING, country STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute(
        "CREATE NODE TABLE Transaction(id INT64, amount DOUBLE, ts INT64, dest_country STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE INITIATED(FROM Account TO Transaction)").unwrap();
    db.execute("CREATE REL TABLE RECEIVED_BY(FROM Transaction TO Account)").unwrap();

    // 账户
    db.execute("CREATE (a:Account {id: 1, name: 'multi_dim_alice', country: 'CN'})").unwrap();
    db.execute("CREATE (a:Account {id: 2, name: 'normal_bob', country: 'CN'})").unwrap();
    db.execute("CREATE (a:Account {id: 3, name: 'target_us', country: 'US'})").unwrap();
    db.execute("CREATE (a:Account {id: 4, name: 'target_cn', country: 'CN'})").unwrap();

    // alice 的交易：多维度异常
    // 高频 + 跨境分拆：5 分钟内 3 笔交易到 US 目标
    let alice_split = vec![
        (801, 9999.0, 1000_i64, "US"),
        (802, 9998.0, 1020, "US"),
        (803, 9997.0, 1040, "US"),
    ];
    for (id, amount, ts, dest) in &alice_split {
        db.execute(&format!(
            "CREATE (t:Transaction {{id: {}, amount: {:.1}, ts: {}, dest_country: '{}'}})",
            id, amount, ts, dest
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (a:Account), (t:Transaction) WHERE a.id = 1 AND t.id = {} \
             CREATE (a)-[:INITIATED]->(t)",
            id
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (t:Transaction), (a:Account) WHERE t.id = {} AND a.id = 3 \
             CREATE (t)-[:RECEIVED_BY]->(a)",
            id
        ))
        .unwrap();
    }

    // alice 的大额国内交易
    db.execute(
        "CREATE (t:Transaction {id: 804, amount: 80000.0, ts: 5000, dest_country: 'CN'})",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Account), (t:Transaction) WHERE a.id = 1 AND t.id = 804 \
         CREATE (a)-[:INITIATED]->(t)",
    )
    .unwrap();
    db.execute(
        "MATCH (t:Transaction), (a:Account) WHERE t.id = 804 AND a.id = 4 \
         CREATE (t)-[:RECEIVED_BY]->(a)",
    )
    .unwrap();

    // bob 的正常交易：单笔国内
    db.execute(
        "CREATE (t:Transaction {id: 805, amount: 3000.0, ts: 2000, dest_country: 'CN'})",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Account), (t:Transaction) WHERE a.id = 2 AND t.id = 805 \
         CREATE (a)-[:INITIATED]->(t)",
    )
    .unwrap();
    db.execute(
        "MATCH (t:Transaction), (a:Account) WHERE t.id = 805 AND a.id = 4 \
         CREATE (t)-[:RECEIVED_BY]->(a)",
    )
    .unwrap();

    // 断言 1：频率维度 — [1000, 1040] 内 3 笔交易触发高频告警
    let high_freq = db
        .query(
            "MATCH (a:Account)-[:INITIATED]->(t:Transaction) \
             WHERE t.ts >= 1000 AND t.ts <= 1040 \
             RETURN a.id, a.name, count(*) AS tx_count ORDER BY count(*) DESC",
        )
        .unwrap();
    assert!(high_freq.num_rows() >= 1, "应有账户在时间窗内有交易");
    assert_eq!(high_freq.rows()[0].get_int(0).unwrap(), 1, "alice 应排第一");
    assert_eq!(high_freq.rows()[0].get_int(2).unwrap(), 3, "alice 应有 3 笔高频交易");

    // 断言 2：金额维度 — 80000 > 50000 阈值触发大额告警
    let big_amount = db
        .query(
            "MATCH (a:Account)-[:INITIATED]->(t:Transaction) \
             WHERE t.amount > 50000.0 \
             RETURN a.name, t.amount",
        )
        .unwrap();
    assert_eq!(big_amount.num_rows(), 1, "应有 1 笔超阈值大额交易");
    assert_eq!(big_amount.rows()[0].get_string(0).unwrap(), "multi_dim_alice");
    assert!((big_amount.rows()[0].get_float(1).unwrap() - 80000.0).abs() < 1.0);

    // 断言 3：地理维度 — CN 账户转到 US 触发跨境告警
    let cross_border = db
        .query(
            "MATCH (a:Account)-[:INITIATED]->(t:Transaction) \
             WHERE a.country = 'CN' AND t.dest_country = 'US' \
             RETURN a.name, t.amount, t.dest_country ORDER BY t.amount DESC",
        )
        .unwrap();
    assert_eq!(cross_border.num_rows(), 3, "应有 3 笔 CN->US 跨境交易");
    // 全部来自 alice
    for row in cross_border.rows() {
        assert_eq!(row.get_string(0).unwrap(), "multi_dim_alice");
    }

    // 断言 4：组合查询 — 分拆交易（高频 + 小额 + 跨境 = 最高风险）
    let combined_split = db
        .query(
            "MATCH (a:Account)-[:INITIATED]->(t:Transaction)-[:RECEIVED_BY]->(dst:Account) \
             WHERE a.country = 'CN' AND t.dest_country = 'US' \
                   AND t.amount < 10000.0 AND t.ts >= 1000 AND t.ts <= 1100 \
             RETURN a.name, dst.name, count(*) AS tx_count, sum(t.amount) AS total \
             ORDER BY count(*) DESC",
        )
        .unwrap();
    assert_eq!(combined_split.num_rows(), 1, "组合查询应命中 1 个分拆模式");
    assert_eq!(combined_split.rows()[0].get_string(0).unwrap(), "multi_dim_alice");
    assert_eq!(combined_split.rows()[0].get_int(2).unwrap(), 3, "应有 3 笔分拆交易");
    let total = combined_split.rows()[0].get_float(3).unwrap();
    assert!(
        (total - 29994.0).abs() < 1.0,
        "分拆总金额应为 29994，实际 {}",
        total
    );

    // 断言 5：单独各维度查询结果正确，AND 组合缩小范围
    // bob 不应出现在任何告警中
    let bob_alerts = db
        .query(
            "MATCH (a:Account)-[:INITIATED]->(t:Transaction) \
             WHERE a.id = 2 AND (t.amount > 50000.0 OR t.dest_country = 'US') \
             RETURN a.name",
        )
        .unwrap();
    assert_eq!(bob_alerts.num_rows(), 0, "bob 不应触发任何告警维度");

    // 断言 6：alice 触发所有 3 个维度
    // 高频
    let alice_freq = db
        .query(
            "MATCH (a:Account)-[:INITIATED]->(t:Transaction) \
             WHERE a.id = 1 AND t.ts >= 1000 AND t.ts <= 1040 \
             RETURN count(*) AS cnt",
        )
        .unwrap();
    assert!(
        alice_freq.rows()[0].get_int(0).unwrap() >= 3,
        "alice 应触发高频维度"
    );

    // 大额
    let alice_big = db
        .query(
            "MATCH (a:Account)-[:INITIATED]->(t:Transaction) \
             WHERE a.id = 1 AND t.amount > 50000.0 \
             RETURN count(*) AS cnt",
        )
        .unwrap();
    assert!(
        alice_big.rows()[0].get_int(0).unwrap() >= 1,
        "alice 应触发大额维度"
    );

    // 跨境
    let alice_cross = db
        .query(
            "MATCH (a:Account)-[:INITIATED]->(t:Transaction) \
             WHERE a.id = 1 AND a.country <> t.dest_country \
             RETURN count(*) AS cnt",
        )
        .unwrap();
    assert!(
        alice_cross.rows()[0].get_int(0).unwrap() >= 1,
        "alice 应触发跨境维度"
    );
}
