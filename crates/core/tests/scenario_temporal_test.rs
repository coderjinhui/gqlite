/// 时间序列/时态查询场景端到端测试
///
/// 场景设定：事件追踪系统，记录用户行为和系统事件
/// - 节点：Event（事件，用 INT64 存储 Unix 时间戳）、User（用户）、System（系统）
/// - 关系：TRIGGERED_BY（用户触发）、AFFECTS（影响系统）、CAUSED（因果链）、FOLLOWED_BY（时间序列）
///
/// 覆盖功能：DDL/DML、时间范围过滤、可变长路径因果链、ORDER BY 排序、
/// GROUP BY 聚合、CASE WHEN 时间窗口分组、OPTIONAL MATCH + IS NULL 根因分析
use gqlite_core::Database;

// ── 辅助函数 ────────────────────────────────────────────────

/// 创建 schema：3 个节点表 + 4 个关系表
fn create_schema(db: &Database) {
    // 节点表
    db.execute(
        "CREATE NODE TABLE Event(id INT64, name STRING, timestamp INT64, severity STRING, \
         PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE NODE TABLE User(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE System(id INT64, name STRING, PRIMARY KEY(id))").unwrap();

    // 关系表
    db.execute("CREATE REL TABLE TRIGGERED_BY(FROM Event TO User)").unwrap();
    db.execute("CREATE REL TABLE AFFECTS(FROM Event TO System)").unwrap();
    db.execute("CREATE REL TABLE CAUSED(FROM Event TO Event)").unwrap();
    db.execute("CREATE REL TABLE FOLLOWED_BY(FROM Event TO Event)").unwrap();
}

/// 插入完整数据：20 个事件、5 个用户、3 个系统，建立因果链和时间序列关系
fn insert_data(db: &Database) {
    // ── 5 个用户 ──────────────────────────────────────────────
    db.execute("CREATE (u:User {id: 1, name: 'Alice'})").unwrap();
    db.execute("CREATE (u:User {id: 2, name: 'Bob'})").unwrap();
    db.execute("CREATE (u:User {id: 3, name: 'Charlie'})").unwrap();
    db.execute("CREATE (u:User {id: 4, name: 'Diana'})").unwrap();
    db.execute("CREATE (u:User {id: 5, name: 'Eve'})").unwrap();

    // ── 3 个系统 ──────────────────────────────────────────────
    db.execute("CREATE (s:System {id: 1, name: 'WebServer'})").unwrap();
    db.execute("CREATE (s:System {id: 2, name: 'Database'})").unwrap();
    db.execute("CREATE (s:System {id: 3, name: 'MessageQueue'})").unwrap();

    // ── 20 个事件（时间戳递增，范围 1700000000 ~ 1700001000）──
    // 时间戳分布：
    //   "上午"窗口: 1700000000 ~ 1700000299
    //   "下午"窗口: 1700000300 ~ 1700000599
    //   "晚上"窗口: 1700000600 ~ 1700001000
    let events = vec![
        (1, "deploy_start", 1700000000, "info"),
        (2, "config_change", 1700000050, "info"),
        (3, "cpu_spike", 1700000100, "warning"),
        (4, "memory_leak", 1700000150, "critical"),
        (5, "disk_alert", 1700000200, "warning"),
        (6, "network_timeout", 1700000250, "critical"),
        (7, "service_restart", 1700000300, "info"),
        (8, "cache_miss", 1700000350, "warning"),
        (9, "db_slow_query", 1700000400, "warning"),
        (10, "api_error", 1700000450, "critical"),
        (11, "load_balance", 1700000500, "info"),
        (12, "ssl_renewal", 1700000550, "info"),
        (13, "backup_start", 1700000600, "info"),
        (14, "queue_overflow", 1700000650, "critical"),
        (15, "disk_cleanup", 1700000700, "info"),
        (16, "security_scan", 1700000750, "warning"),
        (17, "log_rotation", 1700000800, "info"),
        (18, "failover", 1700000850, "critical"),
        (19, "recovery", 1700000900, "info"),
        (20, "health_check", 1700000950, "info"),
    ];
    for (id, name, ts, severity) in &events {
        db.execute(&format!(
            "CREATE (e:Event {{id: {}, name: '{}', timestamp: {}, severity: '{}'}})",
            id, name, ts, severity
        ))
        .unwrap();
    }

    // ── TRIGGERED_BY 关系（用户触发的事件）──────────────────────
    // Alice 触发: 1(deploy_start), 2(config_change), 7(service_restart), 13(backup_start)
    // Bob 触发: 3(cpu_spike), 9(db_slow_query), 15(disk_cleanup)
    // Charlie 触发: 5(disk_alert), 11(load_balance), 16(security_scan)
    // Diana 触发: 12(ssl_renewal), 17(log_rotation), 20(health_check)
    // Eve 触发: 19(recovery)
    let triggered_by = vec![
        (1, 1),
        (2, 1),
        (7, 1),
        (13, 1),
        (3, 2),
        (9, 2),
        (15, 2),
        (5, 3),
        (11, 3),
        (16, 3),
        (12, 4),
        (17, 4),
        (20, 4),
        (19, 5),
    ];
    for (event_id, user_id) in triggered_by {
        db.execute(&format!(
            "MATCH (e:Event), (u:User) WHERE e.id = {} AND u.id = {} \
             CREATE (e)-[:TRIGGERED_BY]->(u)",
            event_id, user_id
        ))
        .unwrap();
    }

    // ── AFFECTS 关系（事件影响的系统）──────────────────────────
    // WebServer(1): deploy_start(1), cpu_spike(3), network_timeout(6), api_error(10), failover(18)
    // Database(2): config_change(2), memory_leak(4), db_slow_query(9), backup_start(13)
    // MessageQueue(3): queue_overflow(14), load_balance(11), cache_miss(8)
    let affects = vec![
        (1, 1),
        (3, 1),
        (6, 1),
        (10, 1),
        (18, 1),
        (2, 2),
        (4, 2),
        (9, 2),
        (13, 2),
        (14, 3),
        (11, 3),
        (8, 3),
    ];
    for (event_id, system_id) in affects {
        db.execute(&format!(
            "MATCH (e:Event), (s:System) WHERE e.id = {} AND s.id = {} \
             CREATE (e)-[:AFFECTS]->(s)",
            event_id, system_id
        ))
        .unwrap();
    }

    // ── CAUSED 关系（因果链）───────────────────────────────────
    // 因果链 A: deploy_start(1) -> config_change(2) -> cpu_spike(3) -> memory_leak(4)
    // 因果链 B: memory_leak(4) -> network_timeout(6) -> api_error(10)
    // 因果链 C: queue_overflow(14) -> failover(18) -> recovery(19)
    // 独立根因: disk_alert(5) 无 CAUSED 入边
    //          db_slow_query(9) 无 CAUSED 入边
    let caused = vec![
        (1, 2),   // deploy_start -> config_change
        (2, 3),   // config_change -> cpu_spike
        (3, 4),   // cpu_spike -> memory_leak
        (4, 6),   // memory_leak -> network_timeout
        (6, 10),  // network_timeout -> api_error
        (14, 18), // queue_overflow -> failover
        (18, 19), // failover -> recovery
    ];
    for (from_id, to_id) in caused {
        db.execute(&format!(
            "MATCH (e1:Event), (e2:Event) WHERE e1.id = {} AND e2.id = {} \
             CREATE (e1)-[:CAUSED]->(e2)",
            from_id, to_id
        ))
        .unwrap();
    }

    // ── FOLLOWED_BY 关系（时间序列顺序，按时间戳依次串联）──────
    // 1->2->3->4->5->6->7->8->9->10->11->12->13->14->15->16->17->18->19->20
    for i in 1..20 {
        db.execute(&format!(
            "MATCH (e1:Event), (e2:Event) WHERE e1.id = {} AND e2.id = {} \
             CREATE (e1)-[:FOLLOWED_BY]->(e2)",
            i,
            i + 1
        ))
        .unwrap();
    }
}

/// 构建完整的事件追踪数据库
fn setup_temporal_db() -> Database {
    let db = Database::in_memory();
    create_schema(&db);
    insert_data(&db);
    db
}

// ── 测试用例 ────────────────────────────────────────────────

// ────────────────────────────────────────────────────────────
// 1. 建表 + 插入数据验证
// ────────────────────────────────────────────────────────────

#[test]
fn temporal_schema_and_data() {
    let db = setup_temporal_db();

    // 验证 20 个事件
    let result = db.query("MATCH (e:Event) RETURN e.id").unwrap();
    assert_eq!(result.num_rows(), 20, "应有 20 个事件");

    // 验证 5 个用户
    let result = db.query("MATCH (u:User) RETURN u.id").unwrap();
    assert_eq!(result.num_rows(), 5, "应有 5 个用户");

    // 验证 3 个系统
    let result = db.query("MATCH (s:System) RETURN s.id").unwrap();
    assert_eq!(result.num_rows(), 3, "应有 3 个系统");

    // 验证 TRIGGERED_BY 关系数量
    let result = db.query("MATCH (e:Event)-[:TRIGGERED_BY]->(u:User) RETURN e.id, u.id").unwrap();
    assert_eq!(result.num_rows(), 14, "应有 14 条 TRIGGERED_BY 关系");

    // 验证 AFFECTS 关系数量
    let result = db.query("MATCH (e:Event)-[:AFFECTS]->(s:System) RETURN e.id, s.id").unwrap();
    assert_eq!(result.num_rows(), 12, "应有 12 条 AFFECTS 关系");

    // 验证 CAUSED 关系数量
    let result = db.query("MATCH (e1:Event)-[:CAUSED]->(e2:Event) RETURN e1.id, e2.id").unwrap();
    assert_eq!(result.num_rows(), 7, "应有 7 条 CAUSED 关系");

    // 验证 FOLLOWED_BY 关系数量
    let result =
        db.query("MATCH (e1:Event)-[:FOLLOWED_BY]->(e2:Event) RETURN e1.id, e2.id").unwrap();
    assert_eq!(result.num_rows(), 19, "应有 19 条 FOLLOWED_BY 关系");

    // 验证具体数据
    let deploy =
        db.query("MATCH (e:Event) WHERE e.id = 1 RETURN e.name, e.timestamp, e.severity").unwrap();
    assert_eq!(deploy.num_rows(), 1);
    assert_eq!(deploy.rows()[0].get_string(0).unwrap(), "deploy_start");
    assert_eq!(deploy.rows()[0].get_int(1).unwrap(), 1700000000);
    assert_eq!(deploy.rows()[0].get_string(2).unwrap(), "info");
}

// ────────────────────────────────────────────────────────────
// 2. 时间戳范围过滤
// ────────────────────────────────────────────────────────────

#[test]
fn events_in_time_range() {
    let db = setup_temporal_db();

    // 查询 "上午" 窗口 (1700000000 ~ 1700000299) 的事件
    // 应包含 id: 1,2,3,4,5,6 （时间戳 0,50,100,150,200,250）
    let result = db
        .query(
            "MATCH (e:Event) \
             WHERE e.timestamp >= 1700000000 AND e.timestamp <= 1700000299 \
             RETURN e.id, e.name ORDER BY e.timestamp",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 6, "上午窗口应有 6 个事件");
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 1); // deploy_start
    assert_eq!(result.rows()[5].get_int(0).unwrap(), 6); // network_timeout

    // 查询 "下午" 窗口 (1700000300 ~ 1700000599)
    // 应包含 id: 7,8,9,10,11,12
    let result = db
        .query(
            "MATCH (e:Event) \
             WHERE e.timestamp >= 1700000300 AND e.timestamp <= 1700000599 \
             RETURN e.id ORDER BY e.timestamp",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 6, "下午窗口应有 6 个事件");
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 7);
    assert_eq!(result.rows()[5].get_int(0).unwrap(), 12);

    // 查询 "晚上" 窗口 (1700000600 ~ 1700001000)
    // 应包含 id: 13,14,15,16,17,18,19,20
    let result = db
        .query(
            "MATCH (e:Event) \
             WHERE e.timestamp >= 1700000600 AND e.timestamp <= 1700001000 \
             RETURN e.id ORDER BY e.timestamp",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 8, "晚上窗口应有 8 个事件");
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 13);
    assert_eq!(result.rows()[7].get_int(0).unwrap(), 20);

    // 精确匹配单个时间戳
    let result = db
        .query(
            "MATCH (e:Event) \
             WHERE e.timestamp = 1700000400 \
             RETURN e.name",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "db_slow_query");

    // 空窗口（无结果）
    let result = db
        .query(
            "MATCH (e:Event) \
             WHERE e.timestamp >= 1700002000 AND e.timestamp <= 1700003000 \
             RETURN e.id",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 0, "超出范围的窗口应无事件");
}

// ────────────────────────────────────────────────────────────
// 3. 可变长路径追踪因果链
// ────────────────────────────────────────────────────────────

#[test]
fn event_causal_chain() {
    let db = setup_temporal_db();

    // 因果链 A: deploy_start(1) -> config_change(2) -> cpu_spike(3) -> memory_leak(4)
    //           -> network_timeout(6) -> api_error(10)
    // 从 deploy_start(1) 出发，1~5 跳可达的事件
    let result = db
        .query(
            "MATCH (e1:Event)-[:CAUSED*1..5]->(e2:Event) \
             WHERE e1.id = 1 \
             RETURN e2.id ORDER BY e2.id",
        )
        .unwrap();

    let ids: Vec<i64> = result.rows().iter().map(|r| r.get_int(0).unwrap()).collect();
    // 应能到达: 2(1跳), 3(2跳), 4(3跳), 6(4跳), 10(5跳)
    assert!(ids.contains(&2), "应到达 config_change(2), 实际: {:?}", ids);
    assert!(ids.contains(&3), "应到达 cpu_spike(3), 实际: {:?}", ids);
    assert!(ids.contains(&4), "应到达 memory_leak(4), 实际: {:?}", ids);
    assert!(ids.contains(&6), "应到达 network_timeout(6), 实际: {:?}", ids);
    assert!(ids.contains(&10), "应到达 api_error(10), 实际: {:?}", ids);

    // 从 memory_leak(4) 出发追踪后续影响（2 跳内）
    let result = db
        .query(
            "MATCH (e1:Event)-[:CAUSED*1..2]->(e2:Event) \
             WHERE e1.id = 4 \
             RETURN e2.id ORDER BY e2.id",
        )
        .unwrap();
    let ids: Vec<i64> = result.rows().iter().map(|r| r.get_int(0).unwrap()).collect();
    assert!(ids.contains(&6), "memory_leak 应导致 network_timeout(6)");
    assert!(ids.contains(&10), "memory_leak 应导致 api_error(10)");

    // 因果链 C: queue_overflow(14) -> failover(18) -> recovery(19)
    let result = db
        .query(
            "MATCH (e1:Event)-[:CAUSED*1..3]->(e2:Event) \
             WHERE e1.id = 14 \
             RETURN e2.id ORDER BY e2.id",
        )
        .unwrap();
    let ids: Vec<i64> = result.rows().iter().map(|r| r.get_int(0).unwrap()).collect();
    assert!(ids.contains(&18), "queue_overflow 应导致 failover(18)");
    assert!(ids.contains(&19), "queue_overflow 应导致 recovery(19)");
}

// ────────────────────────────────────────────────────────────
// 4. 某用户触发的事件，按时间戳降序排列
// ────────────────────────────────────────────────────────────

#[test]
fn recent_events_by_user() {
    let db = setup_temporal_db();

    // Alice(1) 触发的事件: deploy_start(1, ts=0), config_change(2, ts=50),
    //                      service_restart(7, ts=300), backup_start(13, ts=600)
    // 按时间戳降序排列
    let result = db
        .query(
            "MATCH (e:Event)-[:TRIGGERED_BY]->(u:User) \
             WHERE u.id = 1 \
             RETURN e.name, e.timestamp ORDER BY e.timestamp DESC",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 4, "Alice 应触发了 4 个事件");
    // 降序：backup_start(600), service_restart(300), config_change(50), deploy_start(0)
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "backup_start");
    assert_eq!(result.rows()[0].get_int(1).unwrap(), 1700000600);
    assert_eq!(result.rows()[1].get_string(0).unwrap(), "service_restart");
    assert_eq!(result.rows()[2].get_string(0).unwrap(), "config_change");
    assert_eq!(result.rows()[3].get_string(0).unwrap(), "deploy_start");
    assert_eq!(result.rows()[3].get_int(1).unwrap(), 1700000000);

    // Bob(2) 触发的事件: cpu_spike(3, ts=100), db_slow_query(9, ts=400), disk_cleanup(15, ts=700)
    let result = db
        .query(
            "MATCH (e:Event)-[:TRIGGERED_BY]->(u:User) \
             WHERE u.id = 2 \
             RETURN e.name, e.timestamp ORDER BY e.timestamp DESC",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 3, "Bob 应触发了 3 个事件");
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "disk_cleanup");
    assert_eq!(result.rows()[1].get_string(0).unwrap(), "db_slow_query");
    assert_eq!(result.rows()[2].get_string(0).unwrap(), "cpu_spike");

    // Eve(5) 只触发了 1 个事件: recovery(19, ts=900)
    let result = db
        .query(
            "MATCH (e:Event)-[:TRIGGERED_BY]->(u:User) \
             WHERE u.id = 5 \
             RETURN e.name, e.timestamp",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1, "Eve 应只触发了 1 个事件");
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "recovery");
}

// ────────────────────────────────────────────────────────────
// 5. GROUP BY severity + COUNT 统计各级别事件数
// ────────────────────────────────────────────────────────────

#[test]
fn event_frequency_by_severity() {
    let db = setup_temporal_db();

    // severity 分布:
    //   info: 1,2,7,11,12,13,15,17,19,20 = 10 个
    //   warning: 3,5,8,9,16 = 5 个
    //   critical: 4,6,10,14,18 = 5 个
    let result = db
        .query(
            "MATCH (e:Event) \
             RETURN e.severity, count(*) AS cnt",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 3, "应有 3 种 severity 级别");

    let mut severity_counts: std::collections::HashMap<String, i64> =
        std::collections::HashMap::new();
    for row in result.rows() {
        let severity = row.get_string(0).unwrap().to_string();
        let count = row.get_int(1).unwrap();
        severity_counts.insert(severity, count);
    }

    assert_eq!(severity_counts["info"], 10, "info 事件应有 10 个");
    assert_eq!(severity_counts["warning"], 5, "warning 事件应有 5 个");
    assert_eq!(severity_counts["critical"], 5, "critical 事件应有 5 个");

    // 总数验证
    let total: i64 = severity_counts.values().sum();
    assert_eq!(total, 20, "事件总数应为 20");
}

// ────────────────────────────────────────────────────────────
// 6. 系统影响分析（多跳因果链影响某系统的所有事件）
// ────────────────────────────────────────────────────────────

#[test]
fn system_impact_analysis() {
    let db = setup_temporal_db();

    // 直接影响 WebServer(1) 的事件: 1,3,6,10,18
    let direct = db
        .query(
            "MATCH (e:Event)-[:AFFECTS]->(s:System) \
             WHERE s.id = 1 \
             RETURN e.id ORDER BY e.id",
        )
        .unwrap();
    assert_eq!(direct.num_rows(), 5, "直接影响 WebServer 的事件应有 5 个");
    let direct_ids: Vec<i64> = direct.rows().iter().map(|r| r.get_int(0).unwrap()).collect();
    assert_eq!(direct_ids, vec![1, 3, 6, 10, 18]);

    // 追踪影响 WebServer 的所有因果链上游事件（多跳）
    // 直接影响的事件: 1,3,6,10,18
    // 事件 3(cpu_spike) 的因果上游: 2(config_change)->3, 1(deploy_start)->2->3
    //   即 1,2 也是间接原因
    // 事件 6(network_timeout) 的上游: 4->6, 3->4->6, 2->3->4->6, 1->2->3->4->6
    //   即 4,3,2,1 都是间接原因
    // 事件 10(api_error) 的上游: 6->10, 4->6->10, ... 追溯到 1
    // 事件 18(failover) 的上游: 14->18
    //
    // 通过反向追踪：找所有导致"影响 WebServer 的事件"的上游事件
    // (cause)-[:CAUSED*1..5]->(effect)-[:AFFECTS]->(System)
    let indirect = db
        .query(
            "MATCH (cause:Event)-[:CAUSED*1..5]->(effect:Event)-[:AFFECTS]->(s:System) \
             WHERE s.id = 1 \
             RETURN DISTINCT cause.id ORDER BY cause.id",
        )
        .unwrap();

    let indirect_ids: Vec<i64> = indirect.rows().iter().map(|r| r.get_int(0).unwrap()).collect();
    // 间接原因事件应包含因果链上游节点
    // deploy_start(1) -> config_change(2) -> cpu_spike(3) -> memory_leak(4) -> network_timeout(6)
    // 其中 3,6,10,18 直接影响 WebServer
    // 上游: 1 导致 3(间接通过2), 2 导致 3, 1/2/3 导致 6(通过4), 4 导致 6
    // queue_overflow(14) 导致 failover(18)
    assert!(indirect_ids.contains(&1), "deploy_start 应是间接原因");
    assert!(indirect_ids.contains(&2), "config_change 应是间接原因");
    assert!(indirect_ids.contains(&14), "queue_overflow 应是间接原因");

    // 直接影响 Database(2) 的事件: 2,4,9,13
    let db_direct = db
        .query(
            "MATCH (e:Event)-[:AFFECTS]->(s:System) \
             WHERE s.id = 2 \
             RETURN e.id ORDER BY e.id",
        )
        .unwrap();
    let db_ids: Vec<i64> = db_direct.rows().iter().map(|r| r.get_int(0).unwrap()).collect();
    assert_eq!(db_ids, vec![2, 4, 9, 13]);
}

// ────────────────────────────────────────────────────────────
// 7. CASE WHEN 按时间窗口分组统计
// ────────────────────────────────────────────────────────────

#[test]
fn time_window_aggregation() {
    let db = setup_temporal_db();

    // 将事件按时间窗口分为 上午/下午/晚上，并统计每个窗口的事件数
    let result = db
        .query(
            "MATCH (e:Event) \
             RETURN \
                CASE \
                    WHEN e.timestamp >= 1700000600 THEN 'evening' \
                    WHEN e.timestamp >= 1700000300 THEN 'afternoon' \
                    ELSE 'morning' \
                END AS time_window, \
                count(*) AS cnt",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 3, "应有 3 个时间窗口");

    let mut window_counts: std::collections::HashMap<String, i64> =
        std::collections::HashMap::new();
    for row in result.rows() {
        let window = row.get_string(0).unwrap().to_string();
        let count = row.get_int(1).unwrap();
        window_counts.insert(window, count);
    }

    assert_eq!(window_counts["morning"], 6, "上午窗口应有 6 个事件");
    assert_eq!(window_counts["afternoon"], 6, "下午窗口应有 6 个事件");
    assert_eq!(window_counts["evening"], 8, "晚上窗口应有 8 个事件");

    // 验证各窗口内的 critical 事件数
    // morning critical: 4(memory_leak), 6(network_timeout) = 2
    // afternoon critical: 10(api_error) = 1
    // evening critical: 14(queue_overflow), 18(failover) = 2
    let result = db
        .query(
            "MATCH (e:Event) \
             WHERE e.severity = 'critical' \
             RETURN \
                CASE \
                    WHEN e.timestamp >= 1700000600 THEN 'evening' \
                    WHEN e.timestamp >= 1700000300 THEN 'afternoon' \
                    ELSE 'morning' \
                END AS time_window, \
                count(*) AS cnt",
        )
        .unwrap();

    let mut critical_counts: std::collections::HashMap<String, i64> =
        std::collections::HashMap::new();
    for row in result.rows() {
        let window = row.get_string(0).unwrap().to_string();
        let count = row.get_int(1).unwrap();
        critical_counts.insert(window, count);
    }

    assert_eq!(critical_counts.get("morning").copied().unwrap_or(0), 2);
    assert_eq!(critical_counts.get("afternoon").copied().unwrap_or(0), 1);
    assert_eq!(critical_counts.get("evening").copied().unwrap_or(0), 2);
}

// ────────────────────────────────────────────────────────────
// 8. FOLLOWED_BY 链验证事件时间顺序
// ────────────────────────────────────────────────────────────

#[test]
fn event_sequence_order() {
    let db = setup_temporal_db();

    // 验证所有 FOLLOWED_BY 关系中，前置事件的时间戳 < 后续事件的时间戳
    let result = db
        .query(
            "MATCH (e1:Event)-[:FOLLOWED_BY]->(e2:Event) \
             RETURN e1.id, e1.timestamp, e2.id, e2.timestamp \
             ORDER BY e1.timestamp",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 19, "应有 19 条 FOLLOWED_BY 关系");

    for row in result.rows() {
        let e1_id = row.get_int(0).unwrap();
        let e1_ts = row.get_int(1).unwrap();
        let e2_id = row.get_int(2).unwrap();
        let e2_ts = row.get_int(3).unwrap();
        assert!(
            e1_ts < e2_ts,
            "事件 {} (ts={}) 应在事件 {} (ts={}) 之前",
            e1_id,
            e1_ts,
            e2_id,
            e2_ts
        );
    }

    // 通过可变长路径验证序列连通性：从第 1 个事件应能到达最后一个事件
    // deploy_start(1) -> ... -> health_check(20)，需要 19 跳
    // 使用 shortestPath 来验证
    let result = db
        .query(
            "MATCH (a:Event), (b:Event), \
             p = shortestPath((a)-[:FOLLOWED_BY*..20]->(b)) \
             WHERE a.id = 1 AND b.id = 20 \
             RETURN length(p) AS len",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1, "应找到从事件 1 到事件 20 的路径");
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 19, "路径长度应为 19");

    // 验证部分序列的连续性
    // 从 event 5 到 event 10 应经过 5 步
    let result = db
        .query(
            "MATCH (a:Event), (b:Event), \
             p = shortestPath((a)-[:FOLLOWED_BY*..10]->(b)) \
             WHERE a.id = 5 AND b.id = 10 \
             RETURN length(p) AS len",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 5);
}

// ────────────────────────────────────────────────────────────
// 9. 并发事件检测（相同时间窗口内的事件）
// ────────────────────────────────────────────────────────────

#[test]
fn concurrent_events() {
    let db = setup_temporal_db();

    // 查找时间戳差值在 100 以内的事件对（"近乎同时"发生）
    // 使用自连接 + 时间差过滤
    // 只选 e1.id < e2.id 避免重复对和自环
    let result = db
        .query(
            "MATCH (e1:Event), (e2:Event) \
             WHERE e1.id < e2.id \
             AND e2.timestamp - e1.timestamp >= 0 \
             AND e2.timestamp - e1.timestamp <= 100 \
             RETURN e1.id, e2.id, e1.timestamp, e2.timestamp \
             ORDER BY e1.id, e2.id",
        )
        .unwrap();

    // 时间戳间隔 50，100 以内的相邻对:
    // (1,2): 0-50=50, (2,3): 50-100=50, (3,4): 100-150=50
    // (4,5): 150-200=50, (5,6): 200-250=50, (6,7): 250-300=50
    // etc.
    // 也包括跨一个的对（差=100）：(1,3), (2,4), (3,5), ...
    assert!(result.num_rows() > 0, "应有多对近乎同时发生的事件");

    // 验证找到的对确实在时间窗口内
    for row in result.rows() {
        let ts1 = row.get_int(2).unwrap();
        let ts2 = row.get_int(3).unwrap();
        let diff = ts2 - ts1;
        assert!((0..=100).contains(&diff), "事件对的时间差应在 0~100 之内，实际 {}", diff);
    }

    // 具体验证：deploy_start(1, ts=0) 和 config_change(2, ts=50) 应是一对
    let pair_check = db
        .query(
            "MATCH (e1:Event), (e2:Event) \
             WHERE e1.id = 1 AND e2.id = 2 \
             RETURN e2.timestamp - e1.timestamp AS diff",
        )
        .unwrap();
    assert_eq!(pair_check.num_rows(), 1);
    assert_eq!(pair_check.rows()[0].get_int(0).unwrap(), 50);

    // 统计每个小窗口（200 区间内）有多少事件
    // 窗口 1700000000~1700000199: id 1,2,3,4 = 4 个事件
    let window1 = db
        .query(
            "MATCH (e:Event) \
             WHERE e.timestamp >= 1700000000 AND e.timestamp <= 1700000199 \
             RETURN count(*) AS cnt",
        )
        .unwrap();
    assert_eq!(window1.rows()[0].get_int(0).unwrap(), 4);
}

// ────────────────────────────────────────────────────────────
// 10. 根因分析：找没有 CAUSED 入边的事件
// ────────────────────────────────────────────────────────────

#[test]
fn root_cause_analysis() {
    let db = setup_temporal_db();

    // 根因事件 = 参与因果链但没有 CAUSED 入边的事件
    // 因果链中的所有事件: 1,2,3,4,6,10,14,18,19
    // 有 CAUSED 入边的: 2,3,4,6,10,18,19
    // 根因事件（有出边但无入边）: 1(deploy_start), 14(queue_overflow)
    //
    // 策略：找所有有 CAUSED 出边的事件，然后排除有 CAUSED 入边的
    // 使用 OPTIONAL MATCH + IS NULL 模式

    // 先找所有在 CAUSED 链中的事件（有出边的）
    let has_caused_out = db
        .query(
            "MATCH (e:Event)-[:CAUSED]->(e2:Event) \
             RETURN DISTINCT e.id ORDER BY e.id",
        )
        .unwrap();
    let out_ids: Vec<i64> = has_caused_out.rows().iter().map(|r| r.get_int(0).unwrap()).collect();
    // 有 CAUSED 出边的事件: 1,2,3,4,6,14,18
    assert!(out_ids.contains(&1), "deploy_start 应有 CAUSED 出边");
    assert!(out_ids.contains(&14), "queue_overflow 应有 CAUSED 出边");

    // 找有 CAUSED 入边的事件
    let has_caused_in = db
        .query(
            "MATCH (e1:Event)-[:CAUSED]->(e:Event) \
             RETURN DISTINCT e.id ORDER BY e.id",
        )
        .unwrap();
    let in_ids: Vec<i64> = has_caused_in.rows().iter().map(|r| r.get_int(0).unwrap()).collect();
    // 有 CAUSED 入边的事件: 2,3,4,6,10,18,19
    assert!(in_ids.contains(&2));
    assert!(in_ids.contains(&10));
    assert!(in_ids.contains(&19));

    // 根因事件 = 有出边但无入边
    let root_causes: Vec<i64> = out_ids.iter().filter(|id| !in_ids.contains(id)).copied().collect();
    assert_eq!(root_causes.len(), 2, "应有 2 个根因事件");
    assert!(root_causes.contains(&1), "deploy_start(1) 应是根因");
    assert!(root_causes.contains(&14), "queue_overflow(14) 应是根因");

    // 使用 OPTIONAL MATCH + IS NULL 在单条查询中找根因事件
    // 找参与因果链（有 CAUSED 出边）且没有 CAUSED 入边的事件
    // 注意：OPTIONAL MATCH 模式从已绑定变量 (e) 出发，使用反向边 <-[:CAUSED]-
    let result = db
        .query(
            "MATCH (e:Event)-[:CAUSED]->(downstream:Event) \
             OPTIONAL MATCH (e)<-[:CAUSED]-(upstream:Event) \
             WHERE upstream.id IS NULL \
             RETURN DISTINCT e.id, e.name ORDER BY e.id",
        )
        .unwrap();

    let root_ids: Vec<i64> = result.rows().iter().map(|r| r.get_int(0).unwrap()).collect();
    assert_eq!(root_ids.len(), 2, "单查询根因分析应找到 2 个根因事件");
    assert_eq!(root_ids[0], 1, "第一个根因应是 deploy_start(1)");
    assert_eq!(root_ids[1], 14, "第二个根因应是 queue_overflow(14)");

    // 验证根因事件的名称
    assert_eq!(result.rows()[0].get_string(1).unwrap(), "deploy_start");
    assert_eq!(result.rows()[1].get_string(1).unwrap(), "queue_overflow");

    // 验证根因影响范围
    // deploy_start(1) 的影响链: 1->2->3->4->6->10，共影响 5 个下游事件
    let chain_a = db
        .query(
            "MATCH (e:Event)-[:CAUSED*1..5]->(downstream:Event) \
             WHERE e.id = 1 \
             RETURN count(downstream) AS impact_count",
        )
        .unwrap();
    assert_eq!(chain_a.rows()[0].get_int(0).unwrap(), 5, "deploy_start 应影响 5 个下游事件");

    // queue_overflow(14) 的影响链: 14->18->19，共影响 2 个下游事件
    let chain_c = db
        .query(
            "MATCH (e:Event)-[:CAUSED*1..5]->(downstream:Event) \
             WHERE e.id = 14 \
             RETURN count(downstream) AS impact_count",
        )
        .unwrap();
    assert_eq!(chain_c.rows()[0].get_int(0).unwrap(), 2, "queue_overflow 应影响 2 个下游事件");
}
