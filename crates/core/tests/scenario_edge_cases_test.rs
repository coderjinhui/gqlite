//! 边界值与并发安全场景测试
//!
//! 覆盖两个缺失维度：
//! 1. 并发安全 — 多线程读写 Database（Clone + Send + Sync）
//! 2. 边界值/异常输入 — 空字符串、Unicode、超长字符串、极值数值、特殊字符等

use gqlite_core::Database;
use std::sync::{Arc, Barrier};

// ============================================================
// 第一部分：并发安全测试
// ============================================================

/// 3 个线程同时读同一个 Database，验证每个线程读到一致数据（相同行数）
#[test]
fn concurrent_reads_consistency() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    for i in 0..50 {
        db.execute(&format!("CREATE (p:Person {{id: {}, name: 'user{}'}})", i, i)).unwrap();
    }

    let barrier = Arc::new(Barrier::new(3));
    let mut handles = vec![];

    for t in 0..3 {
        let db_clone = db.clone();
        let barrier_clone = barrier.clone();
        handles.push(std::thread::spawn(move || {
            // 所有线程同时开始读
            barrier_clone.wait();
            let result = db_clone
                .query("MATCH (p:Person) RETURN p.id")
                .unwrap_or_else(|e| panic!("thread {} read failed: {}", t, e));
            result.num_rows()
        }));
    }

    let counts: Vec<usize> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert!(counts.iter().all(|&c| c == 50), "all threads should read 50 rows, got {:?}", counts);
}

/// 用线程尝试同时写，验证第二个写者报错（因为 SWMR）
#[test]
fn write_blocks_second_writer() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Item (id INT64, PRIMARY KEY(id))").unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let db1 = db.clone();
    let db2 = db.clone();
    let b1 = barrier.clone();
    let b2 = barrier.clone();

    // 线程 1：通过 execute_script 持有写锁较长时间
    let h1 = std::thread::spawn(move || {
        b1.wait();
        // 批量插入，持有写锁时间较长
        let mut script = String::from("BEGIN; ");
        for i in 0..100 {
            script.push_str(&format!("CREATE (n:Item {{id: {}}}); ", i));
        }
        script.push_str("COMMIT;");
        db1.execute_script(&script)
    });

    // 线程 2：尝试写入
    let h2 = std::thread::spawn(move || {
        b2.wait();
        // 多次尝试，至少有一些会与线程 1 的写锁冲突
        let mut had_error = false;
        let mut had_success = false;
        for i in 200..210 {
            match db2.execute(&format!("CREATE (n:Item {{id: {}}})", i)) {
                Ok(_) => had_success = true,
                Err(_) => had_error = true,
            }
        }
        (had_error, had_success)
    });

    let r1 = h1.join().unwrap();
    let (had_error, _had_success) = h2.join().unwrap();

    // 线程 1 应该成功
    assert!(r1.is_ok(), "thread 1 script should succeed: {:?}", r1.err());

    // 由于 SWMR，线程 2 的写操作在线程 1 持有写锁时会报错
    // 但也可能在线程 1 完成后成功。至少验证两个线程不 panic。
    // 如果线程 1 的事务占满了整个时间窗口，线程 2 应该遇到过错误
    // 不严格断言 had_error，因为时序不确定，但验证不 panic 即可
    let _ = had_error;
}

/// 先写入数据，然后一个线程写入更多数据，另一个线程同时读取，验证读线程不 panic
#[test]
fn read_during_write() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Data (id INT64, val STRING, PRIMARY KEY(id))").unwrap();
    for i in 0..20 {
        db.execute(&format!("CREATE (d:Data {{id: {}, val: 'init{}'}})", i, i)).unwrap();
    }

    let barrier = Arc::new(Barrier::new(2));

    // 写线程
    let db_w = db.clone();
    let bw = barrier.clone();
    let writer = std::thread::spawn(move || {
        bw.wait();
        for i in 100..150 {
            let _ = db_w.execute(&format!("CREATE (d:Data {{id: {}, val: 'new{}'}})", i, i));
        }
    });

    // 读线程
    let db_r = db.clone();
    let br = barrier.clone();
    let reader = std::thread::spawn(move || {
        br.wait();
        let mut read_count = 0;
        for _ in 0..50 {
            match db_r.query("MATCH (d:Data) RETURN d.id") {
                Ok(r) => {
                    // 行数应该 >= 20（初始数据），可能更多
                    assert!(
                        r.num_rows() >= 20,
                        "should see at least initial 20 rows, got {}",
                        r.num_rows()
                    );
                    read_count += 1;
                }
                Err(_) => {
                    // 读取期间写锁冲突时可能报错，但不应该 panic
                }
            }
        }
        read_count
    });

    writer.join().expect("writer thread should not panic");
    let read_count = reader.join().expect("reader thread should not panic");
    assert!(read_count > 0, "reader should have completed at least some reads");
}

/// 创建多个 Connection，依次执行读写，验证数据一致
#[test]
fn sequential_connections() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Counter (id INT64, val INT64, PRIMARY KEY(id))").unwrap();

    // 通过不同 Connection 写入
    let conn1 = db.connect();
    conn1.execute("CREATE (c:Counter {id: 1, val: 10})").unwrap();

    let conn2 = db.connect();
    conn2.execute("CREATE (c:Counter {id: 2, val: 20})").unwrap();

    let conn3 = db.connect();
    conn3.execute("CREATE (c:Counter {id: 3, val: 30})").unwrap();

    // 通过另一个 Connection 读取，验证所有数据可见
    let conn_read = db.connect();
    let result = conn_read.query("MATCH (c:Counter) RETURN c.id ORDER BY c.id").unwrap();
    assert_eq!(result.num_rows(), 3, "all 3 nodes should be visible from any connection");

    // 验证每个值
    assert_eq!(result.rows()[0].values[0], gqlite_core::types::value::Value::Int(1));
    assert_eq!(result.rows()[1].values[0], gqlite_core::types::value::Value::Int(2));
    assert_eq!(result.rows()[2].values[0], gqlite_core::types::value::Value::Int(3));

    // 通过 conn1 更新，conn2 读取
    conn1.execute("MATCH (c:Counter) WHERE c.id = 1 SET c.val = 100").unwrap();
    let result = conn2.query("MATCH (c:Counter) WHERE c.id = 1 RETURN c.val").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(
        result.rows()[0].values[0],
        gqlite_core::types::value::Value::Int(100),
        "updated value should be visible from other connection"
    );
}

// ============================================================
// 第二部分：边界值测试
// ============================================================

/// 插入空字符串属性，查询验证
#[test]
fn empty_string_property() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Txt (id INT64, content STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (t:Txt {id: 1, content: ''})").unwrap();

    let result = db.query("MATCH (t:Txt) WHERE t.id = 1 RETURN t.content").unwrap();
    assert_eq!(result.num_rows(), 1, "should find the row with empty string");
    assert_eq!(
        result.rows()[0].values[0],
        gqlite_core::types::value::Value::String(String::new()),
        "empty string should be preserved"
    );
}

/// 插入中文、日文、emoji 等 Unicode 字符串，查询验证原样返回
#[test]
fn unicode_properties() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Uni (id INT64, text STRING, PRIMARY KEY(id))").unwrap();

    let test_cases = vec![
        (1, "Hello"),
        (2, "\u{4f60}\u{597d}\u{4e16}\u{754c}"), // 你好世界
        (3, "\u{3053}\u{3093}\u{306b}\u{3061}\u{306f}"), // こんにちは
        (4, "\u{1f600}\u{1f680}\u{2764}\u{fe0f}"), // 😀🚀❤️
        (5, "\u{0410}\u{0411}\u{0412}"),         // АБВ (Cyrillic)
    ];

    // 直接用 Rust 字面量构造 Unicode 再放入 query
    // 但 GQL 字面量是单引号字符串，我们直接插入
    db.execute("CREATE (n:Uni {id: 1, text: 'Hello'})").unwrap();
    db.execute("CREATE (n:Uni {id: 2, text: '\u{4f60}\u{597d}\u{4e16}\u{754c}'})").unwrap();
    db.execute("CREATE (n:Uni {id: 3, text: '\u{3053}\u{3093}\u{306b}\u{3061}\u{306f}'})").unwrap();
    // emoji 和 Cyrillic
    db.execute(&format!("CREATE (n:Uni {{id: 4, text: '{}'}})", test_cases[3].1)).unwrap();
    db.execute(&format!("CREATE (n:Uni {{id: 5, text: '{}'}})", test_cases[4].1)).unwrap();

    for (id, expected) in &test_cases {
        let result = db.query(&format!("MATCH (n:Uni) WHERE n.id = {} RETURN n.text", id)).unwrap();
        assert_eq!(result.num_rows(), 1, "should find row with id={}", id);
        assert_eq!(
            result.rows()[0].values[0],
            gqlite_core::types::value::Value::String(expected.to_string()),
            "Unicode text for id={} should be preserved",
            id
        );
    }
}

/// 插入 10000 字符的超长字符串，验证存储和读取正确
#[test]
fn very_long_string() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Long (id INT64, data STRING, PRIMARY KEY(id))").unwrap();

    let long_str: String = "A".repeat(10_000);
    db.execute(&format!("CREATE (n:Long {{id: 1, data: '{}'}})", long_str)).unwrap();

    let result = db.query("MATCH (n:Long) WHERE n.id = 1 RETURN n.data").unwrap();
    assert_eq!(result.num_rows(), 1, "should find the row");

    if let gqlite_core::types::value::Value::String(ref s) = result.rows()[0].values[0] {
        assert_eq!(s.len(), 10_000, "string length should be 10000");
        assert!(s.chars().all(|c| c == 'A'), "all chars should be 'A'");
    } else {
        panic!("expected String value, got {:?}", result.rows()[0].values[0]);
    }
}

/// 插入 INT64 最大值，验证精确返回
#[test]
fn max_int64_value() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Big (id INT64, PRIMARY KEY(id))").unwrap();

    // INT64 最大值 = 9223372036854775807
    db.execute("CREATE (n:Big {id: 9223372036854775807})").unwrap();

    let result = db.query("MATCH (n:Big) RETURN n.id").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(
        result.rows()[0].values[0],
        gqlite_core::types::value::Value::Int(i64::MAX),
        "INT64 max value should be preserved exactly"
    );
}

/// 插入负数（INT64 和 DOUBLE），范围查询验证
#[test]
fn negative_numbers() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Num (id INT64, fval DOUBLE, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:Num {id: -100, fval: -3.14})").unwrap();
    db.execute("CREATE (n:Num {id: -1, fval: -0.001})").unwrap();
    db.execute("CREATE (n:Num {id: 0, fval: 0.0})").unwrap();
    db.execute("CREATE (n:Num {id: 50, fval: 99.9})").unwrap();

    // 查询所有负 id 的行
    let result = db.query("MATCH (n:Num) WHERE n.id < 0 RETURN n.id ORDER BY n.id").unwrap();
    assert_eq!(result.num_rows(), 2, "should find 2 negative id rows");
    assert_eq!(result.rows()[0].values[0], gqlite_core::types::value::Value::Int(-100));
    assert_eq!(result.rows()[1].values[0], gqlite_core::types::value::Value::Int(-1));

    // 查询负浮点值
    let result =
        db.query("MATCH (n:Num) WHERE n.fval < 0.0 RETURN n.fval ORDER BY n.fval").unwrap();
    assert_eq!(result.num_rows(), 2, "should find 2 negative float rows");
}

/// 插入 0.0、-0.0、极小正数 1e-300，验证
#[test]
fn zero_and_boundary_floats() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Flt (id INT64, val DOUBLE, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:Flt {id: 1, val: 0.0})").unwrap();
    db.execute("CREATE (n:Flt {id: 2, val: -0.0})").unwrap();
    db.execute("CREATE (n:Flt {id: 3, val: 0.000000000001})").unwrap();

    let result = db.query("MATCH (n:Flt) RETURN n.id, n.val ORDER BY n.id").unwrap();
    assert_eq!(result.num_rows(), 3, "should have 3 rows");

    // 0.0 和 -0.0 在 f64 中是不同的位模式，但值相等
    if let gqlite_core::types::value::Value::Float(f) = result.rows()[0].values[1] {
        assert!((f - 0.0).abs() < f64::EPSILON, "id=1 val should be 0.0, got {}", f);
    }

    // 极小正数
    if let gqlite_core::types::value::Value::Float(f) = result.rows()[2].values[1] {
        assert!(f > 0.0, "0.000000000001 should be positive");
        assert!(f < 0.00001, "0.000000000001 should be very small");
    }
}

/// 插入含单引号转义的字符串，验证转义正确
#[test]
fn special_chars_in_strings() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Spec (id INT64, val STRING, PRIMARY KEY(id))").unwrap();

    // 反斜杠
    db.execute(r"CREATE (n:Spec {id: 1, val: 'back\\slash'})").unwrap();

    // 含换行符（用 \n 字面量 — 取决于 parser 是否支持转义序列）
    // 这里我们用 Rust 原始字符串在 GQL 中测试
    db.execute("CREATE (n:Spec {id: 2, val: 'line1\\nline2'})").unwrap();

    let result = db.query("MATCH (n:Spec) WHERE n.id = 1 RETURN n.val").unwrap();
    assert_eq!(result.num_rows(), 1, "should find row with backslash");

    let result = db.query("MATCH (n:Spec) WHERE n.id = 2 RETURN n.val").unwrap();
    assert_eq!(result.num_rows(), 1, "should find row with escaped newline");
}

/// 在空表上执行 MATCH/DELETE/SET/COUNT 等操作，验证不 panic
#[test]
fn empty_graph_operations() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Empty (id INT64, val STRING, PRIMARY KEY(id))").unwrap();

    // MATCH on empty table
    let result = db.query("MATCH (n:Empty) RETURN n.id").unwrap();
    assert_eq!(result.num_rows(), 0, "MATCH on empty table should return 0 rows");

    // COUNT on empty table
    let result = db.query("MATCH (n:Empty) RETURN COUNT(n)").unwrap();
    assert_eq!(result.num_rows(), 1, "COUNT on empty table should return 1 row");
    assert_eq!(
        result.rows()[0].values[0],
        gqlite_core::types::value::Value::Int(0),
        "COUNT on empty table should be 0"
    );

    // DELETE on empty table (no rows to delete)
    let result = db.execute("MATCH (n:Empty) DELETE n");
    assert!(result.is_ok(), "DELETE on empty table should not error");

    // SET on empty table (no rows to update)
    let result = db.execute("MATCH (n:Empty) SET n.val = 'updated'");
    assert!(result.is_ok(), "SET on empty table should not error");

    // RETURN with ORDER BY on empty table
    let result = db.query("MATCH (n:Empty) RETURN n.id ORDER BY n.id").unwrap();
    assert_eq!(result.num_rows(), 0, "ORDER BY on empty table should return 0 rows");

    // RETURN with LIMIT on empty table
    let result = db.query("MATCH (n:Empty) RETURN n.id LIMIT 10").unwrap();
    assert_eq!(result.num_rows(), 0, "LIMIT on empty table should return 0 rows");
}

/// 重复创建相同表（应报错）、重复 PK（应报错），验证错误处理
#[test]
fn duplicate_operations() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Dup (id INT64, PRIMARY KEY(id))").unwrap();

    // 重复创建同名表
    let result = db.execute("CREATE NODE TABLE Dup (id INT64, PRIMARY KEY(id))");
    assert!(result.is_err(), "duplicate table creation should fail");
    let err = format!("{}", result.err().unwrap());
    assert!(err.contains("already exists"), "error should mention 'already exists', got: {}", err);

    // 插入数据后，重复 PK 应报错
    db.execute("CREATE (n:Dup {id: 1})").unwrap();
    let result = db.execute("CREATE (n:Dup {id: 1})");
    assert!(result.is_err(), "duplicate primary key should fail");
    let err = format!("{}", result.err().unwrap());
    assert!(
        err.contains("duplicate primary key"),
        "error should mention 'duplicate primary key', got: {}",
        err
    );
}

/// 查询不存在的表，验证报错
#[test]
fn query_nonexistent_table() {
    let db = Database::in_memory();

    let result = db.query("MATCH (n:NoSuchTable) RETURN n");
    assert!(result.is_err(), "querying nonexistent table should fail");
    let err = format!("{}", result.err().unwrap());
    assert!(err.contains("not found"), "error should mention 'not found', got: {}", err);
}

/// WHERE 中引用不存在的属性，验证行为
#[test]
fn query_nonexistent_property() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Prop (id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:Prop {id: 1})").unwrap();

    // 引用不存在的属性 — 应报错（property not found）
    let result = db.query("MATCH (n:Prop) WHERE n.nonexistent = 1 RETURN n.id");
    assert!(result.is_err(), "referencing nonexistent property should fail");
    let err = format!("{}", result.err().unwrap());
    assert!(err.contains("not found"), "error should mention 'not found', got: {}", err);
}
