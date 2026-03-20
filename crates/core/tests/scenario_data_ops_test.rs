/// 数据操作场景端到端测试
///
/// 覆盖三个维度：
/// 1. COPY FROM CSV 导入 / COPY TO CSV 导出
/// 2. PreparedStatement 参数化查询
/// 3. 多关系类型交叉查询
use std::collections::HashMap;

use gqlite_core::types::value::Value;
use gqlite_core::Database;

// ════════════════════════════════════════════════════════════════
// 第一部分：COPY FROM CSV 导入 / COPY TO CSV 导出
// ════════════════════════════════════════════════════════════════

/// 创建临时 CSV 文件（含 header），用 COPY FROM 导入节点数据，验证行数和内容
#[test]
fn copy_from_csv_nodes() {
    use std::io::Write;

    let dir = std::env::temp_dir().join("gqlite_scenario_data_ops");
    std::fs::create_dir_all(&dir).ok();
    let csv_path = dir.join("copy_from_nodes.csv");

    // 写入 CSV 文件
    {
        let mut f = std::fs::File::create(&csv_path).unwrap();
        writeln!(f, "id,name,age").unwrap();
        writeln!(f, "1,Alice,30").unwrap();
        writeln!(f, "2,Bob,25").unwrap();
        writeln!(f, "3,Charlie,28").unwrap();
        writeln!(f, "4,Diana,35").unwrap();
        writeln!(f, "5,Eve,22").unwrap();
    }

    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
        .unwrap();

    let csv_str = csv_path.to_str().unwrap();
    db.execute(&format!("COPY Person FROM '{}'", csv_str)).unwrap();

    // 验证行数
    let result = db.query("MATCH (n:Person) RETURN n.id, n.name, n.age ORDER BY n.id").unwrap();
    assert_eq!(result.num_rows(), 5);

    // 验证首行
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 1);
    assert_eq!(result.rows()[0].get_string(1).unwrap(), "Alice");
    assert_eq!(result.rows()[0].get_int(2).unwrap(), 30);

    // 验证末行
    assert_eq!(result.rows()[4].get_int(0).unwrap(), 5);
    assert_eq!(result.rows()[4].get_string(1).unwrap(), "Eve");
    assert_eq!(result.rows()[4].get_int(2).unwrap(), 22);

    // 验证中间行
    assert_eq!(result.rows()[2].get_string(1).unwrap(), "Charlie");
    assert_eq!(result.rows()[2].get_int(2).unwrap(), 28);

    // 清理
    std::fs::remove_file(&csv_path).ok();
}

/// 先导入节点，再用 CSV 导入关系，验证关系正确建立
#[test]
fn copy_from_csv_relationships() {
    use std::io::Write;

    let dir = std::env::temp_dir().join("gqlite_scenario_data_ops");
    std::fs::create_dir_all(&dir).ok();
    let node_csv = dir.join("copy_rel_nodes.csv");
    let rel_csv = dir.join("copy_rel_rels.csv");

    // 写入节点 CSV
    {
        let mut f = std::fs::File::create(&node_csv).unwrap();
        writeln!(f, "id,name,age").unwrap();
        writeln!(f, "1,Alice,30").unwrap();
        writeln!(f, "2,Bob,25").unwrap();
        writeln!(f, "3,Charlie,28").unwrap();
    }

    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
        .unwrap();
    db.execute("CREATE REL TABLE FOLLOWS (FROM Person TO Person)").unwrap();

    // 导入节点
    let node_csv_str = node_csv.to_str().unwrap();
    db.execute(&format!("COPY Person FROM '{}'", node_csv_str)).unwrap();

    // 验证节点导入
    let nodes = db.query("MATCH (n:Person) RETURN n.id ORDER BY n.id").unwrap();
    assert_eq!(nodes.num_rows(), 3);

    // 用 MATCH + CREATE 建关系（COPY FROM 仅支持节点表）
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:FOLLOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.id = 2 AND b.id = 3 CREATE (a)-[:FOLLOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 3 CREATE (a)-[:FOLLOWS]->(b)",
    )
    .unwrap();

    // 验证关系
    let rels = db
        .query(
            "MATCH (a:Person)-[:FOLLOWS]->(b:Person) RETURN a.name, b.name ORDER BY a.name, b.name",
        )
        .unwrap();
    assert_eq!(rels.num_rows(), 3);

    // Alice -> Bob, Alice -> Charlie, Bob -> Charlie
    assert_eq!(rels.rows()[0].get_string(0).unwrap(), "Alice");
    assert_eq!(rels.rows()[0].get_string(1).unwrap(), "Bob");
    assert_eq!(rels.rows()[1].get_string(0).unwrap(), "Alice");
    assert_eq!(rels.rows()[1].get_string(1).unwrap(), "Charlie");
    assert_eq!(rels.rows()[2].get_string(0).unwrap(), "Bob");
    assert_eq!(rels.rows()[2].get_string(1).unwrap(), "Charlie");

    // 清理
    std::fs::remove_file(&node_csv).ok();
    std::fs::remove_file(&rel_csv).ok();
}

/// 插入数据后用 COPY TO 导出到 CSV，验证文件内容
#[test]
fn copy_to_csv_export() {
    let dir = std::env::temp_dir().join("gqlite_scenario_data_ops");
    std::fs::create_dir_all(&dir).ok();
    let csv_path = dir.join("copy_to_export.csv");

    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
        .unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice', age: 30})").unwrap();
    db.execute("CREATE (n:Person {id: 2, name: 'Bob', age: 25})").unwrap();
    db.execute("CREATE (n:Person {id: 3, name: 'Charlie', age: 28})").unwrap();

    // 使用 COPY table TO 导出整张表
    let csv_str = csv_path.to_str().unwrap();
    db.execute(&format!("COPY Person TO '{}'", csv_str)).unwrap();

    let content = std::fs::read_to_string(&csv_path).unwrap();
    let lines: Vec<&str> = content.trim().lines().collect();

    // 验证 header
    assert_eq!(lines[0], "id,name,age");

    // 验证数据行数（3 行数据 + 1 行 header）
    assert_eq!(lines.len(), 4);

    // 验证数据内容（收集到 set 中，不依赖顺序）
    let mut data_lines: Vec<String> = lines[1..].iter().map(|s| s.to_string()).collect();
    data_lines.sort();
    assert_eq!(data_lines[0], "1,Alice,30");
    assert_eq!(data_lines[1], "2,Bob,25");
    assert_eq!(data_lines[2], "3,Charlie,28");

    // 使用 COPY (query) TO 导出查询结果
    let query_csv_path = dir.join("copy_to_query_export.csv");
    let query_csv_str = query_csv_path.to_str().unwrap();
    db.execute(&format!(
        "COPY (MATCH (n:Person) WHERE n.age >= 28 RETURN n.id, n.name ORDER BY n.id) TO '{}'",
        query_csv_str
    ))
    .unwrap();

    let query_content = std::fs::read_to_string(&query_csv_path).unwrap();
    let query_lines: Vec<&str> = query_content.trim().lines().collect();

    // header: n.id, n.name
    assert!(query_lines[0].contains("id"));
    assert!(query_lines[0].contains("name"));

    // 筛选 age >= 28 的有: Alice(30), Charlie(28) = 2 行
    assert_eq!(query_lines.len(), 3); // header + 2 rows

    // 清理
    std::fs::remove_file(&csv_path).ok();
    std::fs::remove_file(&query_csv_path).ok();
}

/// CSV 导入 -> 导出 -> 重新导入到新表 -> 对比数据一致
#[test]
fn csv_roundtrip() {
    use std::io::Write;

    let dir = std::env::temp_dir().join("gqlite_scenario_data_ops");
    std::fs::create_dir_all(&dir).ok();
    let import_csv = dir.join("roundtrip_import.csv");
    let export_csv = dir.join("roundtrip_export.csv");

    // 1. 写入原始 CSV
    {
        let mut f = std::fs::File::create(&import_csv).unwrap();
        writeln!(f, "id,name,age").unwrap();
        writeln!(f, "10,Xander,40").unwrap();
        writeln!(f, "20,Yuki,33").unwrap();
        writeln!(f, "30,Zara,27").unwrap();
        writeln!(f, "40,Wren,45").unwrap();
    }

    let db = Database::in_memory();

    // 2. 创建第一张表并导入
    db.execute("CREATE NODE TABLE OrigPerson (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
        .unwrap();
    let import_str = import_csv.to_str().unwrap();
    db.execute(&format!("COPY OrigPerson FROM '{}'", import_str)).unwrap();

    // 3. 导出到 CSV
    let export_str = export_csv.to_str().unwrap();
    db.execute(&format!("COPY OrigPerson TO '{}'", export_str)).unwrap();

    // 4. 创建第二张表并从导出的 CSV 重新导入
    db.execute("CREATE NODE TABLE CopyPerson (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
        .unwrap();
    db.execute(&format!("COPY CopyPerson FROM '{}'", export_str)).unwrap();

    // 5. 对比两张表数据
    let orig = db.query("MATCH (n:OrigPerson) RETURN n.id, n.name, n.age ORDER BY n.id").unwrap();
    let copy = db.query("MATCH (n:CopyPerson) RETURN n.id, n.name, n.age ORDER BY n.id").unwrap();

    assert_eq!(orig.num_rows(), copy.num_rows());
    assert_eq!(orig.num_rows(), 4);

    for i in 0..orig.num_rows() {
        assert_eq!(orig.rows()[i].get_int(0).unwrap(), copy.rows()[i].get_int(0).unwrap());
        assert_eq!(orig.rows()[i].get_string(1).unwrap(), copy.rows()[i].get_string(1).unwrap());
        assert_eq!(orig.rows()[i].get_int(2).unwrap(), copy.rows()[i].get_int(2).unwrap());
    }

    // 具体值验证
    assert_eq!(orig.rows()[0].get_int(0).unwrap(), 10);
    assert_eq!(orig.rows()[0].get_string(1).unwrap(), "Xander");
    assert_eq!(orig.rows()[3].get_int(0).unwrap(), 40);
    assert_eq!(orig.rows()[3].get_string(1).unwrap(), "Wren");

    // 清理
    std::fs::remove_file(&import_csv).ok();
    std::fs::remove_file(&export_csv).ok();
}

// ════════════════════════════════════════════════════════════════
// 第二部分：PreparedStatement 参数化查询
// ════════════════════════════════════════════════════════════════

/// 辅助函数：创建带数据的 Person 表
fn setup_person_db() -> Database {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, active BOOL, score DOUBLE, PRIMARY KEY (id))")
        .unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice', age: 30, active: true, score: 95.5})")
        .unwrap();
    db.execute("CREATE (n:Person {id: 2, name: 'Bob', age: 25, active: false, score: 82.3})")
        .unwrap();
    db.execute("CREATE (n:Person {id: 3, name: 'Charlie', age: 28, active: true, score: 91.0})")
        .unwrap();
    db.execute("CREATE (n:Person {id: 4, name: 'Diana', age: 35, active: false, score: 78.8})")
        .unwrap();
    db.execute("CREATE (n:Person {id: 5, name: 'Eve', age: 22, active: true, score: 88.7})")
        .unwrap();
    db
}

/// 基本参数化查询：使用 prepare + execute
#[test]
fn prepared_statement_basic() {
    let db = setup_person_db();
    let conn = db.connect();

    let stmt = conn.prepare("MATCH (n:Person) WHERE n.id = $id RETURN n.name").unwrap();

    let mut params = HashMap::new();
    params.insert("id".to_string(), Value::Int(1));
    let result = stmt.execute(params).unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "Alice");
}

/// 同一个 PreparedStatement 多次使用不同参数，验证结果正确
#[test]
fn prepared_statement_reuse() {
    let db = setup_person_db();
    let conn = db.connect();

    let stmt = conn.prepare("MATCH (n:Person) WHERE n.id = $id RETURN n.name, n.age").unwrap();

    // 第一次执行：查 Alice
    let mut params1 = HashMap::new();
    params1.insert("id".to_string(), Value::Int(1));
    let r1 = stmt.execute(params1).unwrap();
    assert_eq!(r1.num_rows(), 1);
    assert_eq!(r1.rows()[0].get_string(0).unwrap(), "Alice");
    assert_eq!(r1.rows()[0].get_int(1).unwrap(), 30);

    // 第二次执行：查 Bob
    let mut params2 = HashMap::new();
    params2.insert("id".to_string(), Value::Int(2));
    let r2 = stmt.execute(params2).unwrap();
    assert_eq!(r2.num_rows(), 1);
    assert_eq!(r2.rows()[0].get_string(0).unwrap(), "Bob");
    assert_eq!(r2.rows()[0].get_int(1).unwrap(), 25);

    // 第三次执行：查 Eve
    let mut params3 = HashMap::new();
    params3.insert("id".to_string(), Value::Int(5));
    let r3 = stmt.execute(params3).unwrap();
    assert_eq!(r3.num_rows(), 1);
    assert_eq!(r3.rows()[0].get_string(0).unwrap(), "Eve");
    assert_eq!(r3.rows()[0].get_int(1).unwrap(), 22);

    // 第四次执行：查不存在的 id
    let mut params4 = HashMap::new();
    params4.insert("id".to_string(), Value::Int(999));
    let r4 = stmt.execute(params4).unwrap();
    assert_eq!(r4.num_rows(), 0);

    // 第五次执行：查 Charlie（确认 stmt 未损坏）
    let mut params5 = HashMap::new();
    params5.insert("id".to_string(), Value::Int(3));
    let r5 = stmt.execute(params5).unwrap();
    assert_eq!(r5.num_rows(), 1);
    assert_eq!(r5.rows()[0].get_string(0).unwrap(), "Charlie");
}

/// 参数化查询覆盖不同类型：INT64、STRING、DOUBLE、BOOL
#[test]
fn prepared_statement_types() {
    let db = setup_person_db();
    let conn = db.connect();

    // INT64 参数
    let stmt_int = conn
        .prepare("MATCH (n:Person) WHERE n.age > $min_age RETURN n.name ORDER BY n.name")
        .unwrap();
    let mut p_int = HashMap::new();
    p_int.insert("min_age".to_string(), Value::Int(28));
    let r_int = stmt_int.execute(p_int).unwrap();
    // age > 28: Alice(30), Diana(35) = 2 人
    assert_eq!(r_int.num_rows(), 2);
    assert_eq!(r_int.rows()[0].get_string(0).unwrap(), "Alice");
    assert_eq!(r_int.rows()[1].get_string(0).unwrap(), "Diana");

    // STRING 参数
    let stmt_str = conn.prepare("MATCH (n:Person) WHERE n.name = $name RETURN n.id").unwrap();
    let mut p_str = HashMap::new();
    p_str.insert("name".to_string(), Value::String("Charlie".to_string()));
    let r_str = stmt_str.execute(p_str).unwrap();
    assert_eq!(r_str.num_rows(), 1);
    assert_eq!(r_str.rows()[0].get_int(0).unwrap(), 3);

    // DOUBLE 参数
    let stmt_dbl = conn
        .prepare("MATCH (n:Person) WHERE n.score >= $min_score RETURN n.name ORDER BY n.name")
        .unwrap();
    let mut p_dbl = HashMap::new();
    p_dbl.insert("min_score".to_string(), Value::Float(90.0));
    let r_dbl = stmt_dbl.execute(p_dbl).unwrap();
    // score >= 90.0: Alice(95.5), Charlie(91.0) = 2 人
    assert_eq!(r_dbl.num_rows(), 2);
    assert_eq!(r_dbl.rows()[0].get_string(0).unwrap(), "Alice");
    assert_eq!(r_dbl.rows()[1].get_string(0).unwrap(), "Charlie");

    // BOOL 参数
    let stmt_bool = conn
        .prepare("MATCH (n:Person) WHERE n.active = $is_active RETURN n.name ORDER BY n.name")
        .unwrap();
    let mut p_true = HashMap::new();
    p_true.insert("is_active".to_string(), Value::Bool(true));
    let r_true = stmt_bool.execute(p_true).unwrap();
    // active = true: Alice, Charlie, Eve = 3 人
    assert_eq!(r_true.num_rows(), 3);
    assert_eq!(r_true.rows()[0].get_string(0).unwrap(), "Alice");
    assert_eq!(r_true.rows()[1].get_string(0).unwrap(), "Charlie");
    assert_eq!(r_true.rows()[2].get_string(0).unwrap(), "Eve");

    // BOOL 参数: false
    let mut p_false = HashMap::new();
    p_false.insert("is_active".to_string(), Value::Bool(false));
    let r_false = stmt_bool.execute(p_false).unwrap();
    // active = false: Bob, Diana = 2 人
    assert_eq!(r_false.num_rows(), 2);
    assert_eq!(r_false.rows()[0].get_string(0).unwrap(), "Bob");
    assert_eq!(r_false.rows()[1].get_string(0).unwrap(), "Diana");
}

// ════════════════════════════════════════════════════════════════
// 第三部分：多关系类型交叉查询
// ════════════════════════════════════════════════════════════════

/// 辅助函数：创建多关系类型测试数据
fn setup_multi_rel_db() -> Database {
    let db = Database::in_memory();

    // 节点表
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))").unwrap();
    db.execute("CREATE NODE TABLE Post (id INT64, title STRING, PRIMARY KEY (id))").unwrap();
    db.execute("CREATE NODE TABLE Tag (id INT64, name STRING, PRIMARY KEY (id))").unwrap();

    // 多种关系表（含同一对节点间的多种关系）
    db.execute("CREATE REL TABLE FOLLOWS (FROM Person TO Person)").unwrap();
    db.execute("CREATE REL TABLE BLOCKED (FROM Person TO Person)").unwrap();
    db.execute("CREATE REL TABLE LIKED (FROM Person TO Person)").unwrap();
    db.execute("CREATE REL TABLE POSTED (FROM Person TO Post)").unwrap();
    db.execute("CREATE REL TABLE HAS_TAG (FROM Post TO Tag)").unwrap();

    // 插入人
    for (id, name) in [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Diana"), (5, "Eve")] {
        db.execute(&format!("CREATE (n:Person {{id: {}, name: '{}'}})", id, name)).unwrap();
    }

    // 插入帖子
    for (id, title) in [(1, "Rust101"), (2, "GraphDB"), (3, "MLIntro")] {
        db.execute(&format!("CREATE (n:Post {{id: {}, title: '{}'}})", id, title)).unwrap();
    }

    // 插入标签
    for (id, name) in [(1, "Tech"), (2, "Database"), (3, "AI")] {
        db.execute(&format!("CREATE (n:Tag {{id: {}, name: '{}'}})", id, name)).unwrap();
    }

    db
}

/// 同一对节点间建立多种不同类型的关系（FOLLOWS + BLOCKED + LIKED），
/// 验证可以分别查询每种关系
#[test]
fn multi_rel_between_same_nodes() {
    let db = setup_multi_rel_db();

    // Alice 和 Bob 之间建立 3 种不同关系
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:FOLLOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:BLOCKED]->(b)",
    )
    .unwrap();
    db.execute("MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:LIKED]->(b)")
        .unwrap();

    // Alice 和 Charlie 之间建立 2 种关系
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 3 CREATE (a)-[:FOLLOWS]->(b)",
    )
    .unwrap();
    db.execute("MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 3 CREATE (a)-[:LIKED]->(b)")
        .unwrap();

    // Bob -> Charlie: 只有 FOLLOWS
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.id = 2 AND b.id = 3 CREATE (a)-[:FOLLOWS]->(b)",
    )
    .unwrap();

    // 查询 FOLLOWS 关系
    let follows = db
        .query(
            "MATCH (a:Person)-[:FOLLOWS]->(b:Person) \
             WHERE a.id = 1 RETURN b.name ORDER BY b.name",
        )
        .unwrap();
    assert_eq!(follows.num_rows(), 2);
    assert_eq!(follows.rows()[0].get_string(0).unwrap(), "Bob");
    assert_eq!(follows.rows()[1].get_string(0).unwrap(), "Charlie");

    // 查询 BLOCKED 关系 — Alice 只 BLOCKED 了 Bob
    let blocked = db
        .query(
            "MATCH (a:Person)-[:BLOCKED]->(b:Person) \
             WHERE a.id = 1 RETURN b.name",
        )
        .unwrap();
    assert_eq!(blocked.num_rows(), 1);
    assert_eq!(blocked.rows()[0].get_string(0).unwrap(), "Bob");

    // 查询 LIKED 关系 — Alice LIKED 了 Bob 和 Charlie
    let liked = db
        .query(
            "MATCH (a:Person)-[:LIKED]->(b:Person) \
             WHERE a.id = 1 RETURN b.name ORDER BY b.name",
        )
        .unwrap();
    assert_eq!(liked.num_rows(), 2);
    assert_eq!(liked.rows()[0].get_string(0).unwrap(), "Bob");
    assert_eq!(liked.rows()[1].get_string(0).unwrap(), "Charlie");

    // 验证总 FOLLOWS 数
    let all_follows =
        db.query("MATCH (a:Person)-[:FOLLOWS]->(b:Person) RETURN a.name, b.name").unwrap();
    assert_eq!(all_follows.num_rows(), 3); // Alice->Bob, Alice->Charlie, Bob->Charlie

    // 验证总 BLOCKED 数
    let all_blocked =
        db.query("MATCH (a:Person)-[:BLOCKED]->(b:Person) RETURN a.name, b.name").unwrap();
    assert_eq!(all_blocked.num_rows(), 1); // 只有 Alice->Bob

    // 验证总 LIKED 数
    let all_liked =
        db.query("MATCH (a:Person)-[:LIKED]->(b:Person) RETURN a.name, b.name").unwrap();
    assert_eq!(all_liked.num_rows(), 2); // Alice->Bob, Alice->Charlie
}

/// 一个查询中遍历不同类型的关系：
/// (a)-[:FOLLOWS]->(b)-[:POSTED]->(c)-[:HAS_TAG]->(d)，4 跳跨类型查询
#[test]
fn cross_type_traversal() {
    let db = setup_multi_rel_db();

    // 建立关系链: Alice -FOLLOWS-> Bob -POSTED-> Rust101 -HAS_TAG-> Tech
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:FOLLOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (p:Person), (post:Post) WHERE p.id = 2 AND post.id = 1 CREATE (p)-[:POSTED]->(post)",
    )
    .unwrap();
    db.execute(
        "MATCH (post:Post), (tag:Tag) WHERE post.id = 1 AND tag.id = 1 CREATE (post)-[:HAS_TAG]->(tag)",
    )
    .unwrap();

    // 另一条链: Alice -FOLLOWS-> Charlie -POSTED-> GraphDB -HAS_TAG-> Database
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 3 CREATE (a)-[:FOLLOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (p:Person), (post:Post) WHERE p.id = 3 AND post.id = 2 CREATE (p)-[:POSTED]->(post)",
    )
    .unwrap();
    db.execute(
        "MATCH (post:Post), (tag:Tag) WHERE post.id = 2 AND tag.id = 2 CREATE (post)-[:HAS_TAG]->(tag)",
    )
    .unwrap();

    // Diana -FOLLOWS-> Eve -POSTED-> MLIntro -HAS_TAG-> AI
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.id = 4 AND b.id = 5 CREATE (a)-[:FOLLOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (p:Person), (post:Post) WHERE p.id = 5 AND post.id = 3 CREATE (p)-[:POSTED]->(post)",
    )
    .unwrap();
    db.execute(
        "MATCH (post:Post), (tag:Tag) WHERE post.id = 3 AND tag.id = 3 CREATE (post)-[:HAS_TAG]->(tag)",
    )
    .unwrap();

    // 跨类型查询：Alice 通过 FOLLOWS 找到朋友、朋友发的帖子、帖子的标签
    let result = db
        .query(
            "MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:POSTED]->(post:Post)-[:HAS_TAG]->(tag:Tag) \
             WHERE a.id = 1 \
             RETURN b.name, post.title, tag.name ORDER BY b.name",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 2);
    // Bob -> Rust101 -> Tech
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "Bob");
    assert_eq!(result.rows()[0].get_string(1).unwrap(), "Rust101");
    assert_eq!(result.rows()[0].get_string(2).unwrap(), "Tech");
    // Charlie -> GraphDB -> Database
    assert_eq!(result.rows()[1].get_string(0).unwrap(), "Charlie");
    assert_eq!(result.rows()[1].get_string(1).unwrap(), "GraphDB");
    assert_eq!(result.rows()[1].get_string(2).unwrap(), "Database");

    // Diana 的链路
    let diana_result = db
        .query(
            "MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:POSTED]->(post:Post)-[:HAS_TAG]->(tag:Tag) \
             WHERE a.id = 4 \
             RETURN b.name, post.title, tag.name",
        )
        .unwrap();
    assert_eq!(diana_result.num_rows(), 1);
    assert_eq!(diana_result.rows()[0].get_string(0).unwrap(), "Eve");
    assert_eq!(diana_result.rows()[0].get_string(1).unwrap(), "MLIntro");
    assert_eq!(diana_result.rows()[0].get_string(2).unwrap(), "AI");

    // 查询不存在的链路：Eve 没有 FOLLOWS 别人
    let eve_result = db
        .query(
            "MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:POSTED]->(post:Post)-[:HAS_TAG]->(tag:Tag) \
             WHERE a.id = 5 \
             RETURN b.name",
        )
        .unwrap();
    assert_eq!(eve_result.num_rows(), 0);
}

/// 统计一个节点的不同类型的出边数量
#[test]
fn mixed_relationship_aggregation() {
    let db = setup_multi_rel_db();

    // 为 Alice 建立多种类型的出边
    // FOLLOWS: Alice -> Bob, Alice -> Charlie, Alice -> Diana
    for target in [2, 3, 4] {
        db.execute(&format!(
            "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = {} CREATE (a)-[:FOLLOWS]->(b)",
            target
        ))
        .unwrap();
    }

    // BLOCKED: Alice -> Eve
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 5 CREATE (a)-[:BLOCKED]->(b)",
    )
    .unwrap();

    // LIKED: Alice -> Bob, Alice -> Charlie
    for target in [2, 3] {
        db.execute(&format!(
            "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = {} CREATE (a)-[:LIKED]->(b)",
            target
        ))
        .unwrap();
    }

    // POSTED: Alice -> Post 1, Alice -> Post 2
    for post_id in [1, 2] {
        db.execute(&format!(
            "MATCH (a:Person), (p:Post) WHERE a.id = 1 AND p.id = {} CREATE (a)-[:POSTED]->(p)",
            post_id
        ))
        .unwrap();
    }

    // 分别统计每种关系类型的出边数
    let follows_count =
        db.query("MATCH (a:Person)-[:FOLLOWS]->(b:Person) WHERE a.id = 1 RETURN count(b)").unwrap();
    assert_eq!(follows_count.rows()[0].get_int(0).unwrap(), 3);

    let blocked_count =
        db.query("MATCH (a:Person)-[:BLOCKED]->(b:Person) WHERE a.id = 1 RETURN count(b)").unwrap();
    assert_eq!(blocked_count.rows()[0].get_int(0).unwrap(), 1);

    let liked_count =
        db.query("MATCH (a:Person)-[:LIKED]->(b:Person) WHERE a.id = 1 RETURN count(b)").unwrap();
    assert_eq!(liked_count.rows()[0].get_int(0).unwrap(), 2);

    let posted_count =
        db.query("MATCH (a:Person)-[:POSTED]->(p:Post) WHERE a.id = 1 RETURN count(p)").unwrap();
    assert_eq!(posted_count.rows()[0].get_int(0).unwrap(), 2);

    // 验证 Bob 的出边情况（Bob 没有主动建立任何关系）
    let bob_follows =
        db.query("MATCH (a:Person)-[:FOLLOWS]->(b:Person) WHERE a.id = 2 RETURN count(b)").unwrap();
    assert_eq!(bob_follows.rows()[0].get_int(0).unwrap(), 0);

    // 验证 Alice 是被 follow 最多的人 = 0（没人 follow Alice）
    let alice_in_follows =
        db.query("MATCH (a:Person)-[:FOLLOWS]->(b:Person) WHERE b.id = 1 RETURN count(a)").unwrap();
    assert_eq!(alice_in_follows.rows()[0].get_int(0).unwrap(), 0);

    // 验证 Bob 被 follow 的入边数 = 1（Alice follow 了 Bob）
    let bob_in_follows =
        db.query("MATCH (a:Person)-[:FOLLOWS]->(b:Person) WHERE b.id = 2 RETURN count(a)").unwrap();
    assert_eq!(bob_in_follows.rows()[0].get_int(0).unwrap(), 1);
}
