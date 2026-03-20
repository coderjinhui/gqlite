use gqlite_core::Database;
use std::path::PathBuf;

fn temp_db_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("gqlite_atomicity_test");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(format!("{}.graph", name))
}

fn cleanup(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
    let wal = path.with_extension("graph.wal");
    let _ = std::fs::remove_file(&wal);
    let tmp = path.with_extension("graph.tmp");
    let _ = std::fs::remove_file(&tmp);
}

fn count_nodes(db: &Database, label: &str) -> usize {
    let q = format!("MATCH (n:{}) RETURN n", label);
    db.query(&q).map(|r| r.num_rows()).unwrap_or(0)
}

// ============================================================
// 1. INSERT 原子性测试
// ============================================================

/// 单条 INSERT 成功后数据可查。
#[test]
fn insert_single_node_committed() {
    let path = temp_db_path("insert_single_ok");
    cleanup(&path);

    let db = Database::open(&path).unwrap();
    db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:A {id: 1})").unwrap();

    assert_eq!(count_nodes(&db, "A"), 1);
    cleanup(&path);
}

/// 重复 PK INSERT 应报错，且不影响之前已提交的数据。
#[test]
fn insert_duplicate_pk_does_not_corrupt() {
    let path = temp_db_path("insert_dup_pk");
    cleanup(&path);

    let db = Database::open(&path).unwrap();
    db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();

    // 先插入一行
    db.execute("CREATE (n:A {id: 1})").unwrap();
    assert_eq!(count_nodes(&db, "A"), 1);

    // 第二次插入相同 PK，应报错
    let result = db.execute("CREATE (n:A {id: 1})");
    assert!(result.is_err(), "duplicate PK should fail");

    // 原有数据不受影响
    assert_eq!(count_nodes(&db, "A"), 1);

    // 可以继续插入不同的 PK
    db.execute("CREATE (n:A {id: 2})").unwrap();
    assert_eq!(count_nodes(&db, "A"), 2);
    cleanup(&path);
}

/// 批量 INSERT 中如果某条失败，已成功的行的状态验证。
/// 注意：当前实现下已成功的行会保留（缺乏 rollback），
/// 此测试记录当前行为，后续 004 修复后预期行为会变。
#[test]
fn insert_batch_partial_failure_behavior() {
    let path = temp_db_path("insert_batch_partial");
    cleanup(&path);

    let db = Database::open(&path).unwrap();
    db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();

    // 第一条成功
    db.execute("CREATE (n:A {id: 1})").unwrap();

    // 第二条重复 PK 失败
    let result = db.execute("CREATE (n:A {id: 1})");
    assert!(result.is_err());

    // 验证当前行为：第一条的数据保留
    let cnt = count_nodes(&db, "A");
    assert_eq!(cnt, 1, "first insert should be preserved");
    cleanup(&path);
}

/// 多语句执行中，后面语句失败不影响前面语句的提交。
/// 每条语句是独立事务（当前实现）。
#[test]
fn multi_statement_independent_transactions() {
    let path = temp_db_path("multi_stmt_indep");
    cleanup(&path);

    let db = Database::open(&path).unwrap();
    db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();

    // 第一条成功
    db.execute("CREATE (n:A {id: 1})").unwrap();

    // 第二条失败
    let _ = db.execute("CREATE (n:A {id: 1})");

    // 第三条成功
    db.execute("CREATE (n:A {id: 2})").unwrap();

    // 应有 2 行
    assert_eq!(count_nodes(&db, "A"), 2);
    cleanup(&path);
}

// ============================================================
// 2. UPDATE 原子性测试
// ============================================================

/// SET 更新主键为重复值应失败，且原数据不受影响。
#[test]
fn update_pk_duplicate_fails_safely() {
    let path = temp_db_path("update_pk_dup");
    cleanup(&path);

    let db = Database::open(&path).unwrap();
    db.execute("CREATE NODE TABLE A(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:A {id: 1, name: 'Alice'})").unwrap();
    db.execute("CREATE (n:A {id: 2, name: 'Bob'})").unwrap();

    // 尝试将 id=2 的 PK 改为 1（冲突）
    let result = db.execute("MATCH (n:A) WHERE n.id = 2 SET n.id = 1");
    assert!(result.is_err(), "duplicate PK update should fail");

    // 验证两行数据都完好
    assert_eq!(count_nodes(&db, "A"), 2);

    // 验证原始值未被破坏
    let r = db.query("MATCH (n:A) WHERE n.id = 2 RETURN n.name").unwrap();
    assert_eq!(r.num_rows(), 1);
    cleanup(&path);
}

/// SET 不存在的属性应报错。
#[test]
fn update_nonexistent_column_fails() {
    let path = temp_db_path("update_no_col");
    cleanup(&path);

    let db = Database::open(&path).unwrap();
    db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:A {id: 1})").unwrap();

    let result = db.execute("MATCH (n:A) SET n.nonexistent = 'val'");
    assert!(result.is_err(), "setting nonexistent column should fail");

    // 原数据不受影响
    assert_eq!(count_nodes(&db, "A"), 1);
    cleanup(&path);
}

/// 成功的 UPDATE 后数据可查。
#[test]
fn update_success_persists() {
    let path = temp_db_path("update_ok");
    cleanup(&path);

    let db = Database::open(&path).unwrap();
    db.execute("CREATE NODE TABLE A(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:A {id: 1, name: 'Alice'})").unwrap();
    db.execute("MATCH (n:A) WHERE n.id = 1 SET n.name = 'Alicia'").unwrap();

    let r = db.query("MATCH (n:A) WHERE n.id = 1 RETURN n.name").unwrap();
    let rows = r.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get_string(0), Some("Alicia"));
    cleanup(&path);
}

// ============================================================
// 3. DELETE 原子性测试
// ============================================================

/// DELETE 不存在的节点应静默成功（MATCH 不匹配任何行）。
#[test]
fn delete_nonexistent_node_is_noop() {
    let path = temp_db_path("delete_nonexist");
    cleanup(&path);

    let db = Database::open(&path).unwrap();
    db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:A {id: 1})").unwrap();

    // WHERE 条件不匹配，DELETE 无影响
    db.execute("MATCH (n:A) WHERE n.id = 999 DELETE n").unwrap();
    assert_eq!(count_nodes(&db, "A"), 1);
    cleanup(&path);
}

/// DETACH DELETE 正确删除节点及关联关系。
#[test]
fn detach_delete_removes_node_and_rels() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))").unwrap();
    db.execute("CREATE REL TABLE KNOWS (FROM Person TO Person)").unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {id: 2, name: 'Bob'})").unwrap();
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[r:KNOWS]->(b)",
    )
    .unwrap();

    // 有关系的节点用 DETACH DELETE
    db.execute("MATCH (n:Person) WHERE n.id = 1 DETACH DELETE n").unwrap();
    assert_eq!(count_nodes(&db, "Person"), 1);

    // 关系也被删除
    let r = db.query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name").unwrap();
    assert_eq!(r.num_rows(), 0);
}

/// 成功的 DELETE 后数据不可见。
#[test]
fn delete_success_removes_data() {
    let path = temp_db_path("delete_ok");
    cleanup(&path);

    let db = Database::open(&path).unwrap();
    db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:A {id: 1})").unwrap();
    db.execute("CREATE (n:A {id: 2})").unwrap();
    assert_eq!(count_nodes(&db, "A"), 2);

    db.execute("MATCH (n:A) WHERE n.id = 1 DELETE n").unwrap();
    assert_eq!(count_nodes(&db, "A"), 1);
    cleanup(&path);
}

// ============================================================
// 4. DDL + DML 混合测试
// ============================================================

/// CREATE TABLE 后 INSERT 失败，表应该仍然存在。
#[test]
fn create_table_then_failed_insert_keeps_table() {
    let path = temp_db_path("ddl_dml_table_kept");
    cleanup(&path);

    let db = Database::open(&path).unwrap();
    db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();

    // INSERT 第一行成功
    db.execute("CREATE (n:A {id: 1})").unwrap();
    // INSERT 重复 PK 失败
    let _ = db.execute("CREATE (n:A {id: 1})");

    // 表仍然存在，第一行仍在
    assert_eq!(count_nodes(&db, "A"), 1);
    cleanup(&path);
}

/// DROP TABLE 后不能再查询。
#[test]
fn drop_table_makes_data_inaccessible() {
    let path = temp_db_path("ddl_drop");
    cleanup(&path);

    let db = Database::open(&path).unwrap();
    db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:A {id: 1})").unwrap();

    db.execute("DROP TABLE A").unwrap();

    // 查询应报错（表不存在）
    let result = db.query("MATCH (n:A) RETURN n");
    assert!(result.is_err(), "query on dropped table should fail");
    cleanup(&path);
}

/// ALTER TABLE + 引用新列的 INSERT。
#[test]
fn alter_table_add_column_then_insert() {
    let path = temp_db_path("ddl_alter_insert");
    cleanup(&path);

    let db = Database::open(&path).unwrap();
    db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:A {id: 1})").unwrap();

    db.execute("ALTER TABLE A ADD COLUMN name STRING").unwrap();

    // 使用新列
    db.execute("MATCH (n:A) WHERE n.id = 1 SET n.name = 'Alice'").unwrap();

    let r = db.query("MATCH (n:A) WHERE n.id = 1 RETURN n.name").unwrap();
    assert_eq!(r.num_rows(), 1);
    assert_eq!(r.rows()[0].get_string(0), Some("Alice"));
    cleanup(&path);
}

// ============================================================
// 5. Recovery 后状态一致性测试
// ============================================================

/// 已提交写入在数据库重新打开后仍可见（WAL recovery）。
#[test]
fn recovery_committed_data_survives() {
    let path = temp_db_path("recovery_committed");
    cleanup(&path);

    // Phase 1: 写入并关闭（不手动 checkpoint）
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE A(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE (n:A {id: 1, name: 'Alice'})").unwrap();
        db.execute("CREATE (n:A {id: 2, name: 'Bob'})").unwrap();
        // db dropped — WAL flushed
    }

    // Phase 2: 重新打开，验证数据
    {
        let db = Database::open(&path).unwrap();
        let r = db.query("MATCH (n:A) RETURN n.id ORDER BY n.id").unwrap();
        assert_eq!(r.num_rows(), 2);
    }

    cleanup(&path);
}

/// 多次写入 + checkpoint + 重新打开后数据一致。
#[test]
fn recovery_after_checkpoint() {
    let path = temp_db_path("recovery_checkpoint");
    cleanup(&path);

    // Phase 1: 写入 + checkpoint
    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE (n:A {id: 1})").unwrap();
        db.execute("CREATE (n:A {id: 2})").unwrap();
        db.checkpoint().unwrap();
        // checkpoint 后再写入一些数据（WAL 中）
        db.execute("CREATE (n:A {id: 3})").unwrap();
    }

    // Phase 2: 重新打开，验证 checkpoint + WAL 数据都恢复
    {
        let db = Database::open(&path).unwrap();
        assert_eq!(count_nodes(&db, "A"), 3);
    }

    cleanup(&path);
}

/// UPDATE 后重新打开，更新值应保留。
#[test]
fn recovery_update_persists() {
    let path = temp_db_path("recovery_update");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE A(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE (n:A {id: 1, name: 'Alice'})").unwrap();
        db.execute("MATCH (n:A) WHERE n.id = 1 SET n.name = 'Alicia'").unwrap();
    }

    {
        let db = Database::open(&path).unwrap();
        let r = db.query("MATCH (n:A) WHERE n.id = 1 RETURN n.name").unwrap();
        assert_eq!(r.rows()[0].get_string(0), Some("Alicia"));
    }

    cleanup(&path);
}

/// DELETE 后重新打开，删除状态应保留。
#[test]
fn recovery_delete_persists() {
    let path = temp_db_path("recovery_delete");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE (n:A {id: 1})").unwrap();
        db.execute("CREATE (n:A {id: 2})").unwrap();
        db.execute("MATCH (n:A) WHERE n.id = 1 DELETE n").unwrap();
    }

    {
        let db = Database::open(&path).unwrap();
        assert_eq!(count_nodes(&db, "A"), 1);
        let r = db.query("MATCH (n:A) RETURN n.id").unwrap();
        assert_eq!(r.rows()[0].get_int(0), Some(2));
    }

    cleanup(&path);
}

/// DDL 操作（CREATE TABLE）在 recovery 后保留。
#[test]
fn recovery_ddl_persists() {
    let path = temp_db_path("recovery_ddl");
    cleanup(&path);

    {
        let db = Database::open(&path).unwrap();
        db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
        db.execute("CREATE NODE TABLE B(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    }

    {
        let db = Database::open(&path).unwrap();
        // 两张表都应存在
        db.execute("CREATE (n:A {id: 1})").unwrap();
        db.execute("CREATE (n:B {id: 1, name: 'test'})").unwrap();
        assert_eq!(count_nodes(&db, "A"), 1);
        assert_eq!(count_nodes(&db, "B"), 1);
    }

    cleanup(&path);
}

// ============================================================
// 6. 关系操作原子性测试
// ============================================================

/// 插入重复关系应失败，不影响已有数据。
#[test]
fn insert_duplicate_rel_fails_safely() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))").unwrap();
    db.execute("CREATE REL TABLE KNOWS (FROM Person TO Person)").unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {id: 2, name: 'Bob'})").unwrap();

    // 第一次创建关系成功
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[r:KNOWS]->(b)",
    )
    .unwrap();

    // 第二次创建相同关系应失败
    let result = db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[r:KNOWS]->(b)",
    );
    assert!(result.is_err(), "duplicate relationship should fail");

    // 只有一条关系
    let r = db.query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name").unwrap();
    assert_eq!(r.num_rows(), 1);
}

/// 关系 + 节点操作组合：先创建关系，再删除节点。
#[test]
fn rel_survives_after_node_operations() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))").unwrap();
    db.execute("CREATE REL TABLE KNOWS (FROM Person TO Person)").unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'A'})").unwrap();
    db.execute("CREATE (n:Person {id: 2, name: 'B'})").unwrap();
    db.execute("CREATE (n:Person {id: 3, name: 'C'})").unwrap();
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[r:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.id = 2 AND b.id = 3 CREATE (a)-[r:KNOWS]->(b)",
    )
    .unwrap();

    // 删除 id=3 的节点（有入边）
    db.execute("MATCH (n:Person) WHERE n.id = 3 DETACH DELETE n").unwrap();

    // 应剩 2 个节点和 1 条关系
    assert_eq!(count_nodes(&db, "Person"), 2);
    let r = db.query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name").unwrap();
    assert_eq!(r.num_rows(), 1);
}

// ============================================================
// 7. 错误恢复后继续操作测试
// ============================================================

/// 错误后数据库仍可正常使用。
#[test]
fn database_usable_after_error() {
    let path = temp_db_path("usable_after_err");
    cleanup(&path);

    let db = Database::open(&path).unwrap();
    db.execute("CREATE NODE TABLE A(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:A {id: 1})").unwrap();

    // 制造一系列错误
    let _ = db.execute("CREATE (n:A {id: 1})"); // dup PK
    let _ = db.execute("INVALID SYNTAX"); // parse error
    let _ = db.query("MATCH (n:NonExistent) RETURN n"); // unknown table

    // 数据库仍可正常操作
    db.execute("CREATE (n:A {id: 2})").unwrap();
    assert_eq!(count_nodes(&db, "A"), 2);

    let r = db.query("MATCH (n:A) RETURN n.id ORDER BY n.id").unwrap();
    assert_eq!(r.num_rows(), 2);
    cleanup(&path);
}

/// 写错误后读操作不受影响。
#[test]
fn read_works_after_write_error() {
    let path = temp_db_path("read_after_write_err");
    cleanup(&path);

    let db = Database::open(&path).unwrap();
    db.execute("CREATE NODE TABLE A(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:A {id: 1, name: 'Alice'})").unwrap();

    // 写错误
    let _ = db.execute("CREATE (n:A {id: 1, name: 'Duplicate'})");

    // 读操作正常
    let r = db.query("MATCH (n:A) WHERE n.id = 1 RETURN n.name").unwrap();
    assert_eq!(r.rows()[0].get_string(0), Some("Alice"));
    cleanup(&path);
}
