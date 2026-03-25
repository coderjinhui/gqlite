/// 场景测试：企业权限管理 (RBAC)
///
/// 节点类型：User, Role, Permission, Resource
/// 关系类型：HAS_ROLE, INHERITS（角色继承）, GRANTS, APPLIES_TO
///
/// 角色继承层级：
///   SuperAdmin → Admin → Manager → Editor → Viewer
///
use gqlite_core::Database;

/// 构建 RBAC schema 和初始数据
fn setup_rbac() -> Database {
    let db = Database::in_memory();

    // ── 建表 ──────────────────────────────────────────────
    db.execute("CREATE NODE TABLE User(id INT64, name STRING, dept STRING, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE NODE TABLE Role(id INT64, name STRING, level INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute(
        "CREATE NODE TABLE Permission(id INT64, name STRING, action STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE NODE TABLE Resource(id INT64, name STRING, rtype STRING, PRIMARY KEY(id))")
        .unwrap();

    // 关系表
    db.execute("CREATE REL TABLE HAS_ROLE(FROM User TO Role)").unwrap();
    db.execute("CREATE REL TABLE INHERITS(FROM Role TO Role)").unwrap();
    db.execute("CREATE REL TABLE GRANTS(FROM Role TO Permission)").unwrap();
    db.execute("CREATE REL TABLE APPLIES_TO(FROM Permission TO Resource)").unwrap();

    // ── 用户 (10) ────────────────────────────────────────
    let users = [
        (1, "alice", "engineering"),
        (2, "bob", "engineering"),
        (3, "charlie", "marketing"),
        (4, "diana", "marketing"),
        (5, "eve", "finance"),
        (6, "frank", "finance"),
        (7, "grace", "hr"),
        (8, "henry", "hr"),
        (9, "ivy", "ops"),
        (10, "jack", "ops"),
    ];
    for (id, name, dept) in &users {
        db.execute(&format!("CREATE (u:User {{id: {}, name: '{}', dept: '{}'}})", id, name, dept))
            .unwrap();
    }

    // ── 角色 (5) — 层级继承 ─────────────────────────────
    // SuperAdmin(1) → Admin(2) → Manager(3) → Editor(4) → Viewer(5)
    let roles = [
        (1, "super_admin", 1),
        (2, "admin", 2),
        (3, "manager", 3),
        (4, "editor", 4),
        (5, "viewer", 5),
    ];
    for (id, name, level) in &roles {
        db.execute(&format!("CREATE (r:Role {{id: {}, name: '{}', level: {}}})", id, name, level))
            .unwrap();
    }

    // ── 权限 (10) ────────────────────────────────────────
    let permissions = [
        (1, "read", "read"),
        (2, "write", "write"),
        (3, "delete", "delete"),
        (4, "create", "create"),
        (5, "admin_panel", "access"),
        (6, "user_mgmt", "manage"),
        (7, "audit_log", "view"),
        (8, "export", "export"),
        (9, "import", "import"),
        (10, "configure", "config"),
    ];
    for (id, name, action) in &permissions {
        db.execute(&format!(
            "CREATE (p:Permission {{id: {}, name: '{}', action: '{}'}})",
            id, name, action
        ))
        .unwrap();
    }

    // ── 资源 (8) ─────────────────────────────────────────
    let resources = [
        (1, "dashboard", "page"),
        (2, "reports", "page"),
        (3, "user_db", "database"),
        (4, "file_store", "storage"),
        (5, "api_gateway", "service"),
        (6, "log_system", "service"),
        (7, "config_db", "database"),
        (8, "backup_store", "storage"),
    ];
    for (id, name, rtype) in &resources {
        db.execute(&format!(
            "CREATE (r:Resource {{id: {}, name: '{}', rtype: '{}'}})",
            id, name, rtype
        ))
        .unwrap();
    }

    // ── HAS_ROLE 关系 ────────────────────────────────────
    // alice → super_admin, bob → admin, charlie → manager,
    // diana → editor, eve → viewer, frank → viewer,
    // grace → manager, henry → editor, ivy → admin, jack → viewer
    let user_roles = [
        (1, 1),  // alice → super_admin
        (2, 2),  // bob → admin
        (3, 3),  // charlie → manager
        (4, 4),  // diana → editor
        (5, 5),  // eve → viewer
        (6, 5),  // frank → viewer
        (7, 3),  // grace → manager
        (8, 4),  // henry → editor
        (9, 2),  // ivy → admin
        (10, 5), // jack → viewer
    ];
    for (uid, rid) in &user_roles {
        db.execute(&format!(
            "MATCH (u:User), (r:Role) WHERE u.id = {} AND r.id = {} \
             CREATE (u)-[:HAS_ROLE]->(r)",
            uid, rid
        ))
        .unwrap();
    }

    // ── INHERITS 关系（高→低继承链）────────────────────────
    // SuperAdmin(1) → Admin(2) → Manager(3) → Editor(4) → Viewer(5)
    let inherits = [(1, 2), (2, 3), (3, 4), (4, 5)];
    for (from, to) in &inherits {
        db.execute(&format!(
            "MATCH (a:Role), (b:Role) WHERE a.id = {} AND b.id = {} \
             CREATE (a)-[:INHERITS]->(b)",
            from, to
        ))
        .unwrap();
    }

    // ── GRANTS 关系（角色→权限）──────────────────────────
    // super_admin → admin_panel, user_mgmt, configure
    // admin → audit_log, export, import
    // manager → create, delete
    // editor → write
    // viewer → read
    let grants = [
        (1, 5),  // super_admin → admin_panel
        (1, 6),  // super_admin → user_mgmt
        (1, 10), // super_admin → configure
        (2, 7),  // admin → audit_log
        (2, 8),  // admin → export
        (2, 9),  // admin → import
        (3, 3),  // manager → delete
        (3, 4),  // manager → create
        (4, 2),  // editor → write
        (5, 1),  // viewer → read
    ];
    for (rid, pid) in &grants {
        db.execute(&format!(
            "MATCH (r:Role), (p:Permission) WHERE r.id = {} AND p.id = {} \
             CREATE (r)-[:GRANTS]->(p)",
            rid, pid
        ))
        .unwrap();
    }

    // ── APPLIES_TO 关系（权限→资源）─────────────────────
    let applies = [
        (1, 1),  // read → dashboard
        (1, 2),  // read → reports
        (2, 1),  // write → dashboard
        (3, 3),  // delete → user_db
        (4, 4),  // create → file_store
        (5, 1),  // admin_panel → dashboard
        (6, 3),  // user_mgmt → user_db
        (7, 6),  // audit_log → log_system
        (8, 2),  // export → reports
        (9, 4),  // import → file_store
        (10, 7), // configure → config_db
    ];
    for (pid, rid) in &applies {
        db.execute(&format!(
            "MATCH (p:Permission), (r:Resource) WHERE p.id = {} AND r.id = {} \
             CREATE (p)-[:APPLIES_TO]->(r)",
            pid, rid
        ))
        .unwrap();
    }

    db
}

// ── 1. 建表+数据完整性 ──────────────────────────────────────

#[test]
fn rbac_schema_and_data() {
    let db = setup_rbac();

    // 10 用户
    let r = db.query("MATCH (u:User) RETURN count(*)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 10);

    // 5 角色
    let r = db.query("MATCH (r:Role) RETURN count(*)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 5);

    // 10 权限
    let r = db.query("MATCH (p:Permission) RETURN count(*)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 10);

    // 8 资源
    let r = db.query("MATCH (r:Resource) RETURN count(*)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 8);

    // 10 条 HAS_ROLE
    let r = db.query("MATCH (u:User)-[r:HAS_ROLE]->(role:Role) RETURN count(*)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 10);

    // 4 条 INHERITS
    let r = db.query("MATCH (a:Role)-[r:INHERITS]->(b:Role) RETURN count(*)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 4);

    // 10 条 GRANTS
    let r = db.query("MATCH (r:Role)-[g:GRANTS]->(p:Permission) RETURN count(*)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 10);

    // 11 条 APPLIES_TO
    let r =
        db.query("MATCH (p:Permission)-[a:APPLIES_TO]->(res:Resource) RETURN count(*)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 11);
}

// ── 2. 直接权限查询 (1 跳) ──────────────────────────────────

#[test]
fn direct_permissions() {
    let db = setup_rbac();

    // alice 是 super_admin，直接拥有 admin_panel, user_mgmt, configure
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission) \
             WHERE u.name = 'alice' \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 3);
    let names: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(names, vec!["admin_panel", "configure", "user_mgmt"]);

    // eve 是 viewer，直接拥有 read
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission) \
             WHERE u.name = 'eve' \
             RETURN p.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 1);
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "read");
}

// ── 3. 递归查询：通过角色继承链获取间接权限 ──────────────────

#[test]
fn inherited_permissions() {
    let db = setup_rbac();

    // bob 是 admin(2)，继承链：admin → manager → editor → viewer
    // admin 直接权限：audit_log, export, import
    // manager 权限（继承 1 跳）：create, delete
    // editor 权限（继承 2 跳）：write
    // viewer 权限（继承 3 跳）：read
    // 间接权限共 4 个：create, delete, write, read
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:INHERITS*1..3]->(parent:Role)\
             -[:GRANTS]->(p:Permission) \
             WHERE u.name = 'bob' \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    let names: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(names, vec!["create", "delete", "read", "write"]);

    // charlie 是 manager(3)，继承链：manager → editor → viewer
    // 间接权限：write (editor), read (viewer) = 2 个
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:INHERITS*1..3]->(parent:Role)\
             -[:GRANTS]->(p:Permission) \
             WHERE u.name = 'charlie' \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    let names: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(names, vec!["read", "write"]);
}

// ── 4. 反向查询：找拥有某权限的所有用户 ─────────────────────

#[test]
fn users_with_specific_permission() {
    let db = setup_rbac();

    // 直接拥有 read 权限的用户：viewer 角色 → eve, frank, jack
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission) \
             WHERE p.name = 'read' \
             RETURN u.name ORDER BY u.name",
        )
        .unwrap();
    let names: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(names, vec!["eve", "frank", "jack"]);

    // 直接拥有 audit_log 权限的用户：admin 角色 → bob, ivy
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission) \
             WHERE p.name = 'audit_log' \
             RETURN u.name ORDER BY u.name",
        )
        .unwrap();
    let names: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(names, vec!["bob", "ivy"]);
}

// ── 5. 角色继承树 ───────────────────────────────────────────

#[test]
fn role_hierarchy() {
    let db = setup_rbac();

    // 查询所有直接继承关系
    let r = db
        .query(
            "MATCH (parent:Role)-[:INHERITS]->(child:Role) \
             RETURN parent.name, child.name ORDER BY parent.level",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 4);

    // 验证继承链
    let pairs: Vec<(String, String)> = r
        .rows()
        .iter()
        .map(|row| (row.get_string(0).unwrap().to_string(), row.get_string(1).unwrap().to_string()))
        .collect();
    assert!(pairs.contains(&("super_admin".to_string(), "admin".to_string())));
    assert!(pairs.contains(&("admin".to_string(), "manager".to_string())));
    assert!(pairs.contains(&("manager".to_string(), "editor".to_string())));
    assert!(pairs.contains(&("editor".to_string(), "viewer".to_string())));

    // super_admin 通过可变长路径继承所有下级角色
    let r = db
        .query(
            "MATCH (r:Role)-[:INHERITS*1..4]->(child:Role) \
             WHERE r.name = 'super_admin' \
             RETURN child.name ORDER BY child.level",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 4);
    let children: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(children, vec!["admin", "manager", "editor", "viewer"]);
}

// ── 6. EXISTS 子查询：检查某用户是否有某权限 ─────────────────

#[test]
fn permission_check_exists() {
    let db = setup_rbac();

    // alice(super_admin) 直接拥有 admin_panel → EXISTS 应为 true
    let r = db
        .query(
            "MATCH (u:User) \
             WHERE u.name = 'alice' AND \
             EXISTS { MATCH (u)-[:HAS_ROLE]->()-[:GRANTS]->(p:Permission) WHERE p.name = 'admin_panel' } \
             RETURN u.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 1);
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "alice");

    // eve(viewer) 没有 admin_panel → EXISTS 应为 false
    let r = db
        .query(
            "MATCH (u:User) \
             WHERE u.name = 'eve' AND \
             EXISTS { MATCH (u)-[:HAS_ROLE]->()-[:GRANTS]->(p:Permission) WHERE p.name = 'admin_panel' } \
             RETURN u.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 0);

    // NOT EXISTS: 找没有 read 直接权限的用户
    // viewer 角色有 read，所以非 viewer 的用户不具备
    // HAS_ROLE 到非 viewer 角色的用户不会通过此 EXISTS
    let r = db
        .query(
            "MATCH (u:User) \
             WHERE NOT EXISTS { MATCH (u)-[:HAS_ROLE]->()-[:GRANTS]->(p:Permission) WHERE p.name = 'read' } \
             RETURN u.name ORDER BY u.name",
        )
        .unwrap();
    // 非 viewer 角色的用户：alice(super_admin), bob(admin), charlie(manager),
    // diana(editor), grace(manager), henry(editor), ivy(admin) = 7 人
    assert_eq!(r.num_rows(), 7);
    let names: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(names, vec!["alice", "bob", "charlie", "diana", "grace", "henry", "ivy"]);
}

// ── 7. 动态授权：给用户添加角色 ─────────────────────────────

#[test]
fn add_role_to_user() {
    let db = setup_rbac();

    // eve 当前是 viewer，只有 read 权限
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission) \
             WHERE u.name = 'eve' \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 1);
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "read");

    // 给 eve 添加 editor 角色
    db.execute(
        "MATCH (u:User), (r:Role) WHERE u.id = 5 AND r.id = 4 \
         CREATE (u)-[:HAS_ROLE]->(r)",
    )
    .unwrap();

    // 现在 eve 有 viewer(read) + editor(write) 的直接权限 = 2 个
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission) \
             WHERE u.name = 'eve' \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 2);
    let names: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(names, vec!["read", "write"]);
}

// ── 8. 回收权限：删除角色关系 ────────────────────────────────

#[test]
fn remove_role_from_user() {
    let db = setup_rbac();

    // eve 当前是 viewer，有 read 权限
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission) \
             WHERE u.name = 'eve' \
             RETURN p.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 1);

    // 先给 eve 额外添加 editor 角色
    db.execute(
        "MATCH (u:User), (r:Role) WHERE u.id = 5 AND r.id = 4 \
         CREATE (u)-[:HAS_ROLE]->(r)",
    )
    .unwrap();
    let r = db
        .query("MATCH (u:User)-[:HAS_ROLE]->(r:Role) WHERE u.name = 'eve' RETURN count(*)")
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 2);

    // 回收权限：删除 eve 节点（DETACH DELETE）再重建不含 editor 的角色
    // 因为数据库不支持单独删除关系边，我们用 DETACH DELETE 删除 eve 然后重建
    db.execute("MATCH (u:User) WHERE u.id = 5 DETACH DELETE u").unwrap();

    // 重建 eve，只绑定 viewer 角色
    db.execute("CREATE (u:User {id: 5, name: 'eve', dept: 'finance'})").unwrap();
    db.execute(
        "MATCH (u:User), (r:Role) WHERE u.id = 5 AND r.id = 5 \
         CREATE (u)-[:HAS_ROLE]->(r)",
    )
    .unwrap();

    // 验证 eve 只有 viewer 的 read 权限
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission) \
             WHERE u.name = 'eve' \
             RETURN p.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 1);
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "read");
}

// ── 9. ALTER TABLE 添加列 + SET 更新 ────────────────────────

#[test]
fn alter_table_add_column() {
    let db = setup_rbac();

    // 给 Role 表添加 description 列
    db.execute("ALTER TABLE Role ADD description STRING").unwrap();

    // 已有数据的 description 应为 NULL
    let r = db.query("MATCH (r:Role) WHERE r.name = 'admin' RETURN r.description").unwrap();
    assert_eq!(r.num_rows(), 1);
    assert!(r.rows()[0].values[0].is_null());

    // SET 更新 description
    db.execute("MATCH (r:Role) WHERE r.name = 'admin' SET r.description = 'System administrator'")
        .unwrap();

    let r = db.query("MATCH (r:Role) WHERE r.name = 'admin' RETURN r.description").unwrap();
    assert_eq!(r.num_rows(), 1);
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "System administrator");

    // 其他角色的 description 仍为 NULL
    let r = db.query("MATCH (r:Role) WHERE r.name = 'viewer' RETURN r.description").unwrap();
    assert_eq!(r.num_rows(), 1);
    assert!(r.rows()[0].values[0].is_null());
}

// ── 10. DROP TABLE + 重建 ───────────────────────────────────

#[test]
fn drop_and_recreate() {
    let db = setup_rbac();

    // 确认 Permission 表有 10 条数据
    let r = db.query("MATCH (p:Permission) RETURN count(*)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 10);

    // 先删除依赖 Permission 的关系表
    db.execute("DROP TABLE GRANTS").unwrap();
    db.execute("DROP TABLE APPLIES_TO").unwrap();

    // 然后删除 Permission 节点表
    db.execute("DROP TABLE Permission").unwrap();

    // 查询应失败
    let r = db.query("MATCH (p:Permission) RETURN p.name");
    assert!(r.is_err());

    // 重建（可以用不同 schema）
    db.execute(
        "CREATE NODE TABLE Permission(id INT64, name STRING, action STRING, scope STRING, \
         PRIMARY KEY(id))",
    )
    .unwrap();

    // 重建关系表
    db.execute("CREATE REL TABLE GRANTS(FROM Role TO Permission)").unwrap();
    db.execute("CREATE REL TABLE APPLIES_TO(FROM Permission TO Resource)").unwrap();

    // 插入新数据
    db.execute("CREATE (p:Permission {id: 1, name: 'read_v2', action: 'read', scope: 'global'})")
        .unwrap();

    let r = db.query("MATCH (p:Permission) RETURN p.name, p.scope").unwrap();
    assert_eq!(r.num_rows(), 1);
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "read_v2");
    assert_eq!(r.rows()[0].get_string(1).unwrap(), "global");
}

// ── 11. 复杂路径：用户→角色→继承角色→权限→资源 (4跳) ──────

#[test]
fn multi_hop_access_check() {
    let db = setup_rbac();

    // alice 是 super_admin，直接拥有 configure 权限，configure → config_db
    // 查询路径：alice → super_admin → configure → config_db
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission)\
             -[:APPLIES_TO]->(res:Resource) \
             WHERE u.name = 'alice' AND res.name = 'config_db' \
             RETURN u.name, r.name, p.name, res.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 1);
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "alice");
    assert_eq!(r.rows()[0].get_string(1).unwrap(), "super_admin");
    assert_eq!(r.rows()[0].get_string(2).unwrap(), "configure");
    assert_eq!(r.rows()[0].get_string(3).unwrap(), "config_db");

    // bob 是 admin，通过继承（admin → manager → editor → viewer），
    // viewer 的 read 权限对应 dashboard, reports
    // 4 跳：bob → admin -[:INHERITS*1..3]-> viewer → read → dashboard/reports
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:INHERITS*1..3]->(parent:Role)\
             -[:GRANTS]->(p:Permission)-[:APPLIES_TO]->(res:Resource) \
             WHERE u.name = 'bob' AND p.name = 'read' \
             RETURN res.name ORDER BY res.name",
        )
        .unwrap();
    let resources: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(resources, vec!["dashboard", "reports"]);
}

// ── 12. WCC 算法找权限关联组 ────────────────────────────────

#[test]
fn wcc_permission_groups() {
    let db = setup_rbac();

    // 在 GRANTS 关系上运行 WCC：Role 和 Permission 通过 GRANTS 连接
    // 所有 5 个角色和 10 个权限通过 GRANTS 连接：
    // 角色继承链把所有角色间接关联（通过 INHERITS），
    // 但 WCC 只看 GRANTS 关系，每个角色-权限子图可能形成独立组件
    let r = db.query("CALL wcc('GRANTS') YIELD node_id, component_id").unwrap();
    // GRANTS 连接：5 个 Role + 10 个 Permission = 15 个节点
    assert_eq!(r.num_rows(), 15);

    // 统计不同的 component 数量
    let mut components: Vec<i64> = r.rows().iter().map(|row| row.get_int(1).unwrap()).collect();
    components.sort();
    components.dedup();

    // 每个角色独立连接不同的权限（没有共享权限），
    // 所以有 5 个独立连通分量
    assert_eq!(components.len(), 5);
}

// ── 13. 资源层级继承：父资源授权覆盖子资源 ───────────────────

#[test]
fn hierarchical_resource_access() {
    let db = setup_rbac();

    db.execute("CREATE REL TABLE CONTAINS(FROM Resource TO Resource)").unwrap();

    db.execute("CREATE (r:Resource {id: 100, name: 'config_schema', rtype: 'schema'})").unwrap();
    db.execute("CREATE (r:Resource {id: 101, name: 'config_table', rtype: 'table'})").unwrap();
    db.execute("CREATE (r:Resource {id: 102, name: 'config_column', rtype: 'column'})").unwrap();

    db.execute(
        "MATCH (parent:Resource), (child:Resource) WHERE parent.id = 7 AND child.id = 100 \
         CREATE (parent)-[:CONTAINS]->(child)",
    )
    .unwrap();
    db.execute(
        "MATCH (parent:Resource), (child:Resource) WHERE parent.id = 100 AND child.id = 101 \
         CREATE (parent)-[:CONTAINS]->(child)",
    )
    .unwrap();
    db.execute(
        "MATCH (parent:Resource), (child:Resource) WHERE parent.id = 101 AND child.id = 102 \
         CREATE (parent)-[:CONTAINS]->(child)",
    )
    .unwrap();

    let alice_access = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission)-[:APPLIES_TO]->(res:Resource)\
             -[:CONTAINS*1..3]->(child:Resource) \
             WHERE u.name = 'alice' AND p.name = 'configure' \
             RETURN child.name ORDER BY child.name",
        )
        .unwrap();
    let alice_children: Vec<&str> =
        alice_access.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(alice_children, vec!["config_column", "config_schema", "config_table"]);

    let eve_access = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission)-[:APPLIES_TO]->(res:Resource)\
             -[:CONTAINS*1..3]->(child:Resource) \
             WHERE u.name = 'eve' \
             RETURN child.name",
        )
        .unwrap();
    assert_eq!(eve_access.num_rows(), 0, "viewer 不应继承 config_db 子资源访问权");
}
