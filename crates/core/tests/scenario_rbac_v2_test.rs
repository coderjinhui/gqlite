/// 场景测试：企业权限管理 RBAC v2 (R-01 ~ R-11)
///
/// 覆盖高级权限场景：单边撤销、继承去重、deny/allow 冲突、多租户隔离、
/// 资源树深层授权、临时权限、批量变更、超级角色热点、审计链路、环路检测、级联收敛。
///
/// 节点类型：User, Role, Permission, Resource, Organization, Action, AuditEvent
/// 关系类型：HAS_ROLE, INHERITS, GRANTS, APPLIES_TO, BELONGS_TO, CONTAINS,
///           PERFORMED, ACTED_ON, LOGGED_AS
use gqlite_core::Database;

// ── R-01: 单独撤销角色边 ─────────────────────────────────────────
// 不删用户节点，仅移除一条 HAS_ROLE 边，验证权限即时收敛。
// 当前引擎不支持直接删边，采用 DETACH DELETE + 重建模式。

#[test]
fn r01_revoke_single_role_edge() {
    let db = Database::in_memory();

    // Schema
    db.execute("CREATE NODE TABLE User(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Role(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Permission(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE HAS_ROLE(FROM User TO Role)").unwrap();
    db.execute("CREATE REL TABLE GRANTS(FROM Role TO Permission)").unwrap();

    // Data: user alice with two roles (editor + viewer)
    db.execute("CREATE (u:User {id: 1, name: 'alice'})").unwrap();
    db.execute("CREATE (r:Role {id: 1, name: 'editor'})").unwrap();
    db.execute("CREATE (r:Role {id: 2, name: 'viewer'})").unwrap();
    db.execute("CREATE (p:Permission {id: 1, name: 'write'})").unwrap();
    db.execute("CREATE (p:Permission {id: 2, name: 'read'})").unwrap();

    db.execute(
        "MATCH (r:Role), (p:Permission) WHERE r.id = 1 AND p.id = 1 CREATE (r)-[:GRANTS]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (r:Role), (p:Permission) WHERE r.id = 2 AND p.id = 2 CREATE (r)-[:GRANTS]->(p)",
    )
    .unwrap();

    // Alice has both roles
    db.execute("MATCH (u:User), (r:Role) WHERE u.id = 1 AND r.id = 1 CREATE (u)-[:HAS_ROLE]->(r)")
        .unwrap();
    db.execute("MATCH (u:User), (r:Role) WHERE u.id = 1 AND r.id = 2 CREATE (u)-[:HAS_ROLE]->(r)")
        .unwrap();

    // Before: alice has 2 permissions (write + read)
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission) \
             WHERE u.name = 'alice' \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 2);
    let names: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(names, vec!["read", "write"]);

    // Revoke editor role: DETACH DELETE user, recreate with only viewer role
    db.execute("MATCH (u:User) WHERE u.id = 1 DETACH DELETE u").unwrap();
    db.execute("CREATE (u:User {id: 1, name: 'alice'})").unwrap();
    db.execute("MATCH (u:User), (r:Role) WHERE u.id = 1 AND r.id = 2 CREATE (u)-[:HAS_ROLE]->(r)")
        .unwrap();

    // After: alice has only 1 permission (read)
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission) \
             WHERE u.name = 'alice' \
             RETURN p.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 1);
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "read");

    // Roles and permissions still intact
    let r = db.query("MATCH (r:Role) RETURN count(*)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 2);
    let r = db.query("MATCH (p:Permission) RETURN count(*)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 2);

    // GRANTS edges still intact
    let r = db.query("MATCH (r:Role)-[:GRANTS]->(p:Permission) RETURN count(*)").unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 2);
}

// ── R-02: 直接授权与继承授权并存 ─────────────────────────────────
// 用户同时拥有直接权限和角色继承权限，通过 DISTINCT 去重。

#[test]
fn r02_direct_and_inherited_permission_dedup() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE User(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Role(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Permission(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE HAS_ROLE(FROM User TO Role)").unwrap();
    db.execute("CREATE REL TABLE INHERITS(FROM Role TO Role)").unwrap();
    db.execute("CREATE REL TABLE GRANTS(FROM Role TO Permission)").unwrap();

    // Roles: admin inherits from viewer
    db.execute("CREATE (r:Role {id: 1, name: 'admin'})").unwrap();
    db.execute("CREATE (r:Role {id: 2, name: 'viewer'})").unwrap();
    db.execute(
        "MATCH (a:Role), (b:Role) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:INHERITS]->(b)",
    )
    .unwrap();

    // Permissions
    db.execute("CREATE (p:Permission {id: 1, name: 'read'})").unwrap();
    db.execute("CREATE (p:Permission {id: 2, name: 'manage'})").unwrap();

    // admin grants: manage + read (direct)
    // viewer grants: read
    db.execute(
        "MATCH (r:Role), (p:Permission) WHERE r.id = 1 AND p.id = 2 CREATE (r)-[:GRANTS]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (r:Role), (p:Permission) WHERE r.id = 1 AND p.id = 1 CREATE (r)-[:GRANTS]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (r:Role), (p:Permission) WHERE r.id = 2 AND p.id = 1 CREATE (r)-[:GRANTS]->(p)",
    )
    .unwrap();

    // User bob has admin role
    db.execute("CREATE (u:User {id: 1, name: 'bob'})").unwrap();
    db.execute("MATCH (u:User), (r:Role) WHERE u.id = 1 AND r.id = 1 CREATE (u)-[:HAS_ROLE]->(r)")
        .unwrap();

    // Without DISTINCT: direct permissions (read, manage) + inherited via viewer (read) = 3 rows
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission) \
             WHERE u.name = 'bob' \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    let direct_names: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(direct_names, vec!["manage", "read"]);

    let r_inherited = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:INHERITS*1..2]->(parent:Role)\
             -[:GRANTS]->(p:Permission) \
             WHERE u.name = 'bob' \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    let inherited_names: Vec<&str> =
        r_inherited.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(inherited_names, vec!["read"]);

    // Combined with DISTINCT: collect both direct and inherited, dedup in Rust
    let mut all_perms: Vec<String> = Vec::new();
    for name in &direct_names {
        all_perms.push(name.to_string());
    }
    for name in &inherited_names {
        all_perms.push(name.to_string());
    }
    all_perms.sort();
    all_perms.dedup();

    // Effective permissions after dedup: manage, read (2 unique)
    assert_eq!(all_perms, vec!["manage", "read"]);
    assert_eq!(all_perms.len(), 2, "deduped effective permissions should be 2");
}

// ── R-03: deny/allow 冲突解析 ───────────────────────────────────
// 使用 GrantRule 节点携带 grant_type ('allow'/'deny') 和 priority 属性，
// Role -> GrantRule -> Permission 链路，deny 高优先级覆盖 allow。

#[test]
fn r03_deny_allow_conflict_resolution() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE User(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Role(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Permission(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute(
        "CREATE NODE TABLE GrantRule(id INT64, grant_type STRING, priority INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE HAS_ROLE(FROM User TO Role)").unwrap();
    db.execute("CREATE REL TABLE HAS_GRANT(FROM Role TO GrantRule)").unwrap();
    db.execute("CREATE REL TABLE GRANT_TARGET(FROM GrantRule TO Permission)").unwrap();

    // Roles: editor, restricted
    db.execute("CREATE (r:Role {id: 1, name: 'editor'})").unwrap();
    db.execute("CREATE (r:Role {id: 2, name: 'restricted'})").unwrap();

    // Permissions: write, delete, read
    db.execute("CREATE (p:Permission {id: 1, name: 'write'})").unwrap();
    db.execute("CREATE (p:Permission {id: 2, name: 'delete'})").unwrap();
    db.execute("CREATE (p:Permission {id: 3, name: 'read'})").unwrap();

    // GrantRules: editor allows write(10), delete(11), read(12); restricted denies delete(13)
    db.execute("CREATE (g:GrantRule {id: 10, grant_type: 'allow', priority: 10})").unwrap();
    db.execute("CREATE (g:GrantRule {id: 11, grant_type: 'allow', priority: 10})").unwrap();
    db.execute("CREATE (g:GrantRule {id: 12, grant_type: 'allow', priority: 10})").unwrap();
    db.execute("CREATE (g:GrantRule {id: 13, grant_type: 'deny', priority: 100})").unwrap();

    // editor -> grants (allow)
    db.execute(
        "MATCH (r:Role), (g:GrantRule) WHERE r.id = 1 AND g.id = 10 CREATE (r)-[:HAS_GRANT]->(g)",
    )
    .unwrap();
    db.execute(
        "MATCH (r:Role), (g:GrantRule) WHERE r.id = 1 AND g.id = 11 CREATE (r)-[:HAS_GRANT]->(g)",
    )
    .unwrap();
    db.execute(
        "MATCH (r:Role), (g:GrantRule) WHERE r.id = 1 AND g.id = 12 CREATE (r)-[:HAS_GRANT]->(g)",
    )
    .unwrap();
    // restricted -> deny grant
    db.execute(
        "MATCH (r:Role), (g:GrantRule) WHERE r.id = 2 AND g.id = 13 CREATE (r)-[:HAS_GRANT]->(g)",
    )
    .unwrap();

    // GrantRule -> Permission targets
    db.execute(
        "MATCH (g:GrantRule), (p:Permission) WHERE g.id = 10 AND p.id = 1 \
         CREATE (g)-[:GRANT_TARGET]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (g:GrantRule), (p:Permission) WHERE g.id = 11 AND p.id = 2 \
         CREATE (g)-[:GRANT_TARGET]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (g:GrantRule), (p:Permission) WHERE g.id = 12 AND p.id = 3 \
         CREATE (g)-[:GRANT_TARGET]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (g:GrantRule), (p:Permission) WHERE g.id = 13 AND p.id = 2 \
         CREATE (g)-[:GRANT_TARGET]->(p)",
    )
    .unwrap();

    // User charlie has both roles
    db.execute("CREATE (u:User {id: 1, name: 'charlie'})").unwrap();
    db.execute("MATCH (u:User), (r:Role) WHERE u.id = 1 AND r.id = 1 CREATE (u)-[:HAS_ROLE]->(r)")
        .unwrap();
    db.execute("MATCH (u:User), (r:Role) WHERE u.id = 1 AND r.id = 2 CREATE (u)-[:HAS_ROLE]->(r)")
        .unwrap();

    // Query all ALLOW grants: User -> Role -> GrantRule(allow) -> Permission
    let allows = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:HAS_GRANT]->(g:GrantRule)\
             -[:GRANT_TARGET]->(p:Permission) \
             WHERE u.name = 'charlie' AND g.grant_type = 'allow' \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    let allow_names: Vec<&str> =
        allows.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(allow_names, vec!["delete", "read", "write"]);

    // Query all DENY grants
    let denies = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:HAS_GRANT]->(g:GrantRule)\
             -[:GRANT_TARGET]->(p:Permission) \
             WHERE u.name = 'charlie' AND g.grant_type = 'deny' \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    let deny_names: Vec<&str> =
        denies.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(deny_names, vec!["delete"]);

    // Conflict resolution in Rust: deny overrides allow
    let mut effective: Vec<String> = Vec::new();
    for name in &allow_names {
        if !deny_names.contains(name) {
            effective.push(name.to_string());
        }
    }
    effective.sort();
    assert_eq!(effective, vec!["read", "write"]);
    assert!(
        !effective.contains(&"delete".to_string()),
        "delete should be denied"
    );

    // Verify high-priority deny via query: the deny grant for 'delete' has priority 100
    let deny_priority = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:HAS_GRANT]->(g:GrantRule)\
             -[:GRANT_TARGET]->(p:Permission) \
             WHERE u.name = 'charlie' AND g.grant_type = 'deny' AND p.name = 'delete' \
             RETURN g.priority",
        )
        .unwrap();
    assert_eq!(deny_priority.num_rows(), 1);
    assert_eq!(deny_priority.rows()[0].get_int(0).unwrap(), 100);
}

// ── R-04: 多租户角色隔离 ────────────────────────────────────────
// 相同角色名存在于不同 org，用户权限解析仅在所属租户子图。

#[test]
fn r04_multi_tenant_role_isolation() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE User(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Role(id INT64, name STRING, org_id INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE NODE TABLE Permission(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute(
        "CREATE NODE TABLE Organization(id INT64, name STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE HAS_ROLE(FROM User TO Role)").unwrap();
    db.execute("CREATE REL TABLE GRANTS(FROM Role TO Permission)").unwrap();
    db.execute("CREATE REL TABLE BELONGS_TO(FROM User TO Organization)").unwrap();

    // Two organizations
    db.execute("CREATE (o:Organization {id: 1, name: 'acme'})").unwrap();
    db.execute("CREATE (o:Organization {id: 2, name: 'globex'})").unwrap();

    // Same role name "editor" in both orgs, but different permissions
    db.execute("CREATE (r:Role {id: 1, name: 'editor', org_id: 1})").unwrap(); // acme editor
    db.execute("CREATE (r:Role {id: 2, name: 'editor', org_id: 2})").unwrap(); // globex editor

    // Permissions
    db.execute("CREATE (p:Permission {id: 1, name: 'edit_docs'})").unwrap();
    db.execute("CREATE (p:Permission {id: 2, name: 'edit_code'})").unwrap();
    db.execute("CREATE (p:Permission {id: 3, name: 'edit_wiki'})").unwrap();

    // acme editor -> edit_docs, edit_wiki
    db.execute(
        "MATCH (r:Role), (p:Permission) WHERE r.id = 1 AND p.id = 1 CREATE (r)-[:GRANTS]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (r:Role), (p:Permission) WHERE r.id = 1 AND p.id = 3 CREATE (r)-[:GRANTS]->(p)",
    )
    .unwrap();

    // globex editor -> edit_code
    db.execute(
        "MATCH (r:Role), (p:Permission) WHERE r.id = 2 AND p.id = 2 CREATE (r)-[:GRANTS]->(p)",
    )
    .unwrap();

    // Users: alice belongs to acme, bob belongs to globex
    db.execute("CREATE (u:User {id: 1, name: 'alice'})").unwrap();
    db.execute("CREATE (u:User {id: 2, name: 'bob'})").unwrap();
    db.execute(
        "MATCH (u:User), (o:Organization) WHERE u.id = 1 AND o.id = 1 \
         CREATE (u)-[:BELONGS_TO]->(o)",
    )
    .unwrap();
    db.execute(
        "MATCH (u:User), (o:Organization) WHERE u.id = 2 AND o.id = 2 \
         CREATE (u)-[:BELONGS_TO]->(o)",
    )
    .unwrap();

    // Assign roles (same name, different org)
    db.execute("MATCH (u:User), (r:Role) WHERE u.id = 1 AND r.id = 1 CREATE (u)-[:HAS_ROLE]->(r)")
        .unwrap();
    db.execute("MATCH (u:User), (r:Role) WHERE u.id = 2 AND r.id = 2 CREATE (u)-[:HAS_ROLE]->(r)")
        .unwrap();

    // alice belongs to org 1 (acme). Query her roles filtered by org_id = 1.
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission) \
             WHERE u.name = 'alice' AND r.org_id = 1 \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    let alice_perms: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(alice_perms, vec!["edit_docs", "edit_wiki"]);

    // Verify alice's org membership
    let r = db
        .query(
            "MATCH (u:User)-[:BELONGS_TO]->(o:Organization) \
             WHERE u.name = 'alice' \
             RETURN o.name",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "acme");

    // bob belongs to org 2 (globex). Query his roles filtered by org_id = 2.
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission) \
             WHERE u.name = 'bob' AND r.org_id = 2 \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    let bob_perms: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(bob_perms, vec!["edit_code"]);

    // Cross-check: alice should NOT see edit_code, bob should NOT see edit_docs/edit_wiki
    assert!(!alice_perms.contains(&"edit_code"));
    assert!(!bob_perms.contains(&"edit_docs"));
    assert!(!bob_perms.contains(&"edit_wiki"));

    // Cross-tenant query: alice's roles in globex (org 2) should return nothing
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission) \
             WHERE u.name = 'alice' AND r.org_id = 2 \
             RETURN p.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 0, "alice should have no roles in globex");
}

// ── R-05: 资源树深层授权 ────────────────────────────────────────
// 资源目录树 6 层深，父资源授权对子资源生效。

#[test]
fn r05_deep_resource_tree_authorization() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE User(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Role(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Permission(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Resource(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE HAS_ROLE(FROM User TO Role)").unwrap();
    db.execute("CREATE REL TABLE GRANTS(FROM Role TO Permission)").unwrap();
    db.execute("CREATE REL TABLE APPLIES_TO(FROM Permission TO Resource)").unwrap();
    db.execute("CREATE REL TABLE CONTAINS(FROM Resource TO Resource)").unwrap();

    // Build a 6-level resource tree:
    // root(1) -> dept(2) -> project(3) -> module(4) -> package(5) -> file(6) -> function(7)
    db.execute("CREATE (r:Resource {id: 1, name: 'root'})").unwrap();
    db.execute("CREATE (r:Resource {id: 2, name: 'dept'})").unwrap();
    db.execute("CREATE (r:Resource {id: 3, name: 'project'})").unwrap();
    db.execute("CREATE (r:Resource {id: 4, name: 'module'})").unwrap();
    db.execute("CREATE (r:Resource {id: 5, name: 'package'})").unwrap();
    db.execute("CREATE (r:Resource {id: 6, name: 'file'})").unwrap();
    db.execute("CREATE (r:Resource {id: 7, name: 'function'})").unwrap();

    let contains_pairs = [(1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7)];
    for (parent, child) in &contains_pairs {
        db.execute(&format!(
            "MATCH (a:Resource), (b:Resource) WHERE a.id = {} AND b.id = {} \
             CREATE (a)-[:CONTAINS]->(b)",
            parent, child
        ))
        .unwrap();
    }

    // Permission: manage -> root resource
    db.execute("CREATE (p:Permission {id: 1, name: 'manage'})").unwrap();
    db.execute(
        "MATCH (p:Permission), (r:Resource) WHERE p.id = 1 AND r.id = 1 \
         CREATE (p)-[:APPLIES_TO]->(r)",
    )
    .unwrap();

    // Role: admin with manage permission
    db.execute("CREATE (r:Role {id: 1, name: 'admin'})").unwrap();
    db.execute(
        "MATCH (r:Role), (p:Permission) WHERE r.id = 1 AND p.id = 1 CREATE (r)-[:GRANTS]->(p)",
    )
    .unwrap();

    // User: alice has admin role
    db.execute("CREATE (u:User {id: 1, name: 'alice'})").unwrap();
    db.execute("MATCH (u:User), (r:Role) WHERE u.id = 1 AND r.id = 1 CREATE (u)-[:HAS_ROLE]->(r)")
        .unwrap();

    // Verify: alice can access all 6 child resources via CONTAINS*1..6
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(role:Role)-[:GRANTS]->(p:Permission)\
             -[:APPLIES_TO]->(res:Resource)-[:CONTAINS*1..6]->(child:Resource) \
             WHERE u.name = 'alice' \
             RETURN child.name ORDER BY child.name",
        )
        .unwrap();
    let children: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(children, vec!["dept", "file", "function", "module", "package", "project"]);
    assert_eq!(children.len(), 6, "should propagate to all 6 child levels");

    // Verify the deepest resource (function) is reachable
    assert!(children.contains(&"function"), "deepest level should be accessible");

    // User without root access should see nothing
    db.execute("CREATE (u:User {id: 2, name: 'guest'})").unwrap();
    db.execute("CREATE (r:Role {id: 2, name: 'guest_role'})").unwrap();
    db.execute("MATCH (u:User), (r:Role) WHERE u.id = 2 AND r.id = 2 CREATE (u)-[:HAS_ROLE]->(r)")
        .unwrap();

    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(role:Role)-[:GRANTS]->(p:Permission)\
             -[:APPLIES_TO]->(res:Resource)-[:CONTAINS*1..6]->(child:Resource) \
             WHERE u.name = 'guest' \
             RETURN child.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 0, "guest should have no resource access");
}

// ── R-06: 临时权限生效/失效 ─────────────────────────────────────
// 使用 GrantPolicy 节点携带 valid_from / valid_to 属性，
// Role -> GrantPolicy -> Permission 链路，按模拟当前时间过滤。

#[test]
fn r06_temporal_permission_validity() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE User(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Role(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Permission(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute(
        "CREATE NODE TABLE GrantPolicy(id INT64, valid_from INT64, valid_to INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE HAS_ROLE(FROM User TO Role)").unwrap();
    db.execute("CREATE REL TABLE HAS_POLICY(FROM Role TO GrantPolicy)").unwrap();
    db.execute("CREATE REL TABLE POLICY_TARGET(FROM GrantPolicy TO Permission)").unwrap();

    // Role and permissions
    db.execute("CREATE (r:Role {id: 1, name: 'temp_editor'})").unwrap();
    db.execute("CREATE (p:Permission {id: 1, name: 'edit'})").unwrap();
    db.execute("CREATE (p:Permission {id: 2, name: 'publish'})").unwrap();
    db.execute("CREATE (p:Permission {id: 3, name: 'archive'})").unwrap();

    // GrantPolicy nodes with temporal validity:
    //   now = 1000
    //   edit: valid 500..1500 (active at 1000)
    //   publish: valid 500..900 (expired at 1000)
    //   archive: valid 1100..2000 (not yet active at 1000)
    db.execute("CREATE (g:GrantPolicy {id: 1, valid_from: 500, valid_to: 1500})").unwrap();
    db.execute("CREATE (g:GrantPolicy {id: 2, valid_from: 500, valid_to: 900})").unwrap();
    db.execute("CREATE (g:GrantPolicy {id: 3, valid_from: 1100, valid_to: 2000})").unwrap();

    // Role -> GrantPolicy edges
    db.execute(
        "MATCH (r:Role), (g:GrantPolicy) WHERE r.id = 1 AND g.id = 1 \
         CREATE (r)-[:HAS_POLICY]->(g)",
    )
    .unwrap();
    db.execute(
        "MATCH (r:Role), (g:GrantPolicy) WHERE r.id = 1 AND g.id = 2 \
         CREATE (r)-[:HAS_POLICY]->(g)",
    )
    .unwrap();
    db.execute(
        "MATCH (r:Role), (g:GrantPolicy) WHERE r.id = 1 AND g.id = 3 \
         CREATE (r)-[:HAS_POLICY]->(g)",
    )
    .unwrap();

    // GrantPolicy -> Permission edges
    db.execute(
        "MATCH (g:GrantPolicy), (p:Permission) WHERE g.id = 1 AND p.id = 1 \
         CREATE (g)-[:POLICY_TARGET]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (g:GrantPolicy), (p:Permission) WHERE g.id = 2 AND p.id = 2 \
         CREATE (g)-[:POLICY_TARGET]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (g:GrantPolicy), (p:Permission) WHERE g.id = 3 AND p.id = 3 \
         CREATE (g)-[:POLICY_TARGET]->(p)",
    )
    .unwrap();

    // User with the temp role
    db.execute("CREATE (u:User {id: 1, name: 'diana'})").unwrap();
    db.execute("MATCH (u:User), (r:Role) WHERE u.id = 1 AND r.id = 1 CREATE (u)-[:HAS_ROLE]->(r)")
        .unwrap();

    // At time=1000: only 'edit' should be active (500 <= 1000 <= 1500)
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:HAS_POLICY]->(g:GrantPolicy)\
             -[:POLICY_TARGET]->(p:Permission) \
             WHERE u.name = 'diana' AND g.valid_from <= 1000 AND g.valid_to >= 1000 \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    let active: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(active, vec!["edit"], "at time=1000, only 'edit' is active");

    // At time=800: edit + publish should be active
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:HAS_POLICY]->(g:GrantPolicy)\
             -[:POLICY_TARGET]->(p:Permission) \
             WHERE u.name = 'diana' AND g.valid_from <= 800 AND g.valid_to >= 800 \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    let active_800: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(active_800, vec!["edit", "publish"], "at time=800, edit+publish active");

    // At time=1200: edit + archive should be active
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:HAS_POLICY]->(g:GrantPolicy)\
             -[:POLICY_TARGET]->(p:Permission) \
             WHERE u.name = 'diana' AND g.valid_from <= 1200 AND g.valid_to >= 1200 \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    let active_1200: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(active_1200, vec!["archive", "edit"], "at time=1200, edit+archive active");

    // At time=2500: nothing should be active
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:HAS_POLICY]->(g:GrantPolicy)\
             -[:POLICY_TARGET]->(p:Permission) \
             WHERE u.name = 'diana' AND g.valid_from <= 2500 AND g.valid_to >= 2500 \
             RETURN p.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 0, "at time=2500, all permissions expired");
}

// ── R-07: 批量加角色/撤角色 ─────────────────────────────────────
// 一个事务中给用户添加 5 个角色再移除 3 个，验证最终权限集。

#[test]
fn r07_batch_role_changes_in_transaction() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE User(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Role(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Permission(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE HAS_ROLE(FROM User TO Role)").unwrap();
    db.execute("CREATE REL TABLE GRANTS(FROM Role TO Permission)").unwrap();

    // 5 roles with distinct permissions
    let role_perm = [
        (1, "role_a", 1, "perm_a"),
        (2, "role_b", 2, "perm_b"),
        (3, "role_c", 3, "perm_c"),
        (4, "role_d", 4, "perm_d"),
        (5, "role_e", 5, "perm_e"),
    ];
    for (rid, rname, pid, pname) in &role_perm {
        db.execute(&format!(
            "CREATE (r:Role {{id: {}, name: '{}'}})",
            rid, rname
        ))
        .unwrap();
        db.execute(&format!(
            "CREATE (p:Permission {{id: {}, name: '{}'}})",
            pid, pname
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (r:Role), (p:Permission) WHERE r.id = {} AND p.id = {} \
             CREATE (r)-[:GRANTS]->(p)",
            rid, pid
        ))
        .unwrap();
    }

    // User eve starts with no roles
    db.execute("CREATE (u:User {id: 1, name: 'eve'})").unwrap();

    // In a single transaction: add all 5 roles
    db.execute_script(
        "BEGIN; \
         MATCH (u:User), (r:Role) WHERE u.id = 1 AND r.id = 1 CREATE (u)-[:HAS_ROLE]->(r); \
         MATCH (u:User), (r:Role) WHERE u.id = 1 AND r.id = 2 CREATE (u)-[:HAS_ROLE]->(r); \
         MATCH (u:User), (r:Role) WHERE u.id = 1 AND r.id = 3 CREATE (u)-[:HAS_ROLE]->(r); \
         MATCH (u:User), (r:Role) WHERE u.id = 1 AND r.id = 4 CREATE (u)-[:HAS_ROLE]->(r); \
         MATCH (u:User), (r:Role) WHERE u.id = 1 AND r.id = 5 CREATE (u)-[:HAS_ROLE]->(r); \
         COMMIT;",
    )
    .unwrap();

    // Verify all 5 roles assigned
    let r = db
        .query("MATCH (u:User)-[:HAS_ROLE]->(r:Role) WHERE u.name = 'eve' RETURN count(*)")
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 5);

    // All 5 permissions
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission) \
             WHERE u.name = 'eve' \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 5);

    // Now remove 3 roles (b, c, d) by DETACH DELETE + recreate with only a, e
    db.execute("MATCH (u:User) WHERE u.id = 1 DETACH DELETE u").unwrap();
    db.execute("CREATE (u:User {id: 1, name: 'eve'})").unwrap();

    db.execute_script(
        "BEGIN; \
         MATCH (u:User), (r:Role) WHERE u.id = 1 AND r.id = 1 CREATE (u)-[:HAS_ROLE]->(r); \
         MATCH (u:User), (r:Role) WHERE u.id = 1 AND r.id = 5 CREATE (u)-[:HAS_ROLE]->(r); \
         COMMIT;",
    )
    .unwrap();

    // Verify final permission set: perm_a + perm_e
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission) \
             WHERE u.name = 'eve' \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    let final_perms: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(final_perms, vec!["perm_a", "perm_e"]);

    // Verify role count is now 2
    let r = db
        .query("MATCH (u:User)-[:HAS_ROLE]->(r:Role) WHERE u.name = 'eve' RETURN count(*)")
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 2);
}

// ── R-08: 超级角色热点读 ────────────────────────────────────────
// super_admin 挂载 200+ 权限，验证查询正确不退化。

#[test]
fn r08_super_admin_hotspot_query() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE User(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Role(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Permission(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE HAS_ROLE(FROM User TO Role)").unwrap();
    db.execute("CREATE REL TABLE GRANTS(FROM Role TO Permission)").unwrap();

    // super_admin role
    db.execute("CREATE (r:Role {id: 1, name: 'super_admin'})").unwrap();

    // Create 210 permissions and link them all to super_admin
    for i in 1..=210 {
        db.execute(&format!(
            "CREATE (p:Permission {{id: {}, name: 'perm_{}'}})",
            i, i
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (r:Role), (p:Permission) WHERE r.id = 1 AND p.id = {} \
             CREATE (r)-[:GRANTS]->(p)",
            i
        ))
        .unwrap();
    }

    // User with super_admin role
    db.execute("CREATE (u:User {id: 1, name: 'admin_user'})").unwrap();
    db.execute("MATCH (u:User), (r:Role) WHERE u.id = 1 AND r.id = 1 CREATE (u)-[:HAS_ROLE]->(r)")
        .unwrap();

    // Query all permissions for the super_admin user
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission) \
             WHERE u.name = 'admin_user' \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 210, "super_admin should have all 210 permissions");

    // Verify first and last
    let first = r.rows()[0].get_string(0).unwrap();
    assert_eq!(first, "perm_1");
    // Just verify we have 210 distinct results
    let names: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    let mut unique = names.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), 210, "all 210 permissions should be unique");

    // Query count to verify aggregation also works
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission) \
             WHERE u.name = 'admin_user' \
             RETURN count(*)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 210);

    // Verify another user without super_admin gets no permissions
    db.execute("CREATE (u:User {id: 2, name: 'regular_user'})").unwrap();
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission) \
             WHERE u.name = 'regular_user' \
             RETURN count(*)",
        )
        .unwrap();
    assert_eq!(r.rows()[0].get_int(0).unwrap(), 0);
}

// ── R-09: 审计链路图 ───────────────────────────────────────────
// User → Action → Resource → AuditEvent，多跳追溯查询。

#[test]
fn r09_audit_trail_multi_hop_trace() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE User(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute(
        "CREATE NODE TABLE Action(id INT64, name STRING, timestamp INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE NODE TABLE Resource(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute(
        "CREATE NODE TABLE AuditEvent(id INT64, event_type STRING, detail STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE PERFORMED(FROM User TO Action)").unwrap();
    db.execute("CREATE REL TABLE ACTED_ON(FROM Action TO Resource)").unwrap();
    db.execute("CREATE REL TABLE LOGGED_AS(FROM Action TO AuditEvent)").unwrap();

    // Users
    db.execute("CREATE (u:User {id: 1, name: 'alice'})").unwrap();
    db.execute("CREATE (u:User {id: 2, name: 'bob'})").unwrap();

    // Resources
    db.execute("CREATE (r:Resource {id: 1, name: 'user_db'})").unwrap();
    db.execute("CREATE (r:Resource {id: 2, name: 'config_db'})").unwrap();

    // Actions performed by alice
    db.execute("CREATE (a:Action {id: 1, name: 'grant_role', timestamp: 1000})").unwrap();
    db.execute("CREATE (a:Action {id: 2, name: 'revoke_role', timestamp: 1100})").unwrap();
    db.execute("CREATE (a:Action {id: 3, name: 'modify_config', timestamp: 1200})").unwrap();

    // Action performed by bob
    db.execute("CREATE (a:Action {id: 4, name: 'read_data', timestamp: 1300})").unwrap();

    // User -> Action edges
    db.execute(
        "MATCH (u:User), (a:Action) WHERE u.id = 1 AND a.id = 1 CREATE (u)-[:PERFORMED]->(a)",
    )
    .unwrap();
    db.execute(
        "MATCH (u:User), (a:Action) WHERE u.id = 1 AND a.id = 2 CREATE (u)-[:PERFORMED]->(a)",
    )
    .unwrap();
    db.execute(
        "MATCH (u:User), (a:Action) WHERE u.id = 1 AND a.id = 3 CREATE (u)-[:PERFORMED]->(a)",
    )
    .unwrap();
    db.execute(
        "MATCH (u:User), (a:Action) WHERE u.id = 2 AND a.id = 4 CREATE (u)-[:PERFORMED]->(a)",
    )
    .unwrap();

    // Action -> Resource edges
    db.execute(
        "MATCH (a:Action), (r:Resource) WHERE a.id = 1 AND r.id = 1 CREATE (a)-[:ACTED_ON]->(r)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Action), (r:Resource) WHERE a.id = 2 AND r.id = 1 CREATE (a)-[:ACTED_ON]->(r)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Action), (r:Resource) WHERE a.id = 3 AND r.id = 2 CREATE (a)-[:ACTED_ON]->(r)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Action), (r:Resource) WHERE a.id = 4 AND r.id = 1 CREATE (a)-[:ACTED_ON]->(r)",
    )
    .unwrap();

    // Action -> AuditEvent edges
    db.execute(
        "CREATE (e:AuditEvent {id: 1, event_type: 'role_change', detail: 'granted admin to bob'})",
    )
    .unwrap();
    db.execute(
        "CREATE (e:AuditEvent {id: 2, event_type: 'role_change', detail: 'revoked editor from charlie'})",
    )
    .unwrap();
    db.execute(
        "CREATE (e:AuditEvent {id: 3, event_type: 'config_change', detail: 'updated db timeout'})",
    )
    .unwrap();
    db.execute(
        "CREATE (e:AuditEvent {id: 4, event_type: 'data_access', detail: 'read user records'})",
    )
    .unwrap();

    db.execute(
        "MATCH (a:Action), (e:AuditEvent) WHERE a.id = 1 AND e.id = 1 \
         CREATE (a)-[:LOGGED_AS]->(e)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Action), (e:AuditEvent) WHERE a.id = 2 AND e.id = 2 \
         CREATE (a)-[:LOGGED_AS]->(e)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Action), (e:AuditEvent) WHERE a.id = 3 AND e.id = 3 \
         CREATE (a)-[:LOGGED_AS]->(e)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Action), (e:AuditEvent) WHERE a.id = 4 AND e.id = 4 \
         CREATE (a)-[:LOGGED_AS]->(e)",
    )
    .unwrap();

    // Multi-hop trace: find all audit events for alice's actions on user_db
    // Path 1: alice -> action -> user_db (to filter actions on user_db)
    // Path 2: action -> audit_event (to get the event details)
    // Since comma-separated MATCH is not supported, use two separate queries.

    // Step 1: find action IDs for alice on user_db
    let r_actions = db
        .query(
            "MATCH (u:User)-[:PERFORMED]->(a:Action)-[:ACTED_ON]->(res:Resource) \
             WHERE u.name = 'alice' AND res.name = 'user_db' \
             RETURN a.id ORDER BY a.id",
        )
        .unwrap();
    assert_eq!(r_actions.num_rows(), 2, "alice performed 2 actions on user_db");
    let action_ids: Vec<i64> = r_actions
        .rows()
        .iter()
        .map(|row| row.get_int(0).unwrap())
        .collect();
    assert_eq!(action_ids, vec![1, 2]);

    // Step 2: for each action, get the audit event
    let r_event1 = db
        .query(
            "MATCH (a:Action)-[:LOGGED_AS]->(e:AuditEvent) \
             WHERE a.id = 1 \
             RETURN a.name, e.event_type, e.detail",
        )
        .unwrap();
    assert_eq!(r_event1.num_rows(), 1);
    assert_eq!(r_event1.rows()[0].get_string(0).unwrap(), "grant_role");
    assert_eq!(r_event1.rows()[0].get_string(1).unwrap(), "role_change");

    let r_event2 = db
        .query(
            "MATCH (a:Action)-[:LOGGED_AS]->(e:AuditEvent) \
             WHERE a.id = 2 \
             RETURN a.name, e.event_type, e.detail",
        )
        .unwrap();
    assert_eq!(r_event2.num_rows(), 1);
    assert_eq!(r_event2.rows()[0].get_string(0).unwrap(), "revoke_role");
    assert_eq!(r_event2.rows()[0].get_string(1).unwrap(), "role_change");

    // Trace: who performed actions on config_db?
    let r = db
        .query(
            "MATCH (u:User)-[:PERFORMED]->(a:Action)-[:ACTED_ON]->(res:Resource) \
             WHERE res.name = 'config_db' \
             RETURN u.name, a.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 1);
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "alice");
    assert_eq!(r.rows()[0].get_string(1).unwrap(), "modify_config");

    // Trace: all audit events by user bob
    // Step 1: get bob's action IDs
    let bob_actions = db
        .query(
            "MATCH (u:User)-[:PERFORMED]->(a:Action) \
             WHERE u.name = 'bob' \
             RETURN a.id",
        )
        .unwrap();
    assert_eq!(bob_actions.num_rows(), 1);
    let bob_action_id = bob_actions.rows()[0].get_int(0).unwrap();
    assert_eq!(bob_action_id, 4);

    // Step 2: get audit event for bob's action
    let bob_event = db
        .query(&format!(
            "MATCH (a:Action)-[:LOGGED_AS]->(e:AuditEvent) \
             WHERE a.id = {} \
             RETURN e.detail",
            bob_action_id
        ))
        .unwrap();
    assert_eq!(bob_event.num_rows(), 1);
    assert_eq!(bob_event.rows()[0].get_string(0).unwrap(), "read user records");
}

// ── R-10: 角色继承环路检测 ──────────────────────────────────────
// 构造 A→B→C→A 循环继承链，验证可变长路径查询不死循环不 panic，
// 且可通过查询检测到环路存在。
// 注意：BFS 使用 visited set 去重，起始节点预标记为已访问，
// 因此不会返回起始节点本身。

#[test]
fn r10_circular_role_inheritance_detection() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE Role(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE INHERITS(FROM Role TO Role)").unwrap();

    // Create 3 roles forming a cycle: admin -> manager -> lead -> admin
    db.execute("CREATE (r:Role {id: 1, name: 'admin'})").unwrap();
    db.execute("CREATE (r:Role {id: 2, name: 'manager'})").unwrap();
    db.execute("CREATE (r:Role {id: 3, name: 'lead'})").unwrap();

    db.execute(
        "MATCH (a:Role), (b:Role) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:INHERITS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Role), (b:Role) WHERE a.id = 2 AND b.id = 3 CREATE (a)-[:INHERITS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Role), (b:Role) WHERE a.id = 3 AND b.id = 1 CREATE (a)-[:INHERITS]->(b)",
    )
    .unwrap();

    // Variable-length path query should NOT loop/panic
    let result = db.query(
        "MATCH (r:Role)-[:INHERITS*1..10]->(t:Role) \
         WHERE r.name = 'admin' \
         RETURN t.name ORDER BY t.name",
    );

    // The query must not panic — it should return a result
    assert!(result.is_ok(), "variable-length path on cycle must not panic");

    let r = result.unwrap();
    // BFS: starting from admin (visited={admin}), explores manager (visit), lead (visit).
    // lead->admin is skipped because admin is already in visited set.
    // So we see manager and lead, but NOT admin itself.
    assert!(r.num_rows() >= 2, "should return at least manager and lead");

    let mut names: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    names.sort();
    names.dedup();

    assert!(names.contains(&"manager"), "admin should reach manager");
    assert!(names.contains(&"lead"), "admin should reach lead");

    // From manager: BFS visits lead, then admin (since manager is start, visited={manager}).
    // admin->manager is skipped (manager already visited). So manager sees: lead, admin.
    let r = db
        .query(
            "MATCH (r:Role)-[:INHERITS*1..10]->(t:Role) \
             WHERE r.name = 'manager' \
             RETURN t.name ORDER BY t.name",
        )
        .unwrap();
    let mut m_names: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    m_names.sort();
    m_names.dedup();
    assert!(m_names.contains(&"lead"), "manager should reach lead");
    assert!(m_names.contains(&"admin"), "manager should reach admin");

    // Cycle detection: each role can reach all other roles in the cycle.
    // admin reaches {manager, lead}; manager reaches {lead, admin}; lead reaches {admin, manager}
    // All three roles being mutually reachable indicates a cycle.
    let r = db
        .query(
            "MATCH (r:Role)-[:INHERITS*1..10]->(t:Role) \
             WHERE r.name = 'lead' \
             RETURN t.name ORDER BY t.name",
        )
        .unwrap();
    let mut l_names: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    l_names.sort();
    l_names.dedup();
    assert!(l_names.contains(&"admin"), "lead should reach admin");
    assert!(l_names.contains(&"manager"), "lead should reach manager");

    // Verify termination: all queries completed without hanging.
    // The fact that we reach this point proves BFS terminated correctly on the cycle.
}

// ── R-11: 中间角色删除后权限级联收敛 ────────────────────────────
// 删除继承链中间的角色节点 (DETACH DELETE)，
// 验证下游用户权限正确收缩，无悬挂边。

#[test]
fn r11_mid_chain_role_deletion_cascade() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE User(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Role(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Permission(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE HAS_ROLE(FROM User TO Role)").unwrap();
    db.execute("CREATE REL TABLE INHERITS(FROM Role TO Role)").unwrap();
    db.execute("CREATE REL TABLE GRANTS(FROM Role TO Permission)").unwrap();

    // Inheritance chain: RoleA -> RoleB -> RoleC
    db.execute("CREATE (r:Role {id: 1, name: 'role_a'})").unwrap();
    db.execute("CREATE (r:Role {id: 2, name: 'role_b'})").unwrap();
    db.execute("CREATE (r:Role {id: 3, name: 'role_c'})").unwrap();

    db.execute(
        "MATCH (a:Role), (b:Role) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:INHERITS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Role), (b:Role) WHERE a.id = 2 AND b.id = 3 CREATE (a)-[:INHERITS]->(b)",
    )
    .unwrap();

    // Permissions: role_a has perm_a (directly), role_b has perm_x, role_c has perm_y
    db.execute("CREATE (p:Permission {id: 1, name: 'perm_a'})").unwrap();
    db.execute("CREATE (p:Permission {id: 2, name: 'perm_x'})").unwrap();
    db.execute("CREATE (p:Permission {id: 3, name: 'perm_y'})").unwrap();

    db.execute(
        "MATCH (r:Role), (p:Permission) WHERE r.id = 1 AND p.id = 1 CREATE (r)-[:GRANTS]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (r:Role), (p:Permission) WHERE r.id = 2 AND p.id = 2 CREATE (r)-[:GRANTS]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (r:Role), (p:Permission) WHERE r.id = 3 AND p.id = 3 CREATE (r)-[:GRANTS]->(p)",
    )
    .unwrap();

    // User has role_a
    db.execute("CREATE (u:User {id: 1, name: 'frank'})").unwrap();
    db.execute("MATCH (u:User), (r:Role) WHERE u.id = 1 AND r.id = 1 CREATE (u)-[:HAS_ROLE]->(r)")
        .unwrap();

    // Before deletion: frank has perm_a (direct on role_a) +
    // perm_x (via role_a->role_b) + perm_y (via role_a->role_b->role_c)
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission) \
             WHERE u.name = 'frank' \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    let direct: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(direct, vec!["perm_a"], "direct permissions of role_a");

    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:INHERITS*1..2]->(parent:Role)\
             -[:GRANTS]->(p:Permission) \
             WHERE u.name = 'frank' \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    let inherited: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(inherited, vec!["perm_x", "perm_y"], "inherited from role_b and role_c");

    // Collect all effective permissions before deletion
    let mut before_perms: Vec<String> = Vec::new();
    for n in &direct {
        before_perms.push(n.to_string());
    }
    for n in &inherited {
        before_perms.push(n.to_string());
    }
    before_perms.sort();
    assert_eq!(before_perms, vec!["perm_a", "perm_x", "perm_y"]);

    // Delete role_b (mid-chain) with DETACH DELETE
    db.execute("MATCH (r:Role) WHERE r.id = 2 DETACH DELETE r").unwrap();

    // After deletion: role_b is gone, breaking the chain
    let r = db.query("MATCH (r:Role) RETURN r.name ORDER BY r.name").unwrap();
    let roles: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(roles, vec!["role_a", "role_c"], "role_b should be deleted");

    // frank's direct permission from role_a remains
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:GRANTS]->(p:Permission) \
             WHERE u.name = 'frank' \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    let after_direct: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    assert_eq!(after_direct, vec!["perm_a"], "frank still has role_a's direct perm");

    // frank's inherited permissions should be empty (chain broken)
    let r = db
        .query(
            "MATCH (u:User)-[:HAS_ROLE]->(r:Role)-[:INHERITS*1..2]->(parent:Role)\
             -[:GRANTS]->(p:Permission) \
             WHERE u.name = 'frank' \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    assert_eq!(
        r.num_rows(),
        0,
        "no inherited permissions after mid-chain deletion"
    );

    // No dangling INHERITS edges from role_a (its target role_b was DETACH DELETEd)
    let r = db
        .query("MATCH (a:Role)-[:INHERITS]->(b:Role) RETURN a.name, b.name")
        .unwrap();
    // The only INHERITS that should remain is role_b->role_c, but role_b is deleted.
    // role_a->role_b was cleaned by DETACH DELETE of role_b.
    // role_b->role_c was cleaned by DETACH DELETE of role_b.
    // So no INHERITS edges should remain.
    assert_eq!(r.num_rows(), 0, "no dangling INHERITS edges");

    // No dangling GRANTS edges for deleted role_b
    let r = db
        .query("MATCH (r:Role)-[:GRANTS]->(p:Permission) RETURN r.name, p.name ORDER BY r.name")
        .unwrap();
    let grant_roles: Vec<&str> = r.rows().iter().map(|row| row.get_string(0).unwrap()).collect();
    // Only role_a and role_c should have GRANTS
    for role in &grant_roles {
        assert_ne!(*role, "role_b", "no grants from deleted role_b");
    }
    assert_eq!(r.num_rows(), 2, "role_a->perm_a and role_c->perm_y remain");
}
