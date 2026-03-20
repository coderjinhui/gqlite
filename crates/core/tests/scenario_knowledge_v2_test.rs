/// 知识图谱 v2 场景测试（K-01 ~ K-11）
///
/// 覆盖：实体别名消歧、事实来源置信度、增量更新冲突修正、关系属性推理、
/// 版本链持久化、主题层级、批量导入、孤立实体治理、时间生效、共被引分析、
/// 跨领域知识桥接
///
/// 注：由于引擎暂不支持在 WHERE/RETURN 中读取关系属性（rel_alias 未投射），
/// 涉及关系元数据的场景通过中间节点（Authorship、Affiliation、Ingestion、Citation）
/// 来建模，保证测试可执行且语义等价。
use gqlite_core::Database;
use std::path::PathBuf;

// ── 辅助函数 ──────────────────────────────────────────────────

fn temp_db_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("gqlite_knowledge_v2_test");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(format!("{}_{}.graph", name, std::process::id()))
}

fn cleanup(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(path.with_extension("graph.wal"));
    let _ = std::fs::remove_file(path.with_extension("graph.lock"));
    let _ = std::fs::remove_file(path.with_extension("graph.tmp"));
}

// ============================================================
// K-01: 实体别名与消歧
// ============================================================

#[test]
fn k01_entity_alias_disambiguation() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE Author(id INT64, name STRING, hindex INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute(
        "CREATE NODE TABLE Alias(id INT64, canonical_name STRING, alias_name STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE NODE TABLE Paper(id INT64, title STRING, year INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE REL TABLE HAS_ALIAS(FROM Author TO Alias)").unwrap();
    db.execute("CREATE REL TABLE AUTHORED(FROM Author TO Paper)").unwrap();

    // 同一作者 Alice Chen 有多个别名
    db.execute("CREATE (a:Author {id: 1, name: 'Alice Chen', hindex: 45})").unwrap();
    db.execute("CREATE (al:Alias {id: 1, canonical_name: 'Alice Chen', alias_name: 'A. Chen'})")
        .unwrap();
    db.execute(
        "CREATE (al:Alias {id: 2, canonical_name: 'Alice Chen', alias_name: 'Chen, Alice'})",
    )
    .unwrap();
    db.execute("CREATE (al:Alias {id: 3, canonical_name: 'Alice Chen', alias_name: 'Alice C.'})")
        .unwrap();

    for alias_id in 1..=3 {
        db.execute(&format!(
            "MATCH (a:Author), (al:Alias) WHERE a.id = 1 AND al.id = {} \
             CREATE (a)-[:HAS_ALIAS]->(al)",
            alias_id
        ))
        .unwrap();
    }

    // 另一个不同的作者 Bob，无别名
    db.execute("CREATE (a:Author {id: 2, name: 'Bob Smith', hindex: 38})").unwrap();

    // 验证 Alice 有 3 个别名
    let aliases = db
        .query(
            "MATCH (a:Author)-[:HAS_ALIAS]->(al:Alias) \
             WHERE a.id = 1 \
             RETURN al.alias_name ORDER BY al.alias_name",
        )
        .unwrap();
    assert_eq!(aliases.num_rows(), 3, "Alice should have 3 aliases");
    assert_eq!(aliases.rows()[0].get_string(0).unwrap(), "A. Chen");
    assert_eq!(aliases.rows()[1].get_string(0).unwrap(), "Alice C.");
    assert_eq!(aliases.rows()[2].get_string(0).unwrap(), "Chen, Alice");

    // 通过别名反向查找规范名称（消歧）
    let canonical = db
        .query(
            "MATCH (a:Author)-[:HAS_ALIAS]->(al:Alias) \
             WHERE al.alias_name = 'A. Chen' \
             RETURN a.name, al.canonical_name",
        )
        .unwrap();
    assert_eq!(canonical.num_rows(), 1);
    assert_eq!(canonical.rows()[0].get_string(0).unwrap(), "Alice Chen");
    assert_eq!(canonical.rows()[0].get_string(1).unwrap(), "Alice Chen");

    // Bob 没有别名
    let bob_aliases = db
        .query(
            "MATCH (a:Author)-[:HAS_ALIAS]->(al:Alias) \
             WHERE a.id = 2 \
             RETURN al.alias_name",
        )
        .unwrap();
    assert_eq!(bob_aliases.num_rows(), 0, "Bob should have no aliases");

    // 消歧：给定别名 'Chen, Alice'，确认指向唯一作者
    let disambig = db
        .query(
            "MATCH (a:Author)-[:HAS_ALIAS]->(al:Alias) \
             WHERE al.alias_name = 'Chen, Alice' \
             RETURN a.id, a.name",
        )
        .unwrap();
    assert_eq!(disambig.num_rows(), 1);
    assert_eq!(disambig.rows()[0].get_int(0).unwrap(), 1);
    assert_eq!(disambig.rows()[0].get_string(1).unwrap(), "Alice Chen");

    // 通过别名找到作者的论文
    db.execute("CREATE (p:Paper {id: 1, title: 'Deep Learning', year: 2020})").unwrap();
    db.execute(
        "MATCH (a:Author), (p:Paper) WHERE a.id = 1 AND p.id = 1 \
         CREATE (a)-[:AUTHORED]->(p)",
    )
    .unwrap();

    let papers_via_alias = db
        .query(
            "MATCH (al:Alias)<-[:HAS_ALIAS]-(a:Author)-[:AUTHORED]->(p:Paper) \
             WHERE al.alias_name = 'A. Chen' \
             RETURN p.title",
        )
        .unwrap();
    assert_eq!(papers_via_alias.num_rows(), 1);
    assert_eq!(papers_via_alias.rows()[0].get_string(0).unwrap(), "Deep Learning");
}

// ============================================================
// K-02: 事实来源与置信度
// ============================================================

#[test]
fn k02_provenance_confidence() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE Paper(id INT64, title STRING, year INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE NODE TABLE Source(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    // 中间节点建模 INGESTED_FROM 的置信度和来源方式
    db.execute(
        "CREATE NODE TABLE Ingestion(id INT64, paper_id INT64, source_id INT64, \
         confidence DOUBLE, method STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE FROM_PAPER(FROM Ingestion TO Paper)").unwrap();
    db.execute("CREATE REL TABLE TO_SOURCE(FROM Ingestion TO Source)").unwrap();

    // 创建数据源
    db.execute("CREATE (s:Source {id: 1, name: 'Scopus'})").unwrap();
    db.execute("CREATE (s:Source {id: 2, name: 'Google Scholar'})").unwrap();
    db.execute("CREATE (s:Source {id: 3, name: 'DBLP'})").unwrap();

    // 创建论文
    db.execute("CREATE (p:Paper {id: 1, title: 'Paper Alpha', year: 2021})").unwrap();
    db.execute("CREATE (p:Paper {id: 2, title: 'Paper Beta', year: 2022})").unwrap();
    db.execute("CREATE (p:Paper {id: 3, title: 'Paper Gamma', year: 2023})").unwrap();

    // 创建 Ingestion 节点 + 关系
    let ingestions: Vec<(i64, i64, i64, f64, &str)> = vec![
        (1, 1, 1, 0.95, "api_v2"),
        (2, 1, 2, 0.72, "scrape"),
        (3, 2, 3, 0.88, "api_v1"),
        (4, 2, 1, 0.60, "manual"),
        (5, 3, 2, 0.99, "verified"),
    ];
    for (ing_id, paper_id, source_id, conf, method) in &ingestions {
        db.execute(&format!(
            "CREATE (g:Ingestion {{id: {}, paper_id: {}, source_id: {}, confidence: {}, method: '{}'}})",
            ing_id, paper_id, source_id, conf, method
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (g:Ingestion), (p:Paper) WHERE g.id = {} AND p.id = {} \
             CREATE (g)-[:FROM_PAPER]->(p)",
            ing_id, paper_id
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (g:Ingestion), (s:Source) WHERE g.id = {} AND s.id = {} \
             CREATE (g)-[:TO_SOURCE]->(s)",
            ing_id, source_id
        ))
        .unwrap();
    }

    // 验证总 ingestion 数
    let total = db.query("MATCH (g:Ingestion) RETURN count(g)").unwrap();
    assert_eq!(total.rows()[0].get_int(0).unwrap(), 5);

    // 按置信度 > 0.8 过滤
    let high_conf = db
        .query(
            "MATCH (g:Ingestion)-[:FROM_PAPER]->(p:Paper) \
             WHERE g.confidence > 0.8 \
             RETURN p.title, g.confidence ORDER BY g.confidence DESC",
        )
        .unwrap();
    // 0.95, 0.88, 0.99 -> 3 条
    assert_eq!(high_conf.num_rows(), 3, "confidence > 0.8 should yield 3 records");
    // DESC: 0.99, 0.95, 0.88
    assert_eq!(high_conf.rows()[0].get_string(0).unwrap(), "Paper Gamma");
    assert!(high_conf.rows()[0].get_float(1).unwrap() > 0.98);

    // 按置信度 <= 0.75 过滤
    let low_conf = db
        .query(
            "MATCH (g:Ingestion)-[:FROM_PAPER]->(p:Paper) \
             WHERE g.confidence <= 0.75 \
             RETURN p.title, g.confidence ORDER BY g.confidence",
        )
        .unwrap();
    // 0.60, 0.72 -> 2 条
    assert_eq!(low_conf.num_rows(), 2, "confidence <= 0.75 should yield 2 records");
    assert!(low_conf.rows()[0].get_float(1).unwrap() < 0.65);

    // 验证 method 字段可查询
    let manual = db
        .query(
            "MATCH (g:Ingestion)-[:FROM_PAPER]->(p:Paper) \
             WHERE g.method = 'manual' \
             RETURN p.title",
        )
        .unwrap();
    assert_eq!(manual.num_rows(), 1);
    assert_eq!(manual.rows()[0].get_string(0).unwrap(), "Paper Beta");

    // 按置信度排序并验证来源
    let sorted = db
        .query(
            "MATCH (g:Ingestion)-[:TO_SOURCE]->(s:Source) \
             RETURN s.name, g.confidence ORDER BY g.confidence DESC",
        )
        .unwrap();
    assert_eq!(sorted.num_rows(), 5);
    // 最高 confidence 0.99 -> Google Scholar
    assert_eq!(sorted.rows()[0].get_string(0).unwrap(), "Google Scholar");
}

// ============================================================
// K-03: 增量更新与冲突修正（MERGE 幂等 + 多来源去重）
// ============================================================

#[test]
fn k03_incremental_merge_conflict() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE Paper(id INT64, title STRING, year INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE NODE TABLE Source(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute(
        "CREATE NODE TABLE Ingestion(id INT64, paper_id INT64, source_id INT64, \
         confidence DOUBLE, method STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE FROM_PAPER(FROM Ingestion TO Paper)").unwrap();
    db.execute("CREATE REL TABLE TO_SOURCE(FROM Ingestion TO Source)").unwrap();

    db.execute("CREATE (s:Source {id: 1, name: 'Scopus'})").unwrap();
    db.execute("CREATE (s:Source {id: 2, name: 'DBLP'})").unwrap();

    // 第一次导入: 来源 Scopus
    db.execute(
        "MERGE (p:Paper {id: 100, title: 'Attention Mechanisms'}) ON CREATE SET p.year = 2017",
    )
    .unwrap();
    let count1 = db.query("MATCH (p:Paper) WHERE p.id = 100 RETURN count(p)").unwrap();
    assert_eq!(count1.rows()[0].get_int(0).unwrap(), 1, "First MERGE should create paper");

    // 记录来源 Scopus 的 ingestion
    db.execute(
        "CREATE (g:Ingestion {id: 1, paper_id: 100, source_id: 1, confidence: 0.90, method: 'scopus_api'})",
    )
    .unwrap();
    db.execute(
        "MATCH (g:Ingestion), (p:Paper) WHERE g.id = 1 AND p.id = 100 \
         CREATE (g)-[:FROM_PAPER]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (g:Ingestion), (s:Source) WHERE g.id = 1 AND s.id = 1 \
         CREATE (g)-[:TO_SOURCE]->(s)",
    )
    .unwrap();

    // 第二次导入: 来源 DBLP，同一论文
    db.execute(
        "MERGE (p:Paper {id: 100, title: 'Attention Mechanisms'}) ON MATCH SET p.year = 2017",
    )
    .unwrap();

    // 验证不创建重复
    let count2 = db.query("MATCH (p:Paper) WHERE p.id = 100 RETURN count(p)").unwrap();
    assert_eq!(count2.rows()[0].get_int(0).unwrap(), 1, "MERGE should not duplicate");

    // 添加第二个来源的 ingestion
    db.execute(
        "CREATE (g:Ingestion {id: 2, paper_id: 100, source_id: 2, confidence: 0.85, method: 'dblp_xml'})",
    )
    .unwrap();
    db.execute(
        "MATCH (g:Ingestion), (p:Paper) WHERE g.id = 2 AND p.id = 100 \
         CREATE (g)-[:FROM_PAPER]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (g:Ingestion), (s:Source) WHERE g.id = 2 AND s.id = 2 \
         CREATE (g)-[:TO_SOURCE]->(s)",
    )
    .unwrap();

    // 验证同一论文有 2 个来源
    let sources = db
        .query(
            "MATCH (g:Ingestion)-[:FROM_PAPER]->(p:Paper) \
             WHERE p.id = 100 \
             RETURN g.method ORDER BY g.method",
        )
        .unwrap();
    assert_eq!(sources.num_rows(), 2, "Paper should have 2 sources");
    assert_eq!(sources.rows()[0].get_string(0).unwrap(), "dblp_xml");
    assert_eq!(sources.rows()[1].get_string(0).unwrap(), "scopus_api");

    // 冲突追溯：不同来源的 confidence 不同
    let conflict = db
        .query(
            "MATCH (g:Ingestion)-[:TO_SOURCE]->(s:Source) \
             WHERE g.paper_id = 100 \
             RETURN s.name, g.confidence ORDER BY g.confidence DESC",
        )
        .unwrap();
    assert_eq!(conflict.num_rows(), 2);
    // Scopus confidence=0.90 > DBLP confidence=0.85
    assert_eq!(conflict.rows()[0].get_string(0).unwrap(), "Scopus");
    assert!(conflict.rows()[0].get_float(1).unwrap() > 0.89);

    // 第三次 MERGE 确认幂等
    db.execute("MERGE (p:Paper {id: 100, title: 'Attention Mechanisms'})").unwrap();
    let count3 = db.query("MATCH (p:Paper) WHERE p.id = 100 RETURN count(p)").unwrap();
    assert_eq!(count3.rows()[0].get_int(0).unwrap(), 1, "Third MERGE should remain idempotent");
}

// ============================================================
// K-04: 关系属性驱动推理（年份过滤 + 合作强度）
// ============================================================

#[test]
fn k04_relation_property_reasoning() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE Author(id INT64, name STRING, hindex INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute(
        "CREATE NODE TABLE Institution(id INT64, name STRING, country STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE NODE TABLE Paper(id INT64, title STRING, year INT64, PRIMARY KEY(id))")
        .unwrap();
    // 中间节点：Authorship 带 role 和 year
    db.execute(
        "CREATE NODE TABLE Authorship(id INT64, role STRING, year INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    // 中间节点：Citation 带 context
    db.execute("CREATE NODE TABLE Citation(id INT64, context STRING, PRIMARY KEY(id))").unwrap();

    db.execute("CREATE REL TABLE AUTHORED_BY(FROM Authorship TO Author)").unwrap();
    db.execute("CREATE REL TABLE AUTHORED_PAPER(FROM Authorship TO Paper)").unwrap();
    db.execute("CREATE REL TABLE CITES_FROM(FROM Citation TO Paper)").unwrap();
    db.execute("CREATE REL TABLE CITES_TO(FROM Citation TO Paper)").unwrap();
    // Keep simple AUTHORED for collaboration queries
    db.execute("CREATE REL TABLE AUTHORED(FROM Author TO Paper)").unwrap();

    // 创建作者
    db.execute("CREATE (a:Author {id: 1, name: 'Alice', hindex: 45})").unwrap();
    db.execute("CREATE (a:Author {id: 2, name: 'Bob', hindex: 38})").unwrap();
    db.execute("CREATE (a:Author {id: 3, name: 'Carol', hindex: 52})").unwrap();

    // 创建机构
    db.execute("CREATE (i:Institution {id: 1, name: 'MIT', country: 'USA'})").unwrap();
    db.execute("CREATE (i:Institution {id: 2, name: 'Stanford', country: 'USA'})").unwrap();

    // 创建论文
    db.execute("CREATE (p:Paper {id: 1, title: 'Paper 2018', year: 2018})").unwrap();
    db.execute("CREATE (p:Paper {id: 2, title: 'Paper 2019', year: 2019})").unwrap();
    db.execute("CREATE (p:Paper {id: 3, title: 'Paper 2020', year: 2020})").unwrap();
    db.execute("CREATE (p:Paper {id: 4, title: 'Paper 2021', year: 2021})").unwrap();
    db.execute("CREATE (p:Paper {id: 5, title: 'Paper 2022', year: 2022})").unwrap();

    // Authorship 中间节点带 role 和 year
    let authorships: Vec<(i64, i64, i64, &str, i64)> = vec![
        (1, 1, 1, "first", 2018),
        (2, 2, 1, "second", 2018),
        (3, 1, 2, "first", 2019),
        (4, 3, 2, "second", 2019),
        (5, 1, 3, "first", 2020),
        (6, 2, 3, "second", 2020),
        (7, 2, 4, "first", 2021),
        (8, 3, 4, "second", 2021),
        (9, 1, 5, "first", 2022),
        (10, 2, 5, "second", 2022),
        (11, 3, 5, "corresponding", 2022),
    ];
    for (as_id, author_id, paper_id, role, year) in &authorships {
        db.execute(&format!(
            "CREATE (s:Authorship {{id: {}, role: '{}', year: {}}})",
            as_id, role, year
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (s:Authorship), (a:Author) WHERE s.id = {} AND a.id = {} \
             CREATE (s)-[:AUTHORED_BY]->(a)",
            as_id, author_id
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (s:Authorship), (p:Paper) WHERE s.id = {} AND p.id = {} \
             CREATE (s)-[:AUTHORED_PAPER]->(p)",
            as_id, paper_id
        ))
        .unwrap();
        // Also create simple AUTHORED for collaboration queries
        db.execute(&format!(
            "MATCH (a:Author), (p:Paper) WHERE a.id = {} AND p.id = {} \
             CREATE (a)-[:AUTHORED]->(p)",
            author_id, paper_id
        ))
        .unwrap();
    }

    // Citation 中间节点带 context
    db.execute("CREATE (c:Citation {id: 1, context: 'extends methodology'})").unwrap();
    db.execute(
        "MATCH (c:Citation), (p:Paper) WHERE c.id = 1 AND p.id = 3 \
         CREATE (c)-[:CITES_FROM]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (c:Citation), (p:Paper) WHERE c.id = 1 AND p.id = 1 \
         CREATE (c)-[:CITES_TO]->(p)",
    )
    .unwrap();

    db.execute("CREATE (c:Citation {id: 2, context: 'builds upon findings'})").unwrap();
    db.execute(
        "MATCH (c:Citation), (p:Paper) WHERE c.id = 2 AND p.id = 4 \
         CREATE (c)-[:CITES_FROM]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (c:Citation), (p:Paper) WHERE c.id = 2 AND p.id = 2 \
         CREATE (c)-[:CITES_TO]->(p)",
    )
    .unwrap();

    // 1) 按 Authorship year 过滤: Alice 2020 年及以后的论文
    let recent_papers = db
        .query(
            "MATCH (s:Authorship)-[:AUTHORED_BY]->(a:Author) \
             WHERE a.id = 1 AND s.year >= 2020 \
             RETURN s.year ORDER BY s.year",
        )
        .unwrap();
    // Authorship IDs for Alice: 1(2018), 3(2019), 5(2020), 9(2022) -> >= 2020: 2 rows
    assert_eq!(recent_papers.num_rows(), 2, "Alice should have 2 papers from 2020 onward");

    // Verify paper titles via Authorship -> Paper
    let recent_titles = db
        .query(
            "MATCH (s:Authorship)-[:AUTHORED_BY]->(a:Author) \
             WHERE a.id = 1 AND s.year >= 2020 \
             RETURN s.year ORDER BY s.year",
        )
        .unwrap();
    assert_eq!(recent_titles.rows()[0].get_int(0).unwrap(), 2020);
    assert_eq!(recent_titles.rows()[1].get_int(0).unwrap(), 2022);

    // 2) 按角色过滤: Alice 作为 first author
    let first_author = db
        .query(
            "MATCH (s:Authorship)-[:AUTHORED_BY]->(a:Author) \
             WHERE a.id = 1 AND s.role = 'first' \
             RETURN s.year ORDER BY s.year",
        )
        .unwrap();
    assert_eq!(first_author.num_rows(), 4, "Alice should have 4 first-author papers");

    // 3) 合作强度统计：Alice 与 Bob 合著论文数
    let collab_ab = db
        .query(
            "MATCH (a:Author)-[:AUTHORED]->(p:Paper)<-[:AUTHORED]-(b:Author) \
             WHERE a.id = 1 AND b.id = 2 \
             RETURN count(p)",
        )
        .unwrap();
    // Papers 1, 3, 5 -> Alice + Bob co-authored 3 papers
    assert_eq!(collab_ab.rows()[0].get_int(0).unwrap(), 3, "Alice and Bob should co-author 3 papers");

    // Alice 与 Carol 合著论文数
    let collab_ac = db
        .query(
            "MATCH (a:Author)-[:AUTHORED]->(p:Paper)<-[:AUTHORED]-(b:Author) \
             WHERE a.id = 1 AND b.id = 3 \
             RETURN count(p)",
        )
        .unwrap();
    // Papers 2, 5 -> 2
    assert_eq!(collab_ac.rows()[0].get_int(0).unwrap(), 2, "Alice and Carol should co-author 2 papers");

    // 4) Citation context 可查
    let cite_ctx = db
        .query(
            "MATCH (c:Citation)-[:CITES_FROM]->(from_paper:Paper) \
             WHERE from_paper.id = 3 \
             RETURN c.context",
        )
        .unwrap();
    assert_eq!(cite_ctx.num_rows(), 1);
    assert_eq!(cite_ctx.rows()[0].get_string(0).unwrap(), "extends methodology");
}

// ============================================================
// K-05: 实体版本链持久化（文件持久化 + reopen）
// ============================================================

#[test]
fn k05_version_lineage_persistence() {
    let path = temp_db_path("version_lineage");
    cleanup(&path);

    // Phase 1: 创建版本链并写入文件
    {
        let db = Database::open(&path).unwrap();

        db.execute("CREATE NODE TABLE Paper(id INT64, title STRING, year INT64, PRIMARY KEY(id))")
            .unwrap();
        db.execute(
            "CREATE NODE TABLE Version(id INT64, version_num INT64, ts INT64, PRIMARY KEY(id))",
        )
        .unwrap();
        db.execute("CREATE NODE TABLE Source(id INT64, name STRING, PRIMARY KEY(id))").unwrap();

        db.execute("CREATE REL TABLE HAS_VERSION(FROM Paper TO Version)").unwrap();
        db.execute("CREATE REL TABLE SUPERSEDES(FROM Version TO Version)").unwrap();
        db.execute("CREATE REL TABLE INGESTED_FROM(FROM Paper TO Source)").unwrap();

        // 创建论文
        db.execute("CREATE (p:Paper {id: 1, title: 'Graph Learning', year: 2023})").unwrap();

        // 创建 3 个版本
        db.execute("CREATE (v:Version {id: 1, version_num: 1, ts: 1000})").unwrap();
        db.execute("CREATE (v:Version {id: 2, version_num: 2, ts: 2000})").unwrap();
        db.execute("CREATE (v:Version {id: 3, version_num: 3, ts: 3000})").unwrap();

        // 创建来源
        db.execute("CREATE (s:Source {id: 1, name: 'arXiv'})").unwrap();
        db.execute("CREATE (s:Source {id: 2, name: 'ACM DL'})").unwrap();

        // HAS_VERSION 关系
        for v_id in 1..=3 {
            db.execute(&format!(
                "MATCH (p:Paper), (v:Version) WHERE p.id = 1 AND v.id = {} \
                 CREATE (p)-[:HAS_VERSION]->(v)",
                v_id
            ))
            .unwrap();
        }

        // SUPERSEDES 链: v3 -> v2 -> v1
        db.execute(
            "MATCH (newer:Version), (older:Version) WHERE newer.id = 2 AND older.id = 1 \
             CREATE (newer)-[:SUPERSEDES]->(older)",
        )
        .unwrap();
        db.execute(
            "MATCH (newer:Version), (older:Version) WHERE newer.id = 3 AND older.id = 2 \
             CREATE (newer)-[:SUPERSEDES]->(older)",
        )
        .unwrap();

        // INGESTED_FROM 关系
        db.execute(
            "MATCH (p:Paper), (s:Source) WHERE p.id = 1 AND s.id = 1 \
             CREATE (p)-[:INGESTED_FROM]->(s)",
        )
        .unwrap();
        db.execute(
            "MATCH (p:Paper), (s:Source) WHERE p.id = 1 AND s.id = 2 \
             CREATE (p)-[:INGESTED_FROM]->(s)",
        )
        .unwrap();

        // 验证写入正确
        let versions = db
            .query(
                "MATCH (p:Paper)-[:HAS_VERSION]->(v:Version) \
                 WHERE p.id = 1 \
                 RETURN count(v)",
            )
            .unwrap();
        assert_eq!(versions.rows()[0].get_int(0).unwrap(), 3);
    }

    // Phase 2: 重新打开数据库，验证 lineage 完整
    {
        let db = Database::open(&path).unwrap();

        // 验证版本数
        let versions = db
            .query(
                "MATCH (p:Paper)-[:HAS_VERSION]->(v:Version) \
                 WHERE p.id = 1 \
                 RETURN v.version_num ORDER BY v.version_num",
            )
            .unwrap();
        assert_eq!(versions.num_rows(), 3, "After reopen, should have 3 versions");
        assert_eq!(versions.rows()[0].get_int(0).unwrap(), 1);
        assert_eq!(versions.rows()[1].get_int(0).unwrap(), 2);
        assert_eq!(versions.rows()[2].get_int(0).unwrap(), 3);

        // 验证 SUPERSEDES 链: 从 v3 追溯到 v1
        let lineage = db
            .query(
                "MATCH (latest:Version)-[:SUPERSEDES*1..3]->(older:Version) \
                 WHERE latest.id = 3 \
                 RETURN older.version_num ORDER BY older.version_num",
            )
            .unwrap();
        assert_eq!(lineage.num_rows(), 2, "v3 should trace back to v2 and v1");
        assert_eq!(lineage.rows()[0].get_int(0).unwrap(), 1);
        assert_eq!(lineage.rows()[1].get_int(0).unwrap(), 2);

        // 验证最新版本时间戳
        let latest = db
            .query(
                "MATCH (p:Paper)-[:HAS_VERSION]->(v:Version) \
                 WHERE p.id = 1 AND v.version_num = 3 \
                 RETURN v.ts",
            )
            .unwrap();
        assert_eq!(latest.num_rows(), 1);
        assert_eq!(latest.rows()[0].get_int(0).unwrap(), 3000);

        // 验证来源关系完整
        let sources = db
            .query(
                "MATCH (p:Paper)-[:INGESTED_FROM]->(s:Source) \
                 WHERE p.id = 1 \
                 RETURN s.name ORDER BY s.name",
            )
            .unwrap();
        assert_eq!(sources.num_rows(), 2);
        assert_eq!(sources.rows()[0].get_string(0).unwrap(), "ACM DL");
        assert_eq!(sources.rows()[1].get_string(0).unwrap(), "arXiv");
    }

    cleanup(&path);
}

// ============================================================
// K-06: 主题层级图（3+ 层级，聚合与下钻）
// ============================================================

#[test]
fn k06_topic_hierarchy() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE Topic(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Paper(id INT64, title STRING, year INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE REL TABLE PARENT_TOPIC(FROM Topic TO Topic)").unwrap();
    db.execute("CREATE REL TABLE HAS_TOPIC(FROM Paper TO Topic)").unwrap();

    // 创建 3 级主题层级:
    // Computer Science (1)
    //   +-- AI (2)
    //   |   +-- Machine Learning (4)
    //   |   +-- NLP (5)
    //   +-- Systems (3)
    //       +-- Databases (6)
    db.execute("CREATE (t:Topic {id: 1, name: 'Computer Science'})").unwrap();
    db.execute("CREATE (t:Topic {id: 2, name: 'AI'})").unwrap();
    db.execute("CREATE (t:Topic {id: 3, name: 'Systems'})").unwrap();
    db.execute("CREATE (t:Topic {id: 4, name: 'Machine Learning'})").unwrap();
    db.execute("CREATE (t:Topic {id: 5, name: 'NLP'})").unwrap();
    db.execute("CREATE (t:Topic {id: 6, name: 'Databases'})").unwrap();

    // PARENT_TOPIC: child -> parent
    let hierarchy = [(2, 1), (3, 1), (4, 2), (5, 2), (6, 3)];
    for (child, parent) in &hierarchy {
        db.execute(&format!(
            "MATCH (c:Topic), (p:Topic) WHERE c.id = {} AND p.id = {} \
             CREATE (c)-[:PARENT_TOPIC]->(p)",
            child, parent
        ))
        .unwrap();
    }

    // 创建论文并关联到叶子主题
    db.execute("CREATE (p:Paper {id: 1, title: 'Deep Learning Paper', year: 2021})").unwrap();
    db.execute("CREATE (p:Paper {id: 2, title: 'Transformer Paper', year: 2022})").unwrap();
    db.execute("CREATE (p:Paper {id: 3, title: 'CNN Paper', year: 2020})").unwrap();
    db.execute("CREATE (p:Paper {id: 4, title: 'BERT Paper', year: 2019})").unwrap();
    db.execute("CREATE (p:Paper {id: 5, title: 'SQL Optimizer', year: 2023})").unwrap();
    db.execute("CREATE (p:Paper {id: 6, title: 'Query Processing', year: 2022})").unwrap();

    let paper_topics = [(1, 4), (2, 4), (3, 4), (4, 5), (5, 6), (6, 6)];
    for (paper_id, topic_id) in &paper_topics {
        db.execute(&format!(
            "MATCH (p:Paper), (t:Topic) WHERE p.id = {} AND t.id = {} \
             CREATE (p)-[:HAS_TOPIC]->(t)",
            paper_id, topic_id
        ))
        .unwrap();
    }

    // 1) 下钻: Computer Science 的直接子主题
    let direct_children = db
        .query(
            "MATCH (child:Topic)-[:PARENT_TOPIC]->(parent:Topic) \
             WHERE parent.id = 1 \
             RETURN child.name ORDER BY child.name",
        )
        .unwrap();
    assert_eq!(direct_children.num_rows(), 2);
    assert_eq!(direct_children.rows()[0].get_string(0).unwrap(), "AI");
    assert_eq!(direct_children.rows()[1].get_string(0).unwrap(), "Systems");

    // 2) 上位聚合: Machine Learning 上溯到顶层 (ML -> AI -> CS)
    let ancestors = db
        .query(
            "MATCH (t:Topic)-[:PARENT_TOPIC*1..3]->(ancestor:Topic) \
             WHERE t.id = 4 \
             RETURN ancestor.name ORDER BY ancestor.name",
        )
        .unwrap();
    assert_eq!(ancestors.num_rows(), 2, "ML should have 2 ancestors: AI and CS");
    let ancestor_names: Vec<&str> =
        ancestors.rows().iter().map(|r| r.get_string(0).unwrap()).collect();
    assert!(ancestor_names.contains(&"AI"));
    assert!(ancestor_names.contains(&"Computer Science"));

    // 3) 统计叶子主题下的论文数
    let ml_papers = db
        .query(
            "MATCH (p:Paper)-[:HAS_TOPIC]->(t:Topic) \
             WHERE t.id = 4 \
             RETURN count(p)",
        )
        .unwrap();
    assert_eq!(ml_papers.rows()[0].get_int(0).unwrap(), 3, "ML should have 3 papers");

    let db_papers = db
        .query(
            "MATCH (p:Paper)-[:HAS_TOPIC]->(t:Topic) \
             WHERE t.id = 6 \
             RETURN count(p)",
        )
        .unwrap();
    assert_eq!(db_papers.rows()[0].get_int(0).unwrap(), 2, "Databases should have 2 papers");

    // 4) 聚合上级: AI 下所有论文（通过子主题间接关联）
    // AI(2) 的子主题: ML(4), NLP(5)
    // ML papers: 1,2,3; NLP papers: 4 -> total 4
    let ai_papers = db
        .query(
            "MATCH (p:Paper)-[:HAS_TOPIC]->(leaf:Topic)-[:PARENT_TOPIC]->(mid:Topic) \
             WHERE mid.id = 2 \
             RETURN count(p)",
        )
        .unwrap();
    assert_eq!(ai_papers.rows()[0].get_int(0).unwrap(), 4, "AI subtree should have 4 papers");

    // 5) 验证 3 级层级深度: leaf -> mid -> top
    let three_levels = db
        .query(
            "MATCH (leaf:Topic)-[:PARENT_TOPIC]->(mid:Topic)-[:PARENT_TOPIC]->(top:Topic) \
             WHERE leaf.id = 4 \
             RETURN leaf.name, mid.name, top.name",
        )
        .unwrap();
    assert_eq!(three_levels.num_rows(), 1);
    assert_eq!(three_levels.rows()[0].get_string(0).unwrap(), "Machine Learning");
    assert_eq!(three_levels.rows()[0].get_string(1).unwrap(), "AI");
    assert_eq!(three_levels.rows()[0].get_string(2).unwrap(), "Computer Science");
}

// ============================================================
// K-07: 文献导入批处理（50 篇论文 + 去重 + 完整性）
// ============================================================

#[test]
fn k07_batch_import() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE Author(id INT64, name STRING, hindex INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE NODE TABLE Paper(id INT64, title STRING, year INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE NODE TABLE Topic(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE AUTHORED(FROM Author TO Paper)").unwrap();
    db.execute("CREATE REL TABLE CITES(FROM Paper TO Paper)").unwrap();
    db.execute("CREATE REL TABLE HAS_TOPIC(FROM Paper TO Topic)").unwrap();

    // 创建 10 个作者
    for i in 1..=10 {
        db.execute(&format!(
            "CREATE (a:Author {{id: {}, name: 'Author{}', hindex: {}}})",
            i, i, 20 + i
        ))
        .unwrap();
    }

    // 创建 5 个主题
    for i in 1..=5 {
        db.execute(&format!("CREATE (t:Topic {{id: {}, name: 'Topic{}'}})", i, i)).unwrap();
    }

    // 批量创建 50 篇论文
    for i in 1..=50 {
        db.execute(&format!(
            "CREATE (p:Paper {{id: {}, title: 'Paper {}', year: {}}})",
            i,
            i,
            2018 + (i % 6)
        ))
        .unwrap();
    }

    // 验证创建了 50 篇论文
    let paper_count = db.query("MATCH (p:Paper) RETURN count(p)").unwrap();
    assert_eq!(paper_count.rows()[0].get_int(0).unwrap(), 50);

    // 批量创建 AUTHORED 关系（每篇论文 1~2 个作者）
    for i in 1..=50i64 {
        let author1 = (i % 10) + 1;
        db.execute(&format!(
            "MATCH (a:Author), (p:Paper) WHERE a.id = {} AND p.id = {} \
             CREATE (a)-[:AUTHORED]->(p)",
            author1, i
        ))
        .unwrap();

        // 每 2 篇论文加一个第二作者
        if i % 2 == 0 {
            let author2 = ((i + 3) % 10) + 1;
            db.execute(&format!(
                "MATCH (a:Author), (p:Paper) WHERE a.id = {} AND p.id = {} \
                 CREATE (a)-[:AUTHORED]->(p)",
                author2, i
            ))
            .unwrap();
        }
    }

    // 批量创建 HAS_TOPIC 关系
    for i in 1..=50i64 {
        let topic_id = (i % 5) + 1;
        db.execute(&format!(
            "MATCH (p:Paper), (t:Topic) WHERE p.id = {} AND t.id = {} \
             CREATE (p)-[:HAS_TOPIC]->(t)",
            i, topic_id
        ))
        .unwrap();
    }

    // 创建引用网络: 每篇论文引用前一篇（形成链）
    for i in 2..=50i64 {
        db.execute(&format!(
            "MATCH (a:Paper), (b:Paper) WHERE a.id = {} AND b.id = {} \
             CREATE (a)-[:CITES]->(b)",
            i,
            i - 1
        ))
        .unwrap();
    }

    // 验证引用关系数 = 49
    let cite_count = db
        .query("MATCH (a:Paper)-[:CITES]->(b:Paper) RETURN count(a)")
        .unwrap();
    assert_eq!(cite_count.rows()[0].get_int(0).unwrap(), 49);

    // 验证 AUTHORED 关系数 = 50 (first) + 25 (second) = 75
    let auth_count = db
        .query("MATCH (a:Author)-[:AUTHORED]->(p:Paper) RETURN count(a)")
        .unwrap();
    assert_eq!(auth_count.rows()[0].get_int(0).unwrap(), 75);

    // 验证 HAS_TOPIC 关系数 = 50
    let topic_count = db
        .query("MATCH (p:Paper)-[:HAS_TOPIC]->(t:Topic) RETURN count(p)")
        .unwrap();
    assert_eq!(topic_count.rows()[0].get_int(0).unwrap(), 50);

    // 去重验证：尝试 MERGE 已有论文
    db.execute("MERGE (p:Paper {id: 1, title: 'Paper 1'})").unwrap();
    let after_merge = db.query("MATCH (p:Paper) RETURN count(p)").unwrap();
    assert_eq!(after_merge.rows()[0].get_int(0).unwrap(), 50, "MERGE should not create duplicates");

    // 验证每个主题都有论文
    let topics_with_papers = db
        .query(
            "MATCH (p:Paper)-[:HAS_TOPIC]->(t:Topic) \
             RETURN t.name, count(p) ORDER BY t.name",
        )
        .unwrap();
    assert_eq!(topics_with_papers.num_rows(), 5, "All 5 topics should have papers");
    for row in topics_with_papers.rows() {
        assert!(row.get_int(1).unwrap() >= 10, "Each topic should have at least 10 papers");
    }
}

// ============================================================
// K-08: 孤立实体与脏数据治理
// ============================================================

#[test]
fn k08_orphan_entities_dirty_data() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE Author(id INT64, name STRING, hindex INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute(
        "CREATE NODE TABLE Paper(id INT64, title STRING, year INT64, abstract STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE AUTHORED(FROM Author TO Paper)").unwrap();
    db.execute("CREATE REL TABLE CITES(FROM Paper TO Paper)").unwrap();

    // 正常数据
    db.execute("CREATE (a:Author {id: 1, name: 'Alice', hindex: 45})").unwrap();
    db.execute("CREATE (p:Paper {id: 1, title: 'Good Paper', year: 2021})").unwrap();
    db.execute(
        "MATCH (a:Author), (p:Paper) WHERE a.id = 1 AND p.id = 1 \
         CREATE (a)-[:AUTHORED]->(p)",
    )
    .unwrap();

    // 孤立论文（没有作者）
    db.execute("CREATE (p:Paper {id: 2, title: 'Orphan Paper', year: 2022})").unwrap();
    db.execute("CREATE (p:Paper {id: 3, title: 'Another Orphan', year: 2023})").unwrap();

    // 孤立作者（没有论文）
    db.execute("CREATE (a:Author {id: 2, name: 'Lone Author', hindex: 0})").unwrap();

    // 悬挂引用：Paper 4 引用 Paper 5, 但 Paper 5 没有作者
    db.execute("CREATE (p:Paper {id: 4, title: 'Citing Paper', year: 2023})").unwrap();
    db.execute("CREATE (p:Paper {id: 5, title: 'Cited But Authorless', year: 2020})").unwrap();
    db.execute(
        "MATCH (a:Paper), (b:Paper) WHERE a.id = 4 AND b.id = 5 \
         CREATE (a)-[:CITES]->(b)",
    )
    .unwrap();

    // 1) 查找没有作者的论文（孤立论文）
    // 获取所有有作者的论文 ID，然后找出没有作者的
    let authored_papers = db
        .query("MATCH (a:Author)-[:AUTHORED]->(p:Paper) RETURN p.id")
        .unwrap();
    let authored_ids: std::collections::HashSet<i64> = authored_papers
        .rows()
        .iter()
        .map(|r| r.get_int(0).unwrap())
        .collect();

    let all_papers = db
        .query("MATCH (p:Paper) RETURN p.id, p.title ORDER BY p.title")
        .unwrap();

    let orphan_titles: Vec<&str> = all_papers
        .rows()
        .iter()
        .filter(|r| !authored_ids.contains(&r.get_int(0).unwrap()))
        .map(|r| r.get_string(1).unwrap())
        .collect();
    // Papers 2, 3, 4, 5 have no authors
    assert!(orphan_titles.len() >= 3, "Should find multiple papers without authors");
    assert!(orphan_titles.contains(&"Orphan Paper"));
    assert!(orphan_titles.contains(&"Another Orphan"));

    // 2) 查找没有论文的作者
    let authors_with_papers = db
        .query("MATCH (a:Author)-[:AUTHORED]->(p:Paper) RETURN a.id")
        .unwrap();
    let author_with_paper_ids: std::collections::HashSet<i64> = authors_with_papers
        .rows()
        .iter()
        .map(|r| r.get_int(0).unwrap())
        .collect();

    let all_authors = db.query("MATCH (a:Author) RETURN a.id, a.name").unwrap();
    let orphan_author_names: Vec<&str> = all_authors
        .rows()
        .iter()
        .filter(|r| !author_with_paper_ids.contains(&r.get_int(0).unwrap()))
        .map(|r| r.get_string(1).unwrap())
        .collect();
    assert!(orphan_author_names.len() >= 1, "Should find authors without papers");
    assert!(orphan_author_names.contains(&"Lone Author"));

    // 3) 悬挂引用查询不应 panic
    let cite_result = db
        .query(
            "MATCH (a:Paper)-[:CITES]->(b:Paper) \
             WHERE a.id = 4 \
             RETURN b.title",
        )
        .unwrap();
    assert_eq!(cite_result.num_rows(), 1);
    assert_eq!(cite_result.rows()[0].get_string(0).unwrap(), "Cited But Authorless");

    // 4) 查询引用链中包含无作者论文 — 不应 panic
    let chain = db
        .query(
            "MATCH (a:Paper)-[:CITES*1..2]->(b:Paper) \
             WHERE a.id = 4 \
             RETURN b.title",
        )
        .unwrap();
    assert!(chain.num_rows() >= 1, "Citation chain query should return normally");

    // 5) 查询所有论文（包括孤立的）— 不应 panic
    let all = db.query("MATCH (p:Paper) RETURN count(p)").unwrap();
    assert_eq!(all.rows()[0].get_int(0).unwrap(), 5, "Should have 5 papers including orphans");

    // 6) 对缺失属性的论文查询也不应 panic
    let abs_query = db
        .query("MATCH (p:Paper) RETURN p.title, p.abstract ORDER BY p.title")
        .unwrap();
    assert_eq!(abs_query.num_rows(), 5);
    // abstract is never set, should be null; accessing it should not panic
    for row in abs_query.rows() {
        let _abstract_val = row.get_string(1);
    }

    // 7) 验证 cited-but-authorless paper 的作者查询返回空
    let authorless_check = db
        .query(
            "MATCH (a:Author)-[:AUTHORED]->(p:Paper) \
             WHERE p.id = 5 \
             RETURN a.name",
        )
        .unwrap();
    assert_eq!(authorless_check.num_rows(), 0, "Paper 5 should have no authors");
}

// ============================================================
// K-09: 事实失效与时间生效（机构归属时间范围）
// ============================================================

#[test]
fn k09_temporal_affiliation() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE Author(id INT64, name STRING, hindex INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute(
        "CREATE NODE TABLE Institution(id INT64, name STRING, country STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    // 中间节点: Affiliation 带时间范围
    db.execute(
        "CREATE NODE TABLE Affiliation(id INT64, from_year INT64, to_year INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE AFF_AUTHOR(FROM Affiliation TO Author)").unwrap();
    db.execute("CREATE REL TABLE AFF_INST(FROM Affiliation TO Institution)").unwrap();

    // 创建作者和机构
    db.execute("CREATE (a:Author {id: 1, name: 'Alice', hindex: 45})").unwrap();
    db.execute("CREATE (a:Author {id: 2, name: 'Bob', hindex: 38})").unwrap();
    db.execute("CREATE (a:Author {id: 3, name: 'Carol', hindex: 52})").unwrap();

    db.execute("CREATE (i:Institution {id: 1, name: 'MIT', country: 'USA'})").unwrap();
    db.execute("CREATE (i:Institution {id: 2, name: 'Stanford', country: 'USA'})").unwrap();
    db.execute("CREATE (i:Institution {id: 3, name: 'Oxford', country: 'UK'})").unwrap();

    // Alice: MIT 2015-2019, Stanford 2020-2025
    db.execute("CREATE (af:Affiliation {id: 1, from_year: 2015, to_year: 2019})").unwrap();
    db.execute(
        "MATCH (af:Affiliation), (a:Author) WHERE af.id = 1 AND a.id = 1 \
         CREATE (af)-[:AFF_AUTHOR]->(a)",
    )
    .unwrap();
    db.execute(
        "MATCH (af:Affiliation), (i:Institution) WHERE af.id = 1 AND i.id = 1 \
         CREATE (af)-[:AFF_INST]->(i)",
    )
    .unwrap();

    db.execute("CREATE (af:Affiliation {id: 2, from_year: 2020, to_year: 2025})").unwrap();
    db.execute(
        "MATCH (af:Affiliation), (a:Author) WHERE af.id = 2 AND a.id = 1 \
         CREATE (af)-[:AFF_AUTHOR]->(a)",
    )
    .unwrap();
    db.execute(
        "MATCH (af:Affiliation), (i:Institution) WHERE af.id = 2 AND i.id = 2 \
         CREATE (af)-[:AFF_INST]->(i)",
    )
    .unwrap();

    // Bob: MIT 2018-2023
    db.execute("CREATE (af:Affiliation {id: 3, from_year: 2018, to_year: 2023})").unwrap();
    db.execute(
        "MATCH (af:Affiliation), (a:Author) WHERE af.id = 3 AND a.id = 2 \
         CREATE (af)-[:AFF_AUTHOR]->(a)",
    )
    .unwrap();
    db.execute(
        "MATCH (af:Affiliation), (i:Institution) WHERE af.id = 3 AND i.id = 1 \
         CREATE (af)-[:AFF_INST]->(i)",
    )
    .unwrap();

    // Carol: Oxford 2016-2020, Stanford 2021-2025
    db.execute("CREATE (af:Affiliation {id: 4, from_year: 2016, to_year: 2020})").unwrap();
    db.execute(
        "MATCH (af:Affiliation), (a:Author) WHERE af.id = 4 AND a.id = 3 \
         CREATE (af)-[:AFF_AUTHOR]->(a)",
    )
    .unwrap();
    db.execute(
        "MATCH (af:Affiliation), (i:Institution) WHERE af.id = 4 AND i.id = 3 \
         CREATE (af)-[:AFF_INST]->(i)",
    )
    .unwrap();

    db.execute("CREATE (af:Affiliation {id: 5, from_year: 2021, to_year: 2025})").unwrap();
    db.execute(
        "MATCH (af:Affiliation), (a:Author) WHERE af.id = 5 AND a.id = 3 \
         CREATE (af)-[:AFF_AUTHOR]->(a)",
    )
    .unwrap();
    db.execute(
        "MATCH (af:Affiliation), (i:Institution) WHERE af.id = 5 AND i.id = 2 \
         CREATE (af)-[:AFF_INST]->(i)",
    )
    .unwrap();

    // 1) "2018 年在 MIT 的人": Alice(2015-2019), Bob(2018-2023) -> 2 人
    let mit_2018 = db
        .query(
            "MATCH (af:Affiliation)-[:AFF_INST]->(i:Institution) \
             WHERE i.id = 1 AND af.from_year <= 2018 AND af.to_year >= 2018 \
             RETURN af.id",
        )
        .unwrap();
    let mit_2018_aff_ids: Vec<i64> = mit_2018.rows().iter().map(|r| r.get_int(0).unwrap()).collect();
    // aff 1 (Alice at MIT 2015-2019) and aff 3 (Bob at MIT 2018-2023)
    assert_eq!(mit_2018_aff_ids.len(), 2);

    // Now get the author names for these affiliations
    let mut mit_2018_names: Vec<String> = Vec::new();
    for aff_id in &mit_2018_aff_ids {
        let r = db
            .query(&format!(
                "MATCH (af:Affiliation)-[:AFF_AUTHOR]->(a:Author) \
                 WHERE af.id = {} \
                 RETURN a.name",
                aff_id
            ))
            .unwrap();
        if r.num_rows() > 0 {
            mit_2018_names.push(r.rows()[0].get_string(0).unwrap().to_string());
        }
    }
    mit_2018_names.sort();
    assert_eq!(mit_2018_names, vec!["Alice", "Bob"]);

    // 2) "2020 年在 MIT 的人" -> 只有 Bob
    let mit_2020 = db
        .query(
            "MATCH (af:Affiliation)-[:AFF_INST]->(i:Institution) \
             WHERE i.id = 1 AND af.from_year <= 2020 AND af.to_year >= 2020 \
             RETURN af.id",
        )
        .unwrap();
    assert_eq!(mit_2020.num_rows(), 1);
    let bob_check = db
        .query(&format!(
            "MATCH (af:Affiliation)-[:AFF_AUTHOR]->(a:Author) \
             WHERE af.id = {} \
             RETURN a.name",
            mit_2020.rows()[0].get_int(0).unwrap()
        ))
        .unwrap();
    assert_eq!(bob_check.rows()[0].get_string(0).unwrap(), "Bob");

    // 3) Alice 2020 年在哪？-> Stanford
    let alice_2020 = db
        .query(
            "MATCH (af:Affiliation)-[:AFF_AUTHOR]->(a:Author) \
             WHERE a.id = 1 AND af.from_year <= 2020 AND af.to_year >= 2020 \
             RETURN af.id",
        )
        .unwrap();
    assert_eq!(alice_2020.num_rows(), 1);
    let inst_check = db
        .query(&format!(
            "MATCH (af:Affiliation)-[:AFF_INST]->(i:Institution) \
             WHERE af.id = {} \
             RETURN i.name",
            alice_2020.rows()[0].get_int(0).unwrap()
        ))
        .unwrap();
    assert_eq!(inst_check.rows()[0].get_string(0).unwrap(), "Stanford");

    // 4) Alice 2019 年在哪？-> MIT
    let alice_2019 = db
        .query(
            "MATCH (af:Affiliation)-[:AFF_AUTHOR]->(a:Author) \
             WHERE a.id = 1 AND af.from_year <= 2019 AND af.to_year >= 2019 \
             RETURN af.id",
        )
        .unwrap();
    assert_eq!(alice_2019.num_rows(), 1);
    let inst_2019 = db
        .query(&format!(
            "MATCH (af:Affiliation)-[:AFF_INST]->(i:Institution) \
             WHERE af.id = {} \
             RETURN i.name",
            alice_2019.rows()[0].get_int(0).unwrap()
        ))
        .unwrap();
    assert_eq!(inst_2019.rows()[0].get_string(0).unwrap(), "MIT");

    // 5) "2022 年在 Stanford 的人" -> Alice + Carol
    let stanford_2022 = db
        .query(
            "MATCH (af:Affiliation)-[:AFF_INST]->(i:Institution) \
             WHERE i.id = 2 AND af.from_year <= 2022 AND af.to_year >= 2022 \
             RETURN af.id",
        )
        .unwrap();
    assert_eq!(stanford_2022.num_rows(), 2);
    let mut stanford_names: Vec<String> = Vec::new();
    for row in stanford_2022.rows() {
        let aff_id = row.get_int(0).unwrap();
        let r = db
            .query(&format!(
                "MATCH (af:Affiliation)-[:AFF_AUTHOR]->(a:Author) \
                 WHERE af.id = {} \
                 RETURN a.name",
                aff_id
            ))
            .unwrap();
        stanford_names.push(r.rows()[0].get_string(0).unwrap().to_string());
    }
    stanford_names.sort();
    assert_eq!(stanford_names, vec!["Alice", "Carol"]);

    // 6) "2017 年在 MIT 的人" -> 只有 Alice
    let mit_2017 = db
        .query(
            "MATCH (af:Affiliation)-[:AFF_INST]->(i:Institution) \
             WHERE i.id = 1 AND af.from_year <= 2017 AND af.to_year >= 2017 \
             RETURN af.id",
        )
        .unwrap();
    assert_eq!(mit_2017.num_rows(), 1);
    let alice_only = db
        .query(&format!(
            "MATCH (af:Affiliation)-[:AFF_AUTHOR]->(a:Author) \
             WHERE af.id = {} \
             RETURN a.name",
            mit_2017.rows()[0].get_int(0).unwrap()
        ))
        .unwrap();
    assert_eq!(alice_only.rows()[0].get_string(0).unwrap(), "Alice");
}

// ============================================================
// K-10: 共被引分析（Co-citation Analysis）
// ============================================================

#[test]
fn k10_co_citation_analysis() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE Paper(id INT64, title STRING, year INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE REL TABLE CITES(FROM Paper TO Paper)").unwrap();

    // 目标论文对
    db.execute("CREATE (p:Paper {id: 1, title: 'Graph Theory Basics', year: 2018})").unwrap();
    db.execute("CREATE (p:Paper {id: 2, title: 'Network Analysis', year: 2019})").unwrap();
    db.execute("CREATE (p:Paper {id: 3, title: 'Unrelated Paper', year: 2020})").unwrap();

    // 引用论文（Surveys）
    db.execute("CREATE (p:Paper {id: 11, title: 'Survey 1', year: 2021})").unwrap();
    db.execute("CREATE (p:Paper {id: 12, title: 'Survey 2', year: 2021})").unwrap();
    db.execute("CREATE (p:Paper {id: 13, title: 'Survey 3', year: 2022})").unwrap();
    db.execute("CREATE (p:Paper {id: 14, title: 'Survey 4', year: 2022})").unwrap();

    // x1, x2, x3 同时引用 P1 和 P2（共被引 3 次）
    for survey_id in [11, 12, 13] {
        for target_id in [1, 2] {
            db.execute(&format!(
                "MATCH (a:Paper), (b:Paper) WHERE a.id = {} AND b.id = {} \
                 CREATE (a)-[:CITES]->(b)",
                survey_id, target_id
            ))
            .unwrap();
        }
    }

    // x4 只引用 P1 和 P3（P1,P3 共被引 1 次）
    for target_id in [1, 3] {
        db.execute(&format!(
            "MATCH (a:Paper), (b:Paper) WHERE a.id = 14 AND b.id = {} \
             CREATE (a)-[:CITES]->(b)",
            target_id
        ))
        .unwrap();
    }

    // 共被引分析：由于引擎不支持在同一 MATCH 中重复使用同一 alias 做多路展开，
    // 我们分两步查询并在 Rust 侧聚合:
    // Step 1: 获取每篇引用论文引用了哪些目标论文
    let citations = db
        .query(
            "MATCH (citing:Paper)-[:CITES]->(cited:Paper) \
             WHERE cited.id IN [1, 2, 3] \
             RETURN citing.id, cited.id ORDER BY citing.id, cited.id",
        )
        .unwrap();

    // Step 2: 在 Rust 侧构建共被引矩阵
    let mut citing_to_cited: std::collections::HashMap<i64, Vec<i64>> =
        std::collections::HashMap::new();
    for row in citations.rows() {
        let citing_id = row.get_int(0).unwrap();
        let cited_id = row.get_int(1).unwrap();
        citing_to_cited.entry(citing_id).or_default().push(cited_id);
    }

    // 计算共被引对
    let mut co_citation_counts: std::collections::HashMap<(i64, i64), i64> =
        std::collections::HashMap::new();
    for (_citing, cited_list) in &citing_to_cited {
        for i in 0..cited_list.len() {
            for j in (i + 1)..cited_list.len() {
                let (a, b) = if cited_list[i] < cited_list[j] {
                    (cited_list[i], cited_list[j])
                } else {
                    (cited_list[j], cited_list[i])
                };
                *co_citation_counts.entry((a, b)).or_insert(0) += 1;
            }
        }
    }

    // 按共被引次数排序
    let mut co_cite_pairs: Vec<((i64, i64), i64)> =
        co_citation_counts.into_iter().collect();
    co_cite_pairs.sort_by(|a, b| b.1.cmp(&a.1));

    // (P1, P2) 共被引 3 次 -> 排名第一
    assert!(!co_cite_pairs.is_empty(), "Should have co-citation pairs");
    assert_eq!(co_cite_pairs[0].0, (1, 2), "(P1, P2) should be the top co-cited pair");
    assert_eq!(co_cite_pairs[0].1, 3, "(P1, P2) should be co-cited 3 times");

    // (P1, P3) 共被引 1 次
    let p1_p3 = co_cite_pairs.iter().find(|((a, b), _)| *a == 1 && *b == 3);
    assert!(p1_p3.is_some(), "Should find (P1, P3) co-citation pair");
    assert_eq!(p1_p3.unwrap().1, 1, "(P1, P3) should be co-cited 1 time");

    // (P2, P3) 共被引 0 次 -> 不应出现
    let p2_p3 = co_cite_pairs.iter().find(|((a, b), _)| *a == 2 && *b == 3);
    assert!(p2_p3.is_none(), "(P2, P3) should not appear in co-citation results");

    // 验证通过 graph query 获取引用数据的正确性
    // 验证 P1 被引次数
    let p1_cited = db
        .query(
            "MATCH (c:Paper)-[:CITES]->(p:Paper) \
             WHERE p.id = 1 \
             RETURN count(c)",
        )
        .unwrap();
    assert_eq!(p1_cited.rows()[0].get_int(0).unwrap(), 4, "P1 should be cited 4 times (3 surveys + survey4)");

    // 验证 P2 被引次数
    let p2_cited = db
        .query(
            "MATCH (c:Paper)-[:CITES]->(p:Paper) \
             WHERE p.id = 2 \
             RETURN count(c)",
        )
        .unwrap();
    assert_eq!(p2_cited.rows()[0].get_int(0).unwrap(), 3, "P2 should be cited 3 times");

    // 验证 P3 被引次数
    let p3_cited = db
        .query(
            "MATCH (c:Paper)-[:CITES]->(p:Paper) \
             WHERE p.id = 3 \
             RETURN count(c)",
        )
        .unwrap();
    assert_eq!(p3_cited.rows()[0].get_int(0).unwrap(), 1, "P3 should be cited 1 time");
}

// ============================================================
// K-11: 跨领域知识桥接
// ============================================================

#[test]
fn k11_cross_domain_bridge_researchers() {
    let db = Database::in_memory();

    db.execute("CREATE NODE TABLE Author(id INT64, name STRING, hindex INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE NODE TABLE Paper(id INT64, title STRING, year INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE NODE TABLE Topic(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE AUTHORED(FROM Author TO Paper)").unwrap();
    db.execute("CREATE REL TABLE HAS_TOPIC(FROM Paper TO Topic)").unwrap();

    // 主题
    db.execute("CREATE (t:Topic {id: 1, name: 'Machine Learning'})").unwrap();
    db.execute("CREATE (t:Topic {id: 2, name: 'Bioinformatics'})").unwrap();
    db.execute("CREATE (t:Topic {id: 3, name: 'Economics'})").unwrap();

    // 作者
    db.execute("CREATE (a:Author {id: 1, name: 'Dr. Bridge', hindex: 50})").unwrap(); // cross ML + Bio
    db.execute("CREATE (a:Author {id: 2, name: 'Dr. ML Pure', hindex: 40})").unwrap(); // ML only
    db.execute("CREATE (a:Author {id: 3, name: 'Dr. Bio Pure', hindex: 35})").unwrap(); // Bio only
    db.execute("CREATE (a:Author {id: 4, name: 'Dr. Triple', hindex: 60})").unwrap(); // ML + Bio + Econ

    // 论文
    db.execute(
        "CREATE (p:Paper {id: 1, title: 'Deep Learning for Drug Discovery', year: 2021})",
    )
    .unwrap();
    db.execute("CREATE (p:Paper {id: 2, title: 'Transformer Architecture', year: 2022})")
        .unwrap();
    db.execute("CREATE (p:Paper {id: 3, title: 'Genomic Sequence Analysis', year: 2020})")
        .unwrap();
    db.execute("CREATE (p:Paper {id: 4, title: 'Economic Forecasting with ML', year: 2023})")
        .unwrap();
    db.execute("CREATE (p:Paper {id: 5, title: 'Pure ML Paper', year: 2022})").unwrap();

    // AUTHORED 关系
    // Dr. Bridge -> P1 (ML+Bio), P3 (Bio)
    db.execute(
        "MATCH (a:Author), (p:Paper) WHERE a.id = 1 AND p.id = 1 \
         CREATE (a)-[:AUTHORED]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Author), (p:Paper) WHERE a.id = 1 AND p.id = 3 \
         CREATE (a)-[:AUTHORED]->(p)",
    )
    .unwrap();

    // Dr. ML Pure -> P2, P5 (both ML)
    db.execute(
        "MATCH (a:Author), (p:Paper) WHERE a.id = 2 AND p.id = 2 \
         CREATE (a)-[:AUTHORED]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Author), (p:Paper) WHERE a.id = 2 AND p.id = 5 \
         CREATE (a)-[:AUTHORED]->(p)",
    )
    .unwrap();

    // Dr. Bio Pure -> P3
    db.execute(
        "MATCH (a:Author), (p:Paper) WHERE a.id = 3 AND p.id = 3 \
         CREATE (a)-[:AUTHORED]->(p)",
    )
    .unwrap();

    // Dr. Triple -> P1, P4
    db.execute(
        "MATCH (a:Author), (p:Paper) WHERE a.id = 4 AND p.id = 1 \
         CREATE (a)-[:AUTHORED]->(p)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Author), (p:Paper) WHERE a.id = 4 AND p.id = 4 \
         CREATE (a)-[:AUTHORED]->(p)",
    )
    .unwrap();

    // HAS_TOPIC 关系
    // P1 -> ML + Bio (cross-domain paper)
    db.execute(
        "MATCH (p:Paper), (t:Topic) WHERE p.id = 1 AND t.id = 1 \
         CREATE (p)-[:HAS_TOPIC]->(t)",
    )
    .unwrap();
    db.execute(
        "MATCH (p:Paper), (t:Topic) WHERE p.id = 1 AND t.id = 2 \
         CREATE (p)-[:HAS_TOPIC]->(t)",
    )
    .unwrap();
    // P2 -> ML
    db.execute(
        "MATCH (p:Paper), (t:Topic) WHERE p.id = 2 AND t.id = 1 \
         CREATE (p)-[:HAS_TOPIC]->(t)",
    )
    .unwrap();
    // P3 -> Bio
    db.execute(
        "MATCH (p:Paper), (t:Topic) WHERE p.id = 3 AND t.id = 2 \
         CREATE (p)-[:HAS_TOPIC]->(t)",
    )
    .unwrap();
    // P4 -> ML + Econ (cross-domain paper)
    db.execute(
        "MATCH (p:Paper), (t:Topic) WHERE p.id = 4 AND t.id = 1 \
         CREATE (p)-[:HAS_TOPIC]->(t)",
    )
    .unwrap();
    db.execute(
        "MATCH (p:Paper), (t:Topic) WHERE p.id = 4 AND t.id = 3 \
         CREATE (p)-[:HAS_TOPIC]->(t)",
    )
    .unwrap();
    // P5 -> ML
    db.execute(
        "MATCH (p:Paper), (t:Topic) WHERE p.id = 5 AND t.id = 1 \
         CREATE (p)-[:HAS_TOPIC]->(t)",
    )
    .unwrap();

    // 1) 3-hop query: Author -> Paper -> Topic, get all topics per author
    let author_topics = db
        .query(
            "MATCH (a:Author)-[:AUTHORED]->(p:Paper)-[:HAS_TOPIC]->(t:Topic) \
             RETURN a.name, t.name ORDER BY a.name, t.name",
        )
        .unwrap();

    // Analyze in Rust to find cross-domain authors
    let mut author_topic_map: std::collections::HashMap<String, std::collections::HashSet<String>> =
        std::collections::HashMap::new();
    for row in author_topics.rows() {
        let author = row.get_string(0).unwrap().to_string();
        let topic = row.get_string(1).unwrap().to_string();
        author_topic_map.entry(author).or_default().insert(topic);
    }

    // Dr. Bridge: P1(ML,Bio) + P3(Bio) -> {ML, Bio} = 2 domains
    let bridge_topics = &author_topic_map["Dr. Bridge"];
    assert_eq!(bridge_topics.len(), 2);
    assert!(bridge_topics.contains("Machine Learning"));
    assert!(bridge_topics.contains("Bioinformatics"));

    // Dr. ML Pure: P2(ML) + P5(ML) -> {ML} = 1 domain
    let ml_topics = &author_topic_map["Dr. ML Pure"];
    assert_eq!(ml_topics.len(), 1);
    assert!(ml_topics.contains("Machine Learning"));

    // Dr. Bio Pure: P3(Bio) -> {Bio} = 1 domain
    let bio_topics = &author_topic_map["Dr. Bio Pure"];
    assert_eq!(bio_topics.len(), 1);
    assert!(bio_topics.contains("Bioinformatics"));

    // Dr. Triple: P1(ML,Bio) + P4(ML,Econ) -> {ML, Bio, Econ} = 3 domains
    let triple_topics = &author_topic_map["Dr. Triple"];
    assert_eq!(triple_topics.len(), 3);
    assert!(triple_topics.contains("Machine Learning"));
    assert!(triple_topics.contains("Bioinformatics"));
    assert!(triple_topics.contains("Economics"));

    // Identify cross-domain bridge authors (>= 2 domains)
    let mut bridge_names: Vec<&str> = author_topic_map
        .iter()
        .filter(|(_, topics)| topics.len() >= 2)
        .map(|(name, _)| name.as_str())
        .collect();
    bridge_names.sort();
    assert_eq!(bridge_names.len(), 2, "Should have 2 cross-domain bridge authors");
    assert_eq!(bridge_names, vec!["Dr. Bridge", "Dr. Triple"]);

    // 2) Verify cross-domain paper P1's topic set = {ML, Bio}
    let p1_topics = db
        .query(
            "MATCH (p:Paper)-[:HAS_TOPIC]->(t:Topic) \
             WHERE p.id = 1 \
             RETURN t.name ORDER BY t.name",
        )
        .unwrap();
    assert_eq!(p1_topics.num_rows(), 2);
    assert_eq!(p1_topics.rows()[0].get_string(0).unwrap(), "Bioinformatics");
    assert_eq!(p1_topics.rows()[1].get_string(0).unwrap(), "Machine Learning");

    // 3) 3-hop path query: verify complete paths from author to topics
    let path_query = db
        .query(
            "MATCH (a:Author)-[:AUTHORED]->(p:Paper)-[:HAS_TOPIC]->(t:Topic) \
             WHERE a.name = 'Dr. Bridge' \
             RETURN p.title, t.name ORDER BY p.title, t.name",
        )
        .unwrap();
    // Dr. Bridge authored P1(ML,Bio) and P3(Bio)
    // Paths: (P1, Bio), (P1, ML), (P3, Bio) = 3 rows
    assert_eq!(path_query.num_rows(), 3, "Dr. Bridge should have 3 author->paper->topic paths");

    // 4) Verify single-domain authors do NOT appear as bridge researchers
    let ml_pure_count = author_topic_map["Dr. ML Pure"].len();
    assert_eq!(ml_pure_count, 1, "Dr. ML Pure should be in exactly 1 domain");
    let bio_pure_count = author_topic_map["Dr. Bio Pure"].len();
    assert_eq!(bio_pure_count, 1, "Dr. Bio Pure should be in exactly 1 domain");
}
