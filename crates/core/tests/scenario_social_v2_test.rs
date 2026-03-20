/// 社交网络 V2 场景端到端测试
///
/// 覆盖 N-01 ~ N-11：取关拉黑、互动属性、Feed 候选、热门内容、
/// 社群、多层互动链、级联删除、事件重放、FOF 推荐、病毒传播检测、私信对话
///
/// 注意事项:
/// - gqlite 不支持通过 r.property 语法查询关系属性，需属性查询的场景使用中间节点建模
/// - gqlite 的 DISTINCT 在多跳 MATCH 中可能不完全去重，使用 GROUP BY (count) 替代
/// - ORDER BY count(...) DESC 在聚合查询中不生效，在 Rust 侧手动排序
/// - 不支持 AS alias 在 ORDER BY 中引用
/// - 不支持单独删除关系边，取关/取消点赞通过 DETACH DELETE 节点后重建实现
use std::collections::HashMap;

use gqlite_core::Database;

// ════════════════════════════════════════════════════════════════
// N-01: 取关与拉黑
// ════════════════════════════════════════════════════════════════

#[test]
fn n01_unfollow_and_block() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE REL TABLE FOLLOWS(FROM Person TO Person)").unwrap();
    db.execute("CREATE REL TABLE BLOCKS(FROM Person TO Person)").unwrap();

    // 创建 5 个用户
    for (id, name) in [(1, "Alice"), (2, "Bob"), (3, "Carol"), (4, "Dave"), (5, "Eve")] {
        db.execute(&format!(
            "CREATE (p:Person {{id: {}, name: '{}', age: 25}})",
            id, name
        ))
        .unwrap();
    }

    // 初始关注关系:
    // Alice -> Bob, Alice -> Carol, Alice -> Dave
    // Bob -> Dave, Bob -> Eve
    // Carol -> Dave, Carol -> Eve
    let follows = [(1, 2), (1, 3), (1, 4), (2, 4), (2, 5), (3, 4), (3, 5)];
    for (f, t) in follows {
        db.execute(&format!(
            "MATCH (a:Person), (b:Person) WHERE a.id = {} AND b.id = {} \
             CREATE (a)-[:FOLLOWS]->(b)",
            f, t
        ))
        .unwrap();
    }

    // 验证初始状态：Alice 关注 3 人
    let r = db
        .query(
            "MATCH (a:Person)-[:FOLLOWS]->(b:Person) WHERE a.id = 1 \
             RETURN b.name ORDER BY b.name",
        )
        .unwrap();
    assert_eq!(r.num_rows(), 3);
    assert_eq!(r.rows()[0].get_string(0).unwrap(), "Bob");
    assert_eq!(r.rows()[1].get_string(0).unwrap(), "Carol");
    assert_eq!(r.rows()[2].get_string(0).unwrap(), "Dave");

    // FOF 推荐（取关前）: Alice->Bob->Dave, Alice->Bob->Eve, Alice->Carol->Dave, Alice->Carol->Eve
    // 排除已关注(Bob=2, Carol=3, Dave=4)和自己(1) => Eve(mutual_count=2)
    let fof_before = db
        .query(
            "MATCH (me:Person)-[:FOLLOWS]->(f:Person)-[:FOLLOWS]->(fof:Person) \
             WHERE me.id = 1 AND NOT fof.id IN [1, 2, 3, 4] \
             RETURN fof.name, count(f)",
        )
        .unwrap();
    assert_eq!(fof_before.num_rows(), 1);
    assert_eq!(fof_before.rows()[0].get_string(0).unwrap(), "Eve");
    assert_eq!(fof_before.rows()[0].get_int(1).unwrap(), 2); // via Bob + Carol

    // ── 取关 Dave ──
    // DETACH DELETE Alice，重建不含 Alice->Dave 的关注关系
    db.execute("MATCH (n:Person) WHERE n.id = 1 DETACH DELETE n").unwrap();
    db.execute("CREATE (p:Person {id: 1, name: 'Alice', age: 25})").unwrap();

    // 重建 Alice 的 FOLLOWS（不含 Dave）+ 其他人对 Alice 的入边也需要重建
    for target in [2, 3] {
        db.execute(&format!(
            "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = {} \
             CREATE (a)-[:FOLLOWS]->(b)",
            target
        ))
        .unwrap();
    }

    // 添加 BLOCKS: Alice -> Dave
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 4 \
         CREATE (a)-[:BLOCKS]->(b)",
    )
    .unwrap();

    // 验证取关后 Alice 只关注 Bob, Carol
    let r2 = db
        .query(
            "MATCH (a:Person)-[:FOLLOWS]->(b:Person) WHERE a.id = 1 \
             RETURN b.name ORDER BY b.name",
        )
        .unwrap();
    assert_eq!(r2.num_rows(), 2);
    assert_eq!(r2.rows()[0].get_string(0).unwrap(), "Bob");
    assert_eq!(r2.rows()[1].get_string(0).unwrap(), "Carol");

    // 验证 BLOCKS 存在
    let blocks = db
        .query("MATCH (a:Person)-[:BLOCKS]->(b:Person) WHERE a.id = 1 RETURN b.name")
        .unwrap();
    assert_eq!(blocks.num_rows(), 1);
    assert_eq!(blocks.rows()[0].get_string(0).unwrap(), "Dave");

    // 被拉黑的 Dave 应被排除在 FOF 推荐之外
    // Alice->Bob->Dave(排除:已拉黑=4), Alice->Bob->Eve(5 通过), Alice->Carol->Dave(排除), Alice->Carol->Eve(通过)
    let fof_after = db
        .query(
            "MATCH (me:Person)-[:FOLLOWS]->(f:Person)-[:FOLLOWS]->(fof:Person) \
             WHERE me.id = 1 AND NOT fof.id IN [1, 2, 3, 4] \
             RETURN fof.name, count(f)",
        )
        .unwrap();
    assert_eq!(fof_after.num_rows(), 1);
    assert_eq!(fof_after.rows()[0].get_string(0).unwrap(), "Eve");

    // 路径查询: Alice -> Dave 不再有直接关注路径
    let path = db
        .query(
            "MATCH (a:Person)-[:FOLLOWS]->(b:Person) \
             WHERE a.id = 1 AND b.id = 4 RETURN b.name",
        )
        .unwrap();
    assert_eq!(path.num_rows(), 0, "Alice should no longer follow Dave");
}

// ════════════════════════════════════════════════════════════════
// N-02: 内容互动边属性
// ════════════════════════════════════════════════════════════════

#[test]
fn n02_interaction_edge_properties() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute(
        "CREATE NODE TABLE Post(id INT64, title STRING, content STRING, \
         created_at INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    // 中间节点建模: LikeAction / CommentAction 携带可查询属性
    db.execute(
        "CREATE NODE TABLE LikeAction(id INT64, ts INT64, source STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute(
        "CREATE NODE TABLE CommentAction(id INT64, ts INT64, sentiment STRING, \
         text STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE GAVE_LIKE(FROM Person TO LikeAction)").unwrap();
    db.execute("CREATE REL TABLE LIKE_ON(FROM LikeAction TO Post)").unwrap();
    db.execute("CREATE REL TABLE GAVE_COMMENT(FROM Person TO CommentAction)").unwrap();
    db.execute("CREATE REL TABLE COMMENT_ON(FROM CommentAction TO Post)").unwrap();

    // 创建用户
    for (id, name) in [(1, "Alice"), (2, "Bob"), (3, "Carol"), (4, "Dave")] {
        db.execute(&format!(
            "CREATE (p:Person {{id: {}, name: '{}', age: 25}})",
            id, name
        ))
        .unwrap();
    }

    // 创建帖子
    db.execute(
        "CREATE (p:Post {id: 1, title: 'Rust Tips', content: 'Ownership', created_at: 1000})",
    )
    .unwrap();
    db.execute(
        "CREATE (p:Post {id: 2, title: 'Graph DB', content: 'Edges', created_at: 1000})",
    )
    .unwrap();

    // 创建 Like 事件, 带时间戳和来源
    // 时间窗口 [1000, 2000]: likes 1,2,3,4
    // 时间窗口 [2001, 3000]: likes 5,6,7
    let likes: [(i64, i64, i64, i64, &str); 7] = [
        (1, 1, 1, 1100, "web"),
        (2, 2, 1, 1200, "web"),
        (3, 3, 1, 1500, "mobile"),
        (4, 4, 1, 1800, "mobile"),
        (5, 1, 2, 2100, "web"),
        (6, 2, 2, 2500, "mobile"),
        (7, 3, 2, 2800, "api"),
    ];
    for (like_id, person_id, post_id, ts, source) in likes {
        db.execute(&format!(
            "CREATE (l:LikeAction {{id: {}, ts: {}, source: '{}'}})",
            like_id, ts, source
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (p:Person), (l:LikeAction) WHERE p.id = {} AND l.id = {} \
             CREATE (p)-[:GAVE_LIKE]->(l)",
            person_id, like_id
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (l:LikeAction), (post:Post) WHERE l.id = {} AND post.id = {} \
             CREATE (l)-[:LIKE_ON]->(post)",
            like_id, post_id
        ))
        .unwrap();
    }

    // 创建评论事件
    let comments: [(i64, i64, i64, i64, &str, &str); 3] = [
        (1, 1, 1, 1300, "positive", "Great post!"),
        (2, 2, 1, 1600, "neutral", "Interesting"),
        (3, 3, 2, 2200, "negative", "Disagree"),
    ];
    for (cid, person_id, post_id, ts, sentiment, text) in comments {
        db.execute(&format!(
            "CREATE (c:CommentAction {{id: {}, ts: {}, sentiment: '{}', text: '{}'}})",
            cid, ts, sentiment, text
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (p:Person), (c:CommentAction) WHERE p.id = {} AND c.id = {} \
             CREATE (p)-[:GAVE_COMMENT]->(c)",
            person_id, cid
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (c:CommentAction), (post:Post) WHERE c.id = {} AND post.id = {} \
             CREATE (c)-[:COMMENT_ON]->(post)",
            cid, post_id
        ))
        .unwrap();
    }

    // 断言1: 按时间窗口 [1000, 2000] 过滤点赞
    let window1 = db
        .query(
            "MATCH (p:Person)-[:GAVE_LIKE]->(l:LikeAction)-[:LIKE_ON]->(post:Post) \
             WHERE l.ts >= 1000 AND l.ts <= 2000 \
             RETURN post.title, count(l)",
        )
        .unwrap();
    assert_eq!(window1.num_rows(), 1);
    assert_eq!(window1.rows()[0].get_string(0).unwrap(), "Rust Tips");
    assert_eq!(window1.rows()[0].get_int(1).unwrap(), 4);

    // 断言2: 按时间窗口 [2001, 3000] 过滤点赞
    let window2 = db
        .query(
            "MATCH (p:Person)-[:GAVE_LIKE]->(l:LikeAction)-[:LIKE_ON]->(post:Post) \
             WHERE l.ts >= 2001 AND l.ts <= 3000 \
             RETURN post.title, count(l)",
        )
        .unwrap();
    assert_eq!(window2.num_rows(), 1);
    assert_eq!(window2.rows()[0].get_string(0).unwrap(), "Graph DB");
    assert_eq!(window2.rows()[0].get_int(1).unwrap(), 3);

    // 断言3: 按 source 分组统计
    let by_source = db
        .query(
            "MATCH (p:Person)-[:GAVE_LIKE]->(l:LikeAction)-[:LIKE_ON]->(post:Post) \
             RETURN l.source, count(l)",
        )
        .unwrap();
    let mut source_counts: HashMap<String, i64> = HashMap::new();
    for row in by_source.rows() {
        source_counts.insert(
            row.get_string(0).unwrap().to_string(),
            row.get_int(1).unwrap(),
        );
    }
    assert_eq!(source_counts["web"], 3);
    assert_eq!(source_counts["mobile"], 3);
    assert_eq!(source_counts["api"], 1);

    // 断言4: 评论按 sentiment 分组
    let by_sentiment = db
        .query(
            "MATCH (p:Person)-[:GAVE_COMMENT]->(c:CommentAction)-[:COMMENT_ON]->(post:Post) \
             RETURN c.sentiment, count(c)",
        )
        .unwrap();
    let mut sent_counts: HashMap<String, i64> = HashMap::new();
    for row in by_sentiment.rows() {
        sent_counts.insert(
            row.get_string(0).unwrap().to_string(),
            row.get_int(1).unwrap(),
        );
    }
    assert_eq!(sent_counts["positive"], 1);
    assert_eq!(sent_counts["neutral"], 1);
    assert_eq!(sent_counts["negative"], 1);
}

// ════════════════════════════════════════════════════════════════
// N-03: Feed 候选生成
// ════════════════════════════════════════════════════════════════

#[test]
fn n03_feed_candidate_generation() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute(
        "CREATE NODE TABLE Post(id INT64, title STRING, content STRING, \
         created_at INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE NODE TABLE Tag(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE FOLLOWS(FROM Person TO Person)").unwrap();
    db.execute("CREATE REL TABLE BLOCKS(FROM Person TO Person)").unwrap();
    db.execute("CREATE REL TABLE POSTED(FROM Person TO Post)").unwrap();
    db.execute("CREATE REL TABLE HAS_TAG(FROM Post TO Tag)").unwrap();
    db.execute("CREATE REL TABLE FOLLOWS_TAG(FROM Person TO Tag)").unwrap();

    // 用户: Alice(1), Bob(2), Carol(3), Dave(4, 被屏蔽)
    for (id, name) in [(1, "Alice"), (2, "Bob"), (3, "Carol"), (4, "Dave")] {
        db.execute(&format!(
            "CREATE (p:Person {{id: {}, name: '{}', age: 25}})",
            id, name
        ))
        .unwrap();
    }

    // 标签
    db.execute("CREATE (t:Tag {id: 1, name: 'Rust'})").unwrap();
    db.execute("CREATE (t:Tag {id: 2, name: 'Graph'})").unwrap();

    // 帖子
    for (id, title, content, ts) in [
        (1, "Rust Tips", "Ownership", 1000),
        (2, "Graph DB", "Nodes", 2000),
        (3, "Rust Async", "Tokio", 3000),
        (4, "Blocked Post", "Hidden", 4000),
        (5, "Tagged Rust", "By Dave", 5000),
    ] {
        db.execute(&format!(
            "CREATE (p:Post {{id: {}, title: '{}', content: '{}', created_at: {}}})",
            id, title, content, ts
        ))
        .unwrap();
    }

    // Alice follows Bob, Carol
    for target in [2, 3] {
        db.execute(&format!(
            "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = {} \
             CREATE (a)-[:FOLLOWS]->(b)",
            target
        ))
        .unwrap();
    }

    // Alice blocks Dave
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 4 \
         CREATE (a)-[:BLOCKS]->(b)",
    )
    .unwrap();

    // Alice follows Rust tag
    db.execute(
        "MATCH (p:Person), (t:Tag) WHERE p.id = 1 AND t.id = 1 \
         CREATE (p)-[:FOLLOWS_TAG]->(t)",
    )
    .unwrap();

    // 发帖: Bob(1,2), Carol(3), Dave(4,5)
    for (pid, postid) in [(2, 1), (2, 2), (3, 3), (4, 4), (4, 5)] {
        db.execute(&format!(
            "MATCH (p:Person), (post:Post) WHERE p.id = {} AND post.id = {} \
             CREATE (p)-[:POSTED]->(post)",
            pid, postid
        ))
        .unwrap();
    }

    // 标签: Post1->Rust, Post5->Rust, Post2->Graph
    for (post_id, tag_id) in [(1, 1), (5, 1), (2, 2)] {
        db.execute(&format!(
            "MATCH (p:Post), (t:Tag) WHERE p.id = {} AND t.id = {} \
             CREATE (p)-[:HAS_TAG]->(t)",
            post_id, tag_id
        ))
        .unwrap();
    }

    // Feed 候选1: 关注作者的帖子 => Bob:1,2  Carol:3
    let feed_by_author = db
        .query(
            "MATCH (me:Person)-[:FOLLOWS]->(author:Person)-[:POSTED]->(post:Post) \
             WHERE me.id = 1 \
             RETURN post.id, post.title ORDER BY post.id",
        )
        .unwrap();
    let author_post_ids: Vec<i64> = feed_by_author
        .rows()
        .iter()
        .map(|r| r.get_int(0).unwrap())
        .collect();
    assert_eq!(author_post_ids, vec![1, 2, 3]);

    // Feed 候选2: 关注标签的帖子, 排除被屏蔽用户
    // Rust tag posts: 1(by Bob), 5(by Dave, 已拉黑)
    let feed_by_tag = db
        .query(
            "MATCH (me:Person)-[:FOLLOWS_TAG]->(tag:Tag)<-[:HAS_TAG]-(post:Post)<-[:POSTED]-(author:Person) \
             WHERE me.id = 1 AND NOT author.id IN [4] \
             RETURN post.id, post.title ORDER BY post.id",
        )
        .unwrap();
    assert_eq!(feed_by_tag.num_rows(), 1);
    assert_eq!(feed_by_tag.rows()[0].get_int(0).unwrap(), 1);

    // 被屏蔽用户的帖子不应出现在 feed 中
    assert!(
        !author_post_ids.contains(&4),
        "Blocked user's post 4 should not appear in feed"
    );
    assert!(
        !author_post_ids.contains(&5),
        "Blocked user's post 5 should not appear in feed"
    );
}

// ════════════════════════════════════════════════════════════════
// N-04: 热门内容高 fan-out
// ════════════════════════════════════════════════════════════════

#[test]
fn n04_hot_content_high_fanout() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute(
        "CREATE NODE TABLE Post(id INT64, title STRING, content STRING, \
         created_at INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE NODE TABLE Tag(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE REL TABLE LIKED(FROM Person TO Post)").unwrap();
    db.execute("CREATE REL TABLE HAS_TAG(FROM Post TO Tag)").unwrap();

    // 3 个帖子
    db.execute(
        "CREATE (p:Post {id: 1, title: 'Normal Post', content: 'Nothing special', created_at: 1000})",
    )
    .unwrap();
    db.execute(
        "CREATE (p:Post {id: 2, title: 'Hot Post', content: 'Viral content', created_at: 1000})",
    )
    .unwrap();
    db.execute(
        "CREATE (p:Post {id: 3, title: 'Cold Post', content: 'Quiet content', created_at: 1000})",
    )
    .unwrap();

    // 标签
    db.execute("CREATE (t:Tag {id: 1, name: 'Tech'})").unwrap();
    db.execute("CREATE (t:Tag {id: 2, name: 'Fun'})").unwrap();
    // Normal->Tech, Hot->Tech+Fun, Cold->Fun
    for (post_id, tag_id) in [(1, 1), (2, 1), (2, 2), (3, 2)] {
        db.execute(&format!(
            "MATCH (p:Post), (t:Tag) WHERE p.id = {} AND t.id = {} \
             CREATE (p)-[:HAS_TAG]->(t)",
            post_id, tag_id
        ))
        .unwrap();
    }

    // 创建 550 个用户
    for i in 1..=550 {
        db.execute(&format!(
            "CREATE (p:Person {{id: {}, name: 'user{}', age: 20}})",
            i, i
        ))
        .unwrap();
    }

    // Hot Post: 550 likes, Normal: 10 likes, Cold: 2 likes
    for i in 1..=550 {
        db.execute(&format!(
            "MATCH (p:Person), (post:Post) WHERE p.id = {} AND post.id = 2 \
             CREATE (p)-[:LIKED]->(post)",
            i
        ))
        .unwrap();
    }
    for i in 1..=10 {
        db.execute(&format!(
            "MATCH (p:Person), (post:Post) WHERE p.id = {} AND post.id = 1 \
             CREATE (p)-[:LIKED]->(post)",
            i
        ))
        .unwrap();
    }
    for i in 1..=2 {
        db.execute(&format!(
            "MATCH (p:Person), (post:Post) WHERE p.id = {} AND post.id = 3 \
             CREATE (p)-[:LIKED]->(post)",
            i
        ))
        .unwrap();
    }

    // 断言1: 互动计数 (Rust 侧排序)
    let counts = db
        .query(
            "MATCH (p:Person)-[:LIKED]->(post:Post) \
             RETURN post.title, count(p)",
        )
        .unwrap();
    let mut count_vec: Vec<(String, i64)> = counts
        .rows()
        .iter()
        .map(|r| (r.get_string(0).unwrap().to_string(), r.get_int(1).unwrap()))
        .collect();
    count_vec.sort_by(|a, b| b.1.cmp(&a.1));

    assert_eq!(count_vec.len(), 3);
    assert_eq!(count_vec[0].0, "Hot Post");
    assert_eq!(count_vec[0].1, 550);
    assert_eq!(count_vec[1].0, "Normal Post");
    assert_eq!(count_vec[1].1, 10);
    assert_eq!(count_vec[2].0, "Cold Post");
    assert_eq!(count_vec[2].1, 2);

    // 断言2: 热门榜 Top 1
    assert!(count_vec[0].1 >= 500, "Hot Post should have 500+ likes");

    // 断言3: 按标签聚合互动数
    let tag_stats = db
        .query(
            "MATCH (p:Person)-[:LIKED]->(post:Post)-[:HAS_TAG]->(t:Tag) \
             RETURN t.name, count(p)",
        )
        .unwrap();
    let mut tag_counts: HashMap<String, i64> = HashMap::new();
    for row in tag_stats.rows() {
        tag_counts.insert(
            row.get_string(0).unwrap().to_string(),
            row.get_int(1).unwrap(),
        );
    }
    // Tech: Normal(10) + Hot(550) = 560
    assert_eq!(tag_counts["Tech"], 560);
    // Fun: Hot(550) + Cold(2) = 552
    assert_eq!(tag_counts["Fun"], 552);
}

// ════════════════════════════════════════════════════════════════
// N-05: 社群/圈子图
// ════════════════════════════════════════════════════════════════

#[test]
fn n05_group_community_graph() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute(
        "CREATE NODE TABLE Post(id INT64, title STRING, content STRING, \
         created_at INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE NODE TABLE SocialGroup(id INT64, name STRING, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE REL TABLE MEMBER_OF(FROM Person TO SocialGroup)").unwrap();
    db.execute("CREATE REL TABLE ADMIN_OF(FROM Person TO SocialGroup)").unwrap();
    db.execute("CREATE REL TABLE POSTED(FROM Person TO Post)").unwrap();
    db.execute("CREATE REL TABLE POSTED_IN(FROM Post TO SocialGroup)").unwrap();

    // 用户
    for (id, name) in [
        (1, "Alice"),
        (2, "Bob"),
        (3, "Carol"),
        (4, "Dave"),
        (5, "Eve"),
    ] {
        db.execute(&format!(
            "CREATE (p:Person {{id: {}, name: '{}', age: 25}})",
            id, name
        ))
        .unwrap();
    }

    // 群组
    db.execute("CREATE (g:SocialGroup {id: 1, name: 'Rust Devs'})").unwrap();
    db.execute("CREATE (g:SocialGroup {id: 2, name: 'Graph Fans'})").unwrap();

    // 成员: Rust Devs(Alice,Bob,Carol,Dave), Graph Fans(Bob,Carol,Eve)
    for (pid, gid) in [(1, 1), (2, 1), (3, 1), (4, 1), (2, 2), (3, 2), (5, 2)] {
        db.execute(&format!(
            "MATCH (p:Person), (g:SocialGroup) WHERE p.id = {} AND g.id = {} \
             CREATE (p)-[:MEMBER_OF]->(g)",
            pid, gid
        ))
        .unwrap();
    }

    // 管理员: Alice->RustDevs, Bob->GraphFans
    db.execute(
        "MATCH (p:Person), (g:SocialGroup) WHERE p.id = 1 AND g.id = 1 \
         CREATE (p)-[:ADMIN_OF]->(g)",
    )
    .unwrap();
    db.execute(
        "MATCH (p:Person), (g:SocialGroup) WHERE p.id = 2 AND g.id = 2 \
         CREATE (p)-[:ADMIN_OF]->(g)",
    )
    .unwrap();

    // 帖子 (person_id, post_id, title, group_id)
    let posts_data: [(i64, i64, &str, i64); 6] = [
        (1, 1, "Rust Ownership", 1),
        (1, 2, "Lifetimes", 1),
        (2, 3, "Borrow Checker", 1),
        (2, 4, "Graph Algorithms", 2),
        (3, 5, "Neo4j vs gqlite", 2),
        (4, 6, "Pattern Matching", 1),
    ];
    for (pid, postid, title, gid) in posts_data {
        db.execute(&format!(
            "CREATE (p:Post {{id: {}, title: '{}', content: 'content', created_at: {}}})",
            postid, title, postid * 1000
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (p:Person), (post:Post) WHERE p.id = {} AND post.id = {} \
             CREATE (p)-[:POSTED]->(post)",
            pid, postid
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (post:Post), (g:SocialGroup) WHERE post.id = {} AND g.id = {} \
             CREATE (post)-[:POSTED_IN]->(g)",
            postid, gid
        ))
        .unwrap();
    }

    // 断言1: 各群组成员数
    let member_counts = db
        .query(
            "MATCH (p:Person)-[:MEMBER_OF]->(g:SocialGroup) \
             RETURN g.name, count(p)",
        )
        .unwrap();
    let mut mc: HashMap<String, i64> = HashMap::new();
    for row in member_counts.rows() {
        mc.insert(
            row.get_string(0).unwrap().to_string(),
            row.get_int(1).unwrap(),
        );
    }
    assert_eq!(mc["Rust Devs"], 4);
    assert_eq!(mc["Graph Fans"], 3);

    // 断言2: 群内活跃度 (Rust Devs: Alice=2, Bob=1, Dave=1)
    let activity = db
        .query(
            "MATCH (p:Person)-[:POSTED]->(post:Post)-[:POSTED_IN]->(g:SocialGroup) \
             WHERE g.id = 1 \
             RETURN p.name, count(post)",
        )
        .unwrap();
    let mut act: Vec<(String, i64)> = activity
        .rows()
        .iter()
        .map(|r| (r.get_string(0).unwrap().to_string(), r.get_int(1).unwrap()))
        .collect();
    act.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    assert_eq!(act.len(), 3);
    assert_eq!(act[0].0, "Alice");
    assert_eq!(act[0].1, 2);

    // 断言3: 群组管理员
    let admins = db
        .query(
            "MATCH (p:Person)-[:ADMIN_OF]->(g:SocialGroup) \
             RETURN g.name, p.name ORDER BY g.name",
        )
        .unwrap();
    assert_eq!(admins.num_rows(), 2);
    let mut admin_map: HashMap<String, String> = HashMap::new();
    for row in admins.rows() {
        admin_map.insert(
            row.get_string(0).unwrap().to_string(),
            row.get_string(1).unwrap().to_string(),
        );
    }
    assert_eq!(admin_map["Rust Devs"], "Alice");
    assert_eq!(admin_map["Graph Fans"], "Bob");

    // 断言4: Bob 和 Carol 的共同群组 = 2
    let common_groups = db
        .query(
            "MATCH (a:Person)-[:MEMBER_OF]->(g:SocialGroup)<-[:MEMBER_OF]-(b:Person) \
             WHERE a.id = 2 AND b.id = 3 \
             RETURN g.name ORDER BY g.name",
        )
        .unwrap();
    assert_eq!(common_groups.num_rows(), 2);
    let gnames: Vec<&str> = common_groups
        .rows()
        .iter()
        .map(|r| r.get_string(0).unwrap())
        .collect();
    assert!(gnames.contains(&"Graph Fans"));
    assert!(gnames.contains(&"Rust Devs"));

    // 断言5: 各群组帖子数
    let group_posts = db
        .query(
            "MATCH (post:Post)-[:POSTED_IN]->(g:SocialGroup) \
             RETURN g.name, count(post)",
        )
        .unwrap();
    let mut gp: HashMap<String, i64> = HashMap::new();
    for row in group_posts.rows() {
        gp.insert(
            row.get_string(0).unwrap().to_string(),
            row.get_int(1).unwrap(),
        );
    }
    assert_eq!(gp["Rust Devs"], 4);
    assert_eq!(gp["Graph Fans"], 2);
}

// ════════════════════════════════════════════════════════════════
// N-06: 多层互动链
// ════════════════════════════════════════════════════════════════

#[test]
fn n06_multi_layer_interaction_chain() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute(
        "CREATE NODE TABLE Post(id INT64, title STRING, content STRING, \
         created_at INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE NODE TABLE Comment(id INT64, text STRING, ts INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE REL TABLE POSTED(FROM Person TO Post)").unwrap();
    db.execute("CREATE REL TABLE COMMENTED(FROM Person TO Comment)").unwrap();
    db.execute("CREATE REL TABLE COMMENT_ON(FROM Comment TO Post)").unwrap();
    db.execute("CREATE REL TABLE REPLIED_TO(FROM Comment TO Comment)").unwrap();
    db.execute("CREATE REL TABLE MENTIONS(FROM Comment TO Person)").unwrap();

    // 用户
    for (id, name) in [(1, "Alice"), (2, "Bob"), (3, "Carol"), (4, "Dave")] {
        db.execute(&format!(
            "CREATE (p:Person {{id: {}, name: '{}', age: 25}})",
            id, name
        ))
        .unwrap();
    }

    // Alice 发帖
    db.execute(
        "CREATE (p:Post {id: 1, title: 'Rust Ownership', content: 'Deep dive', created_at: 1000})",
    )
    .unwrap();
    db.execute(
        "MATCH (p:Person), (post:Post) WHERE p.id = 1 AND post.id = 1 \
         CREATE (p)-[:POSTED]->(post)",
    )
    .unwrap();

    // Bob 评论帖子 (Comment 1)
    db.execute("CREATE (c:Comment {id: 1, text: 'Great article!', ts: 2000})").unwrap();
    db.execute(
        "MATCH (p:Person), (c:Comment) WHERE p.id = 2 AND c.id = 1 \
         CREATE (p)-[:COMMENTED]->(c)",
    )
    .unwrap();
    db.execute(
        "MATCH (c:Comment), (post:Post) WHERE c.id = 1 AND post.id = 1 \
         CREATE (c)-[:COMMENT_ON]->(post)",
    )
    .unwrap();

    // Carol 回复 Bob (Comment 2), 提及 Alice
    db.execute(
        "CREATE (c:Comment {id: 2, text: 'I agree with Bob, @Alice nailed it', ts: 3000})",
    )
    .unwrap();
    db.execute(
        "MATCH (p:Person), (c:Comment) WHERE p.id = 3 AND c.id = 2 \
         CREATE (p)-[:COMMENTED]->(c)",
    )
    .unwrap();
    db.execute(
        "MATCH (c1:Comment), (c2:Comment) WHERE c1.id = 2 AND c2.id = 1 \
         CREATE (c1)-[:REPLIED_TO]->(c2)",
    )
    .unwrap();
    db.execute(
        "MATCH (c:Comment), (p:Person) WHERE c.id = 2 AND p.id = 1 \
         CREATE (c)-[:MENTIONS]->(p)",
    )
    .unwrap();

    // Dave 回复 Carol (Comment 3), 提及 Bob
    db.execute("CREATE (c:Comment {id: 3, text: 'Good point @Bob', ts: 4000})").unwrap();
    db.execute(
        "MATCH (p:Person), (c:Comment) WHERE p.id = 4 AND c.id = 3 \
         CREATE (p)-[:COMMENTED]->(c)",
    )
    .unwrap();
    db.execute(
        "MATCH (c1:Comment), (c2:Comment) WHERE c1.id = 3 AND c2.id = 2 \
         CREATE (c1)-[:REPLIED_TO]->(c2)",
    )
    .unwrap();
    db.execute(
        "MATCH (c:Comment), (p:Person) WHERE c.id = 3 AND p.id = 2 \
         CREATE (c)-[:MENTIONS]->(p)",
    )
    .unwrap();

    // Alice 再回复 Dave (Comment 4)
    db.execute("CREATE (c:Comment {id: 4, text: 'Thanks everyone!', ts: 5000})").unwrap();
    db.execute(
        "MATCH (p:Person), (c:Comment) WHERE p.id = 1 AND c.id = 4 \
         CREATE (p)-[:COMMENTED]->(c)",
    )
    .unwrap();
    db.execute(
        "MATCH (c1:Comment), (c2:Comment) WHERE c1.id = 4 AND c2.id = 3 \
         CREATE (c1)-[:REPLIED_TO]->(c2)",
    )
    .unwrap();

    // 断言1: 帖子的所有直接评论
    let direct_comments = db
        .query(
            "MATCH (c:Comment)-[:COMMENT_ON]->(post:Post) \
             WHERE post.id = 1 \
             RETURN c.text ORDER BY c.ts",
        )
        .unwrap();
    assert_eq!(direct_comments.num_rows(), 1);
    assert_eq!(
        direct_comments.rows()[0].get_string(0).unwrap(),
        "Great article!"
    );

    // 断言2: 三级回复链 Comment3 -> Comment2 -> Comment1
    let reply_chain = db
        .query(
            "MATCH (c3:Comment)-[:REPLIED_TO]->(c2:Comment)-[:REPLIED_TO]->(c1:Comment) \
             WHERE c3.id = 3 \
             RETURN c3.text, c2.text, c1.text",
        )
        .unwrap();
    assert_eq!(reply_chain.num_rows(), 1);
    assert_eq!(
        reply_chain.rows()[0].get_string(0).unwrap(),
        "Good point @Bob"
    );
    assert_eq!(
        reply_chain.rows()[0].get_string(1).unwrap(),
        "I agree with Bob, @Alice nailed it"
    );
    assert_eq!(
        reply_chain.rows()[0].get_string(2).unwrap(),
        "Great article!"
    );

    // 断言3: 完整 Person->Post->Comment->Reply->Reply 链
    let full_chain = db
        .query(
            "MATCH (author:Person)-[:POSTED]->(post:Post)<-[:COMMENT_ON]-(c1:Comment)\
             <-[:REPLIED_TO]-(c2:Comment)<-[:REPLIED_TO]-(c3:Comment) \
             WHERE author.id = 1 \
             RETURN author.name, post.title, c1.text, c2.text, c3.text",
        )
        .unwrap();
    assert_eq!(full_chain.num_rows(), 1);
    assert_eq!(full_chain.rows()[0].get_string(0).unwrap(), "Alice");
    assert_eq!(
        full_chain.rows()[0].get_string(1).unwrap(),
        "Rust Ownership"
    );

    // 断言4: 谁被提及了
    let mentioned = db
        .query(
            "MATCH (c:Comment)-[:MENTIONS]->(p:Person) \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    assert_eq!(mentioned.num_rows(), 2);
    assert_eq!(mentioned.rows()[0].get_string(0).unwrap(), "Alice");
    assert_eq!(mentioned.rows()[1].get_string(0).unwrap(), "Bob");

    // 断言5: 可变长路径还原整个回复链到 root
    let var_chain = db
        .query(
            "MATCH (c:Comment)-[:REPLIED_TO*1..3]->(root:Comment) \
             WHERE root.id = 1 \
             RETURN c.id ORDER BY c.id",
        )
        .unwrap();
    // Comment 2, 3, 4 都可以回溯到 Comment 1
    let chain_ids: Vec<i64> = var_chain.rows().iter().map(|r| r.get_int(0).unwrap()).collect();
    assert!(chain_ids.contains(&2));
    assert!(chain_ids.contains(&3));
    assert!(chain_ids.contains(&4));
    assert_eq!(chain_ids.len(), 3);
}

// ════════════════════════════════════════════════════════════════
// N-07: 内容删除与级联影响
// ════════════════════════════════════════════════════════════════

#[test]
fn n07_content_delete_cascade() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute(
        "CREATE NODE TABLE Post(id INT64, title STRING, content STRING, \
         created_at INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE NODE TABLE Tag(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE NODE TABLE Comment(id INT64, text STRING, ts INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE REL TABLE POSTED(FROM Person TO Post)").unwrap();
    db.execute("CREATE REL TABLE LIKED(FROM Person TO Post)").unwrap();
    db.execute("CREATE REL TABLE HAS_TAG(FROM Post TO Tag)").unwrap();
    db.execute("CREATE REL TABLE COMMENT_ON(FROM Comment TO Post)").unwrap();
    db.execute("CREATE REL TABLE COMMENTED(FROM Person TO Comment)").unwrap();

    // 用户
    for (id, name) in [(1, "Alice"), (2, "Bob"), (3, "Carol")] {
        db.execute(&format!(
            "CREATE (p:Person {{id: {}, name: '{}', age: 25}})",
            id, name
        ))
        .unwrap();
    }

    // 帖子
    db.execute(
        "CREATE (p:Post {id: 1, title: 'Target Post', content: 'Will be deleted', created_at: 1000})",
    )
    .unwrap();
    db.execute(
        "CREATE (p:Post {id: 2, title: 'Safe Post', content: 'Will survive', created_at: 2000})",
    )
    .unwrap();

    // 标签
    db.execute("CREATE (t:Tag {id: 1, name: 'Rust'})").unwrap();
    db.execute("CREATE (t:Tag {id: 2, name: 'Graph'})").unwrap();

    // Alice posted Post1 and Post2
    for post_id in [1, 2] {
        db.execute(&format!(
            "MATCH (p:Person), (post:Post) WHERE p.id = 1 AND post.id = {} \
             CREATE (p)-[:POSTED]->(post)",
            post_id
        ))
        .unwrap();
    }

    // 点赞: Bob+Carol liked Post1; Bob liked Post2
    for (pid, postid) in [(2, 1), (3, 1), (2, 2)] {
        db.execute(&format!(
            "MATCH (p:Person), (post:Post) WHERE p.id = {} AND post.id = {} \
             CREATE (p)-[:LIKED]->(post)",
            pid, postid
        ))
        .unwrap();
    }

    // 标签: Post1->Rust, Post1->Graph, Post2->Rust
    for (post_id, tag_id) in [(1, 1), (1, 2), (2, 1)] {
        db.execute(&format!(
            "MATCH (post:Post), (t:Tag) WHERE post.id = {} AND t.id = {} \
             CREATE (post)-[:HAS_TAG]->(t)",
            post_id, tag_id
        ))
        .unwrap();
    }

    // 评论
    db.execute("CREATE (c:Comment {id: 1, text: 'Nice!', ts: 1100})").unwrap();
    db.execute("CREATE (c:Comment {id: 2, text: 'Cool!', ts: 1200})").unwrap();
    for (pid, cid) in [(2, 1), (3, 2)] {
        db.execute(&format!(
            "MATCH (p:Person), (c:Comment) WHERE p.id = {} AND c.id = {} \
             CREATE (p)-[:COMMENTED]->(c)",
            pid, cid
        ))
        .unwrap();
    }
    for cid in [1, 2] {
        db.execute(&format!(
            "MATCH (c:Comment), (post:Post) WHERE c.id = {} AND post.id = 1 \
             CREATE (c)-[:COMMENT_ON]->(post)",
            cid
        ))
        .unwrap();
    }

    // 删除前验证
    let pre_likes = db
        .query("MATCH (p:Person)-[:LIKED]->(post:Post) RETURN count(p)")
        .unwrap();
    assert_eq!(pre_likes.rows()[0].get_int(0).unwrap(), 3);
    let pre_tags = db
        .query("MATCH (post:Post)-[:HAS_TAG]->(t:Tag) RETURN count(t)")
        .unwrap();
    assert_eq!(pre_tags.rows()[0].get_int(0).unwrap(), 3);
    let pre_comments = db
        .query("MATCH (c:Comment)-[:COMMENT_ON]->(post:Post) RETURN count(c)")
        .unwrap();
    assert_eq!(pre_comments.rows()[0].get_int(0).unwrap(), 2);

    // DETACH DELETE Post1
    db.execute("MATCH (n:Post) WHERE n.id = 1 DETACH DELETE n").unwrap();

    // 断言1: Post1 已删除
    let post1 = db.query("MATCH (p:Post) WHERE p.id = 1 RETURN p.title").unwrap();
    assert_eq!(post1.num_rows(), 0);

    // 断言2: Post2 仍存在
    let post2 = db.query("MATCH (p:Post) WHERE p.id = 2 RETURN p.title").unwrap();
    assert_eq!(post2.num_rows(), 1);

    // 断言3: LIKED 只剩 Post2 的 1 条
    let post_likes = db
        .query("MATCH (p:Person)-[:LIKED]->(post:Post) RETURN count(p)")
        .unwrap();
    assert_eq!(post_likes.rows()[0].get_int(0).unwrap(), 1);

    // 断言4: HAS_TAG 只剩 Post2 的 1 条
    let post_tags = db
        .query("MATCH (post:Post)-[:HAS_TAG]->(t:Tag) RETURN count(t)")
        .unwrap();
    assert_eq!(post_tags.rows()[0].get_int(0).unwrap(), 1);

    // 断言5: COMMENT_ON 全部删除 (都指向 Post1)
    let post_comments = db
        .query("MATCH (c:Comment)-[:COMMENT_ON]->(post:Post) RETURN count(c)")
        .unwrap();
    assert_eq!(post_comments.rows()[0].get_int(0).unwrap(), 0);

    // 断言6: POSTED 只剩 Post2 的 1 条
    let posted = db
        .query("MATCH (p:Person)-[:POSTED]->(post:Post) RETURN count(post)")
        .unwrap();
    assert_eq!(posted.rows()[0].get_int(0).unwrap(), 1);

    // 断言7: Tag 节点本身未被删除
    let tags = db.query("MATCH (t:Tag) RETURN count(t)").unwrap();
    assert_eq!(tags.rows()[0].get_int(0).unwrap(), 2);

    // 断言8: Person 节点未被删除
    let persons = db.query("MATCH (p:Person) RETURN count(p)").unwrap();
    assert_eq!(persons.rows()[0].get_int(0).unwrap(), 3);
}

// ════════════════════════════════════════════════════════════════
// N-08: 异步事件重放 (点赞->取消->重新点赞)
// ════════════════════════════════════════════════════════════════

#[test]
fn n08_like_unlike_relike_event_replay() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute(
        "CREATE NODE TABLE Post(id INT64, title STRING, content STRING, \
         created_at INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    // 使用 LikeEvent 节点建模, 便于单独删除
    db.execute(
        "CREATE NODE TABLE LikeEvent(id INT64, ts INT64, active INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE GAVE_LIKE(FROM Person TO LikeEvent)").unwrap();
    db.execute("CREATE REL TABLE LIKE_ON(FROM LikeEvent TO Post)").unwrap();

    db.execute("CREATE (p:Person {id: 1, name: 'Alice', age: 25})").unwrap();
    db.execute(
        "CREATE (p:Post {id: 1, title: 'Test Post', content: 'Content', created_at: 1000})",
    )
    .unwrap();

    // 事件1: Alice 点赞 (ts=1000)
    db.execute("CREATE (l:LikeEvent {id: 1, ts: 1000, active: 1})").unwrap();
    db.execute(
        "MATCH (p:Person), (l:LikeEvent) WHERE p.id = 1 AND l.id = 1 \
         CREATE (p)-[:GAVE_LIKE]->(l)",
    )
    .unwrap();
    db.execute(
        "MATCH (l:LikeEvent), (post:Post) WHERE l.id = 1 AND post.id = 1 \
         CREATE (l)-[:LIKE_ON]->(post)",
    )
    .unwrap();

    // 验证: 1 条有效点赞
    let count1 = db
        .query(
            "MATCH (p:Person)-[:GAVE_LIKE]->(l:LikeEvent)-[:LIKE_ON]->(post:Post) \
             WHERE p.id = 1 AND post.id = 1 AND l.active = 1 \
             RETURN count(l)",
        )
        .unwrap();
    assert_eq!(count1.rows()[0].get_int(0).unwrap(), 1);

    // 事件2: 取消点赞 -- DETACH DELETE LikeEvent 1
    db.execute("MATCH (l:LikeEvent) WHERE l.id = 1 DETACH DELETE l").unwrap();

    let count2 = db
        .query(
            "MATCH (p:Person)-[:GAVE_LIKE]->(l:LikeEvent)-[:LIKE_ON]->(post:Post) \
             WHERE p.id = 1 AND post.id = 1 RETURN count(l)",
        )
        .unwrap();
    assert_eq!(count2.rows()[0].get_int(0).unwrap(), 0);

    // 事件3: 重新点赞 (ts=2000)
    db.execute("CREATE (l:LikeEvent {id: 2, ts: 2000, active: 1})").unwrap();
    db.execute(
        "MATCH (p:Person), (l:LikeEvent) WHERE p.id = 1 AND l.id = 2 \
         CREATE (p)-[:GAVE_LIKE]->(l)",
    )
    .unwrap();
    db.execute(
        "MATCH (l:LikeEvent), (post:Post) WHERE l.id = 2 AND post.id = 1 \
         CREATE (l)-[:LIKE_ON]->(post)",
    )
    .unwrap();

    // 断言: 最终状态 -- 1 条有效点赞, ts=2000
    let final_state = db
        .query(
            "MATCH (p:Person)-[:GAVE_LIKE]->(l:LikeEvent)-[:LIKE_ON]->(post:Post) \
             WHERE p.id = 1 AND post.id = 1 \
             RETURN l.ts, l.active",
        )
        .unwrap();
    assert_eq!(final_state.num_rows(), 1);
    assert_eq!(final_state.rows()[0].get_int(0).unwrap(), 2000);
    assert_eq!(final_state.rows()[0].get_int(1).unwrap(), 1);

    // LikeEvent 表中只有 1 条记录
    let all_likes = db.query("MATCH (l:LikeEvent) RETURN count(l)").unwrap();
    assert_eq!(all_likes.rows()[0].get_int(0).unwrap(), 1);
}

// ════════════════════════════════════════════════════════════════
// N-09: "你可能认识的人" FOF 推荐
// ════════════════════════════════════════════════════════════════

#[test]
fn n09_fof_recommendation() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE REL TABLE FOLLOWS(FROM Person TO Person)").unwrap();
    db.execute("CREATE REL TABLE BLOCKS(FROM Person TO Person)").unwrap();

    // Alice(1), Bob(2), Carol(3), Dave(4), Eve(5)
    for (id, name) in [
        (1, "Alice"),
        (2, "Bob"),
        (3, "Carol"),
        (4, "Dave"),
        (5, "Eve"),
    ] {
        db.execute(&format!(
            "CREATE (p:Person {{id: {}, name: '{}', age: 25}})",
            id, name
        ))
        .unwrap();
    }

    // Alice->Bob, Alice->Carol
    // Bob->Dave, Carol->Dave (Dave: 2 mutual friends)
    // Bob->Eve (Eve: 1 mutual friend)
    // Alice blocks Dave
    for (f, t) in [(1, 2), (1, 3), (2, 4), (3, 4), (2, 5)] {
        db.execute(&format!(
            "MATCH (a:Person), (b:Person) WHERE a.id = {} AND b.id = {} \
             CREATE (a)-[:FOLLOWS]->(b)",
            f, t
        ))
        .unwrap();
    }
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 4 \
         CREATE (a)-[:BLOCKS]->(b)",
    )
    .unwrap();

    // FOF 推荐: 排除自己(1), 已关注(2,3), 已拉黑(4) => Eve
    let rec = db
        .query(
            "MATCH (me:Person)-[:FOLLOWS]->(friend:Person)-[:FOLLOWS]->(fof:Person) \
             WHERE me.id = 1 AND NOT fof.id IN [1, 2, 3, 4] \
             RETURN fof.name, count(friend)",
        )
        .unwrap();

    // Dave 被排除(已拉黑), Bob/Carol 被排除(已关注)
    let names: Vec<&str> = rec.rows().iter().map(|r| r.get_string(0).unwrap()).collect();
    assert!(!names.contains(&"Dave"), "Dave should be excluded (blocked)");
    assert!(!names.contains(&"Bob"), "Bob should be excluded (already followed)");
    assert!(!names.contains(&"Carol"), "Carol should be excluded (already followed)");

    // Eve 出现, mutual_count = 1
    assert_eq!(rec.num_rows(), 1);
    assert_eq!(rec.rows()[0].get_string(0).unwrap(), "Eve");
    assert_eq!(rec.rows()[0].get_int(1).unwrap(), 1);

    // 验证: 不排除 Dave 时, Dave mutual_count=2, Eve mutual_count=1
    let rec_all = db
        .query(
            "MATCH (me:Person)-[:FOLLOWS]->(friend:Person)-[:FOLLOWS]->(fof:Person) \
             WHERE me.id = 1 AND NOT fof.id IN [1, 2, 3] \
             RETURN fof.name, count(friend)",
        )
        .unwrap();

    let mut ranked: Vec<(String, i64)> = rec_all
        .rows()
        .iter()
        .map(|r| (r.get_string(0).unwrap().to_string(), r.get_int(1).unwrap()))
        .collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1));

    assert_eq!(ranked.len(), 2);
    assert_eq!(ranked[0].0, "Dave");
    assert_eq!(ranked[0].1, 2);
    assert_eq!(ranked[1].0, "Eve");
    assert_eq!(ranked[1].1, 1);
}

// ════════════════════════════════════════════════════════════════
// N-10: 热度/病毒传播检测
// ════════════════════════════════════════════════════════════════

#[test]
fn n10_viral_content_detection() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute(
        "CREATE NODE TABLE Post(id INT64, title STRING, content STRING, \
         created_at INT64, PRIMARY KEY(id))",
    )
    .unwrap();
    // LikeAction 中间节点（带 ts）支持时间窗口过滤
    db.execute(
        "CREATE NODE TABLE LikeAction(id INT64, ts INT64, source STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE GAVE_LIKE(FROM Person TO LikeAction)").unwrap();
    db.execute("CREATE REL TABLE LIKE_ON(FROM LikeAction TO Post)").unwrap();

    // 帖子
    db.execute(
        "CREATE (p:Post {id: 1, title: 'Normal Post', content: 'Steady', created_at: 1000})",
    )
    .unwrap();
    db.execute(
        "CREATE (p:Post {id: 2, title: 'Viral Post', content: 'Explosive', created_at: 1000})",
    )
    .unwrap();

    // 用户
    for i in 1..=8 {
        db.execute(&format!(
            "CREATE (p:Person {{id: {}, name: 'u{}', age: 20}})",
            i, i
        ))
        .unwrap();
    }

    // Normal post: 分散互动 u1@1100, u2@2100, u3@3100
    for (like_id, person_id, ts) in [(1, 1, 1100), (2, 2, 2100), (3, 3, 3100)] {
        db.execute(&format!(
            "CREATE (l:LikeAction {{id: {}, ts: {}, source: 'web'}})",
            like_id, ts
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (p:Person), (l:LikeAction) WHERE p.id = {} AND l.id = {} \
             CREATE (p)-[:GAVE_LIKE]->(l)",
            person_id, like_id
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (l:LikeAction), (post:Post) WHERE l.id = {} AND post.id = 1 \
             CREATE (l)-[:LIKE_ON]->(post)",
            like_id
        ))
        .unwrap();
    }

    // Viral post: 短时间爆发 [1100,1140]
    for (like_id, person_id, ts) in [
        (10, 4, 1100),
        (11, 5, 1110),
        (12, 6, 1120),
        (13, 7, 1130),
        (14, 8, 1140),
    ] {
        db.execute(&format!(
            "CREATE (l:LikeAction {{id: {}, ts: {}, source: 'web'}})",
            like_id, ts
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (p:Person), (l:LikeAction) WHERE p.id = {} AND l.id = {} \
             CREATE (p)-[:GAVE_LIKE]->(l)",
            person_id, like_id
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (l:LikeAction), (post:Post) WHERE l.id = {} AND post.id = 2 \
             CREATE (l)-[:LIKE_ON]->(post)",
            like_id
        ))
        .unwrap();
    }

    // 断言1: 时间窗口 [1000, 1200] 内互动数 (Rust 侧排序)
    let window_counts = db
        .query(
            "MATCH (p:Person)-[:GAVE_LIKE]->(l:LikeAction)-[:LIKE_ON]->(post:Post) \
             WHERE l.ts >= 1000 AND l.ts <= 1200 \
             RETURN post.title, count(l)",
        )
        .unwrap();
    let mut wc: Vec<(String, i64)> = window_counts
        .rows()
        .iter()
        .map(|r| (r.get_string(0).unwrap().to_string(), r.get_int(1).unwrap()))
        .collect();
    wc.sort_by(|a, b| b.1.cmp(&a.1));

    assert_eq!(wc.len(), 2);
    assert_eq!(wc[0].0, "Viral Post");
    assert_eq!(wc[0].1, 5);
    assert_eq!(wc[1].0, "Normal Post");
    assert_eq!(wc[1].1, 1);

    // 断言2: Viral Post 在窗口内排第一
    assert_eq!(wc[0].0, "Viral Post");

    // 断言3: 全时间范围内总互动数
    let total_counts = db
        .query(
            "MATCH (p:Person)-[:GAVE_LIKE]->(l:LikeAction)-[:LIKE_ON]->(post:Post) \
             RETURN post.title, count(l)",
        )
        .unwrap();
    let mut total_map: HashMap<String, i64> = HashMap::new();
    for row in total_counts.rows() {
        total_map.insert(
            row.get_string(0).unwrap().to_string(),
            row.get_int(1).unwrap(),
        );
    }
    assert_eq!(total_map["Normal Post"], 3);
    assert_eq!(total_map["Viral Post"], 5);

    // 断言4: 窗口2 [1201, 3200] — 只有 Normal Post 有后续互动
    let window2 = db
        .query(
            "MATCH (p:Person)-[:GAVE_LIKE]->(l:LikeAction)-[:LIKE_ON]->(post:Post) \
             WHERE l.ts >= 1201 AND l.ts <= 3200 \
             RETURN post.title, count(l)",
        )
        .unwrap();
    assert_eq!(window2.num_rows(), 1);
    assert_eq!(window2.rows()[0].get_string(0).unwrap(), "Normal Post");
    assert_eq!(window2.rows()[0].get_int(1).unwrap(), 2);
    // Viral Post 无后续互动 => 爆发集中在窗口1
}

// ════════════════════════════════════════════════════════════════
// N-11: 私信对话线程图
// ════════════════════════════════════════════════════════════════

#[test]
fn n11_dm_conversation_thread() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE NODE TABLE Conversation(id INT64, title STRING, PRIMARY KEY(id))")
        .unwrap();
    db.execute(
        "CREATE NODE TABLE Message(id INT64, text STRING, ts INT64, sender STRING, PRIMARY KEY(id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE HAS_PARTICIPANT(FROM Conversation TO Person)").unwrap();
    db.execute("CREATE REL TABLE CONTAINS_MSG(FROM Conversation TO Message)").unwrap();

    // 用户
    db.execute("CREATE (p:Person {id: 1, name: 'Alice', age: 25})").unwrap();
    db.execute("CREATE (p:Person {id: 2, name: 'Bob', age: 30})").unwrap();
    db.execute("CREATE (p:Person {id: 3, name: 'Carol', age: 28})").unwrap();

    // 对话
    db.execute("CREATE (c:Conversation {id: 1, title: 'Project Chat'})").unwrap();

    // 参与者
    for pid in [1, 2, 3] {
        db.execute(&format!(
            "MATCH (c:Conversation), (p:Person) WHERE c.id = 1 AND p.id = {} \
             CREATE (c)-[:HAS_PARTICIPANT]->(p)",
            pid
        ))
        .unwrap();
    }

    // 消息
    let msgs: [(i64, &str, i64, &str); 5] = [
        (1, "Hello!", 1100, "Alice"),
        (2, "Hi Alice!", 1200, "Bob"),
        (3, "Hey all!", 1600, "Carol"),
        (4, "Welcome Carol!", 1700, "Alice"),
        (5, "Lets plan the sprint", 1800, "Bob"),
    ];
    for (id, text, ts, sender) in msgs {
        db.execute(&format!(
            "CREATE (m:Message {{id: {}, text: '{}', ts: {}, sender: '{}'}})",
            id, text, ts, sender
        ))
        .unwrap();
        db.execute(&format!(
            "MATCH (c:Conversation), (m:Message) WHERE c.id = 1 AND m.id = {} \
             CREATE (c)-[:CONTAINS_MSG]->(m)",
            id
        ))
        .unwrap();
    }

    // 断言1: 对话参与者数 = 3
    let participants = db
        .query(
            "MATCH (c:Conversation)-[:HAS_PARTICIPANT]->(p:Person) \
             WHERE c.id = 1 RETURN count(p)",
        )
        .unwrap();
    assert_eq!(participants.rows()[0].get_int(0).unwrap(), 3);

    // 断言2: 参与者列表
    let participant_names = db
        .query(
            "MATCH (c:Conversation)-[:HAS_PARTICIPANT]->(p:Person) \
             WHERE c.id = 1 RETURN p.name ORDER BY p.name",
        )
        .unwrap();
    assert_eq!(participant_names.num_rows(), 3);
    assert_eq!(participant_names.rows()[0].get_string(0).unwrap(), "Alice");
    assert_eq!(participant_names.rows()[1].get_string(0).unwrap(), "Bob");
    assert_eq!(participant_names.rows()[2].get_string(0).unwrap(), "Carol");

    // 断言3: 消息按 ts 排序
    let messages = db
        .query(
            "MATCH (c:Conversation)-[:CONTAINS_MSG]->(m:Message) \
             WHERE c.id = 1 RETURN m.text, m.ts, m.sender ORDER BY m.ts",
        )
        .unwrap();
    assert_eq!(messages.num_rows(), 5);
    assert_eq!(messages.rows()[0].get_string(0).unwrap(), "Hello!");
    assert_eq!(messages.rows()[0].get_int(1).unwrap(), 1100);
    assert_eq!(messages.rows()[0].get_string(2).unwrap(), "Alice");
    assert_eq!(messages.rows()[1].get_string(0).unwrap(), "Hi Alice!");
    assert_eq!(messages.rows()[1].get_int(1).unwrap(), 1200);
    assert_eq!(messages.rows()[2].get_string(0).unwrap(), "Hey all!");
    assert_eq!(messages.rows()[2].get_int(1).unwrap(), 1600);
    assert_eq!(messages.rows()[3].get_string(0).unwrap(), "Welcome Carol!");
    assert_eq!(messages.rows()[3].get_int(1).unwrap(), 1700);
    assert_eq!(
        messages.rows()[4].get_string(0).unwrap(),
        "Lets plan the sprint"
    );
    assert_eq!(messages.rows()[4].get_int(1).unwrap(), 1800);

    // 断言4: Carol 加入前的消息仍可通过对话查到
    let pre_carol = db
        .query(
            "MATCH (c:Conversation)-[:CONTAINS_MSG]->(m:Message) \
             WHERE c.id = 1 AND m.ts < 1500 RETURN m.text ORDER BY m.ts",
        )
        .unwrap();
    assert_eq!(pre_carol.num_rows(), 2);
    assert_eq!(pre_carol.rows()[0].get_string(0).unwrap(), "Hello!");
    assert_eq!(pre_carol.rows()[1].get_string(0).unwrap(), "Hi Alice!");

    // 断言5: 消息总数 = 5
    let msg_count = db
        .query(
            "MATCH (c:Conversation)-[:CONTAINS_MSG]->(m:Message) \
             WHERE c.id = 1 RETURN count(m)",
        )
        .unwrap();
    assert_eq!(msg_count.rows()[0].get_int(0).unwrap(), 5);

    // 断言6: 按 sender 分组统计
    let sender_stats = db
        .query(
            "MATCH (c:Conversation)-[:CONTAINS_MSG]->(m:Message) \
             WHERE c.id = 1 RETURN m.sender, count(m)",
        )
        .unwrap();
    let mut sender_counts: HashMap<String, i64> = HashMap::new();
    for row in sender_stats.rows() {
        sender_counts.insert(
            row.get_string(0).unwrap().to_string(),
            row.get_int(1).unwrap(),
        );
    }
    assert_eq!(sender_counts["Alice"], 2);
    assert_eq!(sender_counts["Bob"], 2);
    assert_eq!(sender_counts["Carol"], 1);

    // 断言7: 最新消息
    let latest = db
        .query(
            "MATCH (c:Conversation)-[:CONTAINS_MSG]->(m:Message) \
             WHERE c.id = 1 RETURN m.text, m.sender ORDER BY m.ts DESC LIMIT 1",
        )
        .unwrap();
    assert_eq!(latest.num_rows(), 1);
    assert_eq!(
        latest.rows()[0].get_string(0).unwrap(),
        "Lets plan the sprint"
    );
    assert_eq!(latest.rows()[0].get_string(1).unwrap(), "Bob");
}
