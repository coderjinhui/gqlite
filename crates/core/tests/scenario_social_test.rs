/// 社交网络场景端到端测试
///
/// 场景：一个社交平台，有用户(Person)、城市(City)、帖子(Post)
/// 关系：FOLLOWS、LIVES_IN、POSTED、LIKED
use gqlite_core::Database;

// ── 辅助函数 ────────────────────────────────────────────────

/// 创建完整 schema：4 个节点表 + 4 个关系表
fn create_schema(db: &Database) {
    // 节点表
    db.execute(
        "CREATE NODE TABLE Person (id INT64, name STRING, age INT64, bio STRING, PRIMARY KEY (id))",
    )
    .unwrap();
    db.execute("CREATE NODE TABLE City (id INT64, name STRING, country STRING, PRIMARY KEY (id))")
        .unwrap();
    db.execute("CREATE NODE TABLE Post (id INT64, title STRING, content STRING, PRIMARY KEY (id))")
        .unwrap();
    db.execute("CREATE NODE TABLE Tag (id INT64, name STRING, PRIMARY KEY (id))").unwrap();

    // 关系表
    db.execute("CREATE REL TABLE FOLLOWS (FROM Person TO Person)").unwrap();
    db.execute("CREATE REL TABLE LIVES_IN (FROM Person TO City)").unwrap();
    db.execute("CREATE REL TABLE POSTED (FROM Person TO Post)").unwrap();
    db.execute("CREATE REL TABLE LIKED (FROM Person TO Post)").unwrap();
    db.execute("CREATE REL TABLE HAS_TAG (FROM Post TO Tag)").unwrap();
}

/// 插入社交网络数据：20 用户、5 城市、12 帖子、5 标签，建立关系网络
fn insert_data(db: &Database) {
    // 5 个城市
    db.execute("CREATE (c:City {id: 1, name: 'Beijing', country: 'China'})").unwrap();
    db.execute("CREATE (c:City {id: 2, name: 'Shanghai', country: 'China'})").unwrap();
    db.execute("CREATE (c:City {id: 3, name: 'Tokyo', country: 'Japan'})").unwrap();
    db.execute("CREATE (c:City {id: 4, name: 'NewYork', country: 'USA'})").unwrap();
    db.execute("CREATE (c:City {id: 5, name: 'London', country: 'UK'})").unwrap();

    // 20 个用户
    db.execute("CREATE (p:Person {id: 1, name: 'Alice', age: 28, bio: 'Software engineer'})")
        .unwrap();
    db.execute("CREATE (p:Person {id: 2, name: 'Bob', age: 32, bio: 'Data scientist'})").unwrap();
    db.execute("CREATE (p:Person {id: 3, name: 'Charlie', age: 25, bio: 'Frontend developer'})")
        .unwrap();
    db.execute("CREATE (p:Person {id: 4, name: 'Diana', age: 30, bio: 'Product manager'})")
        .unwrap();
    db.execute("CREATE (p:Person {id: 5, name: 'Eve', age: 27, bio: 'Backend engineer'})").unwrap();
    db.execute("CREATE (p:Person {id: 6, name: 'Frank', age: 35, bio: 'DevOps engineer'})")
        .unwrap();
    db.execute("CREATE (p:Person {id: 7, name: 'Grace', age: 29, bio: 'UX designer'})").unwrap();
    db.execute("CREATE (p:Person {id: 8, name: 'Hank', age: 31, bio: 'System architect'})")
        .unwrap();
    db.execute("CREATE (p:Person {id: 9, name: 'Ivy', age: 26, bio: 'Mobile developer'})").unwrap();
    db.execute("CREATE (p:Person {id: 10, name: 'Jack', age: 33, bio: 'Tech lead'})").unwrap();
    db.execute("CREATE (p:Person {id: 11, name: 'Kate', age: 24, bio: 'Junior developer'})")
        .unwrap();
    db.execute("CREATE (p:Person {id: 12, name: 'Leo', age: 36, bio: 'CTO'})").unwrap();
    db.execute("CREATE (p:Person {id: 13, name: 'Mia', age: 28, bio: 'Data engineer'})").unwrap();
    db.execute("CREATE (p:Person {id: 14, name: 'Noah', age: 30, bio: 'ML engineer'})").unwrap();
    db.execute("CREATE (p:Person {id: 15, name: 'Olivia', age: 27, bio: 'QA engineer'})").unwrap();
    db.execute("CREATE (p:Person {id: 16, name: 'Paul', age: 34, bio: 'Security engineer'})")
        .unwrap();
    db.execute("CREATE (p:Person {id: 17, name: 'Quinn', age: 29, bio: 'Cloud engineer'})")
        .unwrap();
    db.execute("CREATE (p:Person {id: 18, name: 'Ruby', age: 26, bio: 'Full stack developer'})")
        .unwrap();
    db.execute("CREATE (p:Person {id: 19, name: 'Sam', age: 31, bio: 'Database engineer'})")
        .unwrap();
    db.execute("CREATE (p:Person {id: 20, name: 'Tina', age: 23, bio: 'Intern developer'})")
        .unwrap();

    // 12 个帖子
    db.execute("CREATE (p:Post {id: 1, title: 'Rust Tips', content: 'Ownership is key'})").unwrap();
    db.execute("CREATE (p:Post {id: 2, title: 'Graph DB Intro', content: 'Nodes and edges'})")
        .unwrap();
    db.execute("CREATE (p:Post {id: 3, title: 'ML Basics', content: 'Neural networks'})").unwrap();
    db.execute("CREATE (p:Post {id: 4, title: 'DevOps Guide', content: 'CI/CD pipeline'})")
        .unwrap();
    db.execute("CREATE (p:Post {id: 5, title: 'React Hooks', content: 'useState and useEffect'})")
        .unwrap();
    db.execute("CREATE (p:Post {id: 6, title: 'System Design', content: 'Scalability patterns'})")
        .unwrap();
    db.execute("CREATE (p:Post {id: 7, title: 'Python Tips', content: 'List comprehension'})")
        .unwrap();
    db.execute("CREATE (p:Post {id: 8, title: 'Docker 101', content: 'Container basics'})")
        .unwrap();
    db.execute("CREATE (p:Post {id: 9, title: 'Kubernetes', content: 'Pod scheduling'})").unwrap();
    db.execute(
        "CREATE (p:Post {id: 10, title: 'Go Concurrency', content: 'Goroutines and channels'})",
    )
    .unwrap();
    db.execute("CREATE (p:Post {id: 11, title: 'SQL vs NoSQL', content: 'When to use what'})")
        .unwrap();
    db.execute("CREATE (p:Post {id: 12, title: 'Rust Async', content: 'Tokio runtime'})").unwrap();

    // 5 个标签
    db.execute("CREATE (t:Tag {id: 1, name: 'Rust'})").unwrap();
    db.execute("CREATE (t:Tag {id: 2, name: 'Graph'})").unwrap();
    db.execute("CREATE (t:Tag {id: 3, name: 'AI'})").unwrap();
    db.execute("CREATE (t:Tag {id: 4, name: 'Frontend'})").unwrap();
    db.execute("CREATE (t:Tag {id: 5, name: 'Infra'})").unwrap();

    // LIVES_IN 关系（每个用户住一个城市）
    // Beijing: Alice, Bob, Charlie, Diana (4 人)
    for pid in [1, 2, 3, 4] {
        db.execute(&format!(
            "MATCH (p:Person), (c:City) WHERE p.id = {} AND c.id = 1 CREATE (p)-[:LIVES_IN]->(c)",
            pid
        ))
        .unwrap();
    }
    // Shanghai: Eve, Frank, Grace (3 人)
    for pid in [5, 6, 7] {
        db.execute(&format!(
            "MATCH (p:Person), (c:City) WHERE p.id = {} AND c.id = 2 CREATE (p)-[:LIVES_IN]->(c)",
            pid
        ))
        .unwrap();
    }
    // Tokyo: Hank, Ivy, Jack, Kate (4 人)
    for pid in [8, 9, 10, 11] {
        db.execute(&format!(
            "MATCH (p:Person), (c:City) WHERE p.id = {} AND c.id = 3 CREATE (p)-[:LIVES_IN]->(c)",
            pid
        ))
        .unwrap();
    }
    // NewYork: Leo, Mia, Noah, Olivia, Paul (5 人)
    for pid in [12, 13, 14, 15, 16] {
        db.execute(&format!(
            "MATCH (p:Person), (c:City) WHERE p.id = {} AND c.id = 4 CREATE (p)-[:LIVES_IN]->(c)",
            pid
        ))
        .unwrap();
    }
    // London: Quinn, Ruby, Sam, Tina (4 人)
    for pid in [17, 18, 19, 20] {
        db.execute(&format!(
            "MATCH (p:Person), (c:City) WHERE p.id = {} AND c.id = 5 CREATE (p)-[:LIVES_IN]->(c)",
            pid
        ))
        .unwrap();
    }

    // FOLLOWS 关系（社交网络）— 共 34 条
    let follows = [
        (1, 2),
        (1, 3),
        (1, 5), // Alice follows Bob, Charlie, Eve
        (2, 1),
        (2, 4),
        (2, 6), // Bob follows Alice, Diana, Frank
        (3, 1),
        (3, 2),
        (3, 7), // Charlie follows Alice, Bob, Grace
        (4, 1),
        (4, 5),
        (4, 8), // Diana follows Alice, Eve, Hank
        (5, 1),
        (5, 2),
        (5, 3), // Eve follows Alice, Bob, Charlie
        (6, 2),
        (6, 10), // Frank follows Bob, Jack
        (7, 3),
        (7, 9), // Grace follows Charlie, Ivy
        (8, 4),
        (8, 10), // Hank follows Diana, Jack
        (9, 7),
        (9, 11), // Ivy follows Grace, Kate
        (10, 1),
        (10, 12), // Jack follows Alice, Leo
        (11, 9),  // Kate follows Ivy
        (12, 10),
        (12, 14), // Leo follows Jack, Noah
        (13, 14),
        (13, 2), // Mia follows Noah, Bob
        (14, 12),
        (14, 13), // Noah follows Leo, Mia
        (15, 14), // Olivia follows Noah
        (16, 12), // Paul follows Leo
    ];
    for (from, to) in follows {
        db.execute(&format!(
            "MATCH (a:Person), (b:Person) \
             WHERE a.id = {} AND b.id = {} \
             CREATE (a)-[:FOLLOWS]->(b)",
            from, to
        ))
        .unwrap();
    }

    // POSTED 关系 — 共 12 条
    let posted = [
        (1, 1),
        (1, 12), // Alice posted "Rust Tips" and "Rust Async"
        (2, 2),  // Bob posted "Graph DB Intro"
        (3, 5),  // Charlie posted "React Hooks"
        (5, 3),  // Eve posted "ML Basics"
        (6, 4),
        (6, 8),   // Frank posted "DevOps Guide" and "Docker 101"
        (8, 6),   // Hank posted "System Design"
        (10, 10), // Jack posted "Go Concurrency"
        (12, 9),  // Leo posted "Kubernetes"
        (14, 7),  // Noah posted "Python Tips"
        (19, 11), // Sam posted "SQL vs NoSQL"
    ];
    for (person_id, post_id) in posted {
        db.execute(&format!(
            "MATCH (p:Person), (post:Post) \
             WHERE p.id = {} AND post.id = {} \
             CREATE (p)-[:POSTED]->(post)",
            person_id, post_id
        ))
        .unwrap();
    }

    // LIKED 关系 — 共 18 条
    let liked = [
        (2, 1),
        (3, 1),
        (5, 1),
        (4, 1), // Rust Tips: 4 likes
        (1, 2),
        (5, 2),
        (3, 2), // Graph DB Intro: 3 likes
        (1, 3),
        (2, 3), // ML Basics: 2 likes
        (7, 5),
        (9, 5), // React Hooks: 2 likes
        (8, 6),
        (10, 6),
        (12, 6), // System Design: 3 likes
        (1, 12),
        (2, 12),
        (5, 12),
        (3, 12), // Rust Async: 4 likes
    ];
    for (person_id, post_id) in liked {
        db.execute(&format!(
            "MATCH (p:Person), (post:Post) \
             WHERE p.id = {} AND post.id = {} \
             CREATE (p)-[:LIKED]->(post)",
            person_id, post_id
        ))
        .unwrap();
    }

    // HAS_TAG 关系
    let tagged = [
        (1, 1),  // Rust Tips -> Rust
        (12, 1), // Rust Async -> Rust
        (2, 2),  // Graph DB Intro -> Graph
        (11, 2), // SQL vs NoSQL -> Graph
        (3, 3),  // ML Basics -> AI
        (7, 3),  // Python Tips -> AI
        (5, 4),  // React Hooks -> Frontend
        (4, 5),  // DevOps Guide -> Infra
        (6, 5),  // System Design -> Infra
        (8, 5),  // Docker 101 -> Infra
        (9, 5),  // Kubernetes -> Infra
        (10, 5), // Go Concurrency -> Infra
    ];
    for (post_id, tag_id) in tagged {
        db.execute(&format!(
            "MATCH (post:Post), (tag:Tag) \
             WHERE post.id = {} AND tag.id = {} \
             CREATE (post)-[:HAS_TAG]->(tag)",
            post_id, tag_id
        ))
        .unwrap();
    }
}

/// 创建 schema + 插入数据的一体化辅助函数
fn setup_social_db() -> Database {
    let db = Database::in_memory();
    create_schema(&db);
    insert_data(&db);
    db
}

// ── 测试用例 ────────────────────────────────────────────────

#[test]
fn setup_social_schema() {
    let db = Database::in_memory();
    create_schema(&db);

    // 验证 4 个节点表可被查询
    let r1 = db.query("MATCH (p:Person) RETURN p.id").unwrap();
    assert_eq!(r1.num_rows(), 0);
    let r2 = db.query("MATCH (c:City) RETURN c.id").unwrap();
    assert_eq!(r2.num_rows(), 0);
    let r3 = db.query("MATCH (p:Post) RETURN p.id").unwrap();
    assert_eq!(r3.num_rows(), 0);
    let r4 = db.query("MATCH (t:Tag) RETURN t.id").unwrap();
    assert_eq!(r4.num_rows(), 0);

    // 验证关系表存在：创建节点后能建关系
    db.execute("CREATE (p:Person {id: 1, name: 'Test', age: 20, bio: 'test'})").unwrap();
    db.execute("CREATE (p:Person {id: 2, name: 'Test2', age: 21, bio: 'test2'})").unwrap();
    db.execute("CREATE (c:City {id: 1, name: 'TestCity', country: 'TC'})").unwrap();
    db.execute("CREATE (p:Post {id: 1, title: 'T', content: 'C'})").unwrap();

    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:FOLLOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (p:Person), (c:City) WHERE p.id = 1 AND c.id = 1 CREATE (p)-[:LIVES_IN]->(c)",
    )
    .unwrap();
    db.execute(
        "MATCH (p:Person), (post:Post) \
         WHERE p.id = 1 AND post.id = 1 CREATE (p)-[:POSTED]->(post)",
    )
    .unwrap();
    db.execute(
        "MATCH (p:Person), (post:Post) \
         WHERE p.id = 2 AND post.id = 1 CREATE (p)-[:LIKED]->(post)",
    )
    .unwrap();

    // 验证关系均已建立
    let follows = db.query("MATCH (a:Person)-[:FOLLOWS]->(b:Person) RETURN a.name").unwrap();
    assert_eq!(follows.num_rows(), 1);
    let lives = db.query("MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN p.name").unwrap();
    assert_eq!(lives.num_rows(), 1);
    let posted = db.query("MATCH (p:Person)-[:POSTED]->(post:Post) RETURN p.name").unwrap();
    assert_eq!(posted.num_rows(), 1);
    let liked = db.query("MATCH (p:Person)-[:LIKED]->(post:Post) RETURN p.name").unwrap();
    assert_eq!(liked.num_rows(), 1);
}

#[test]
fn insert_social_data() {
    let db = setup_social_db();

    // 验证节点数量
    let persons = db.query("MATCH (p:Person) RETURN p.id").unwrap();
    assert_eq!(persons.num_rows(), 20);

    let cities = db.query("MATCH (c:City) RETURN c.id").unwrap();
    assert_eq!(cities.num_rows(), 5);

    let posts = db.query("MATCH (p:Post) RETURN p.id").unwrap();
    assert_eq!(posts.num_rows(), 12);

    // 验证关系数量
    let follows = db.query("MATCH (a:Person)-[:FOLLOWS]->(b:Person) RETURN a.id").unwrap();
    assert_eq!(follows.num_rows(), 34); // 34 条 FOLLOWS 关系

    let lives = db.query("MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN p.id").unwrap();
    assert_eq!(lives.num_rows(), 20); // 每个用户住一个城市

    let posted = db.query("MATCH (p:Person)-[:POSTED]->(post:Post) RETURN p.id").unwrap();
    assert_eq!(posted.num_rows(), 12); // 12 条 POSTED 关系

    let liked = db.query("MATCH (p:Person)-[:LIKED]->(post:Post) RETURN p.id").unwrap();
    assert_eq!(liked.num_rows(), 18); // 18 条 LIKED 关系

    // 验证具体数据
    let alice = db.query("MATCH (p:Person) WHERE p.id = 1 RETURN p.name, p.age").unwrap();
    assert_eq!(alice.num_rows(), 1);
    assert_eq!(alice.rows()[0].get_string(0).unwrap(), "Alice");
    assert_eq!(alice.rows()[0].get_int(1).unwrap(), 28);
}

#[test]
fn find_friends_of_friends() {
    let db = setup_social_db();

    // Alice (id=1) 的直接朋友：Bob(2), Charlie(3), Eve(5)
    // Bob 关注：Alice(1), Diana(4), Frank(6)
    // Charlie 关注：Alice(1), Bob(2), Grace(7)
    // Eve 关注：Alice(1), Bob(2), Charlie(3)
    // 二跳节点（去重）：Alice(1), Diana(4), Frank(6), Bob(2), Grace(7), Charlie(3)
    // 排除自己(1)和直接朋友(2,3,5) → Diana(4), Frank(6), Grace(7)
    //
    // gqlite 不支持 NOT (me)-[:FOLLOWS]->(fof) 模式否定语法，
    // 用 NOT fof.id IN [1, 2, 3, 5] 替代（排除自己和直接朋友的 id）
    let result = db
        .query(
            "MATCH (me:Person)-[:FOLLOWS]->(friend:Person)-[:FOLLOWS]->(fof:Person) \
             WHERE me.id = 1 \
             AND NOT fof.id IN [1, 2, 3, 5] \
             RETURN DISTINCT fof.name ORDER BY fof.name",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 3);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "Diana");
    assert_eq!(result.rows()[1].get_string(0).unwrap(), "Frank");
    assert_eq!(result.rows()[2].get_string(0).unwrap(), "Grace");
}

#[test]
fn mutual_friends() {
    let db = setup_social_db();

    // 找 Alice(1) 和 Diana(4) 的共同关注对象
    // Alice 关注：Bob(2), Charlie(3), Eve(5)
    // Diana 关注：Alice(1), Eve(5), Hank(8)
    // 共同关注：Eve(5)
    //
    // 使用两条独立的 MATCH 路径 + WHERE 约束共享变量 m
    // 注意：gqlite 的多模式 MATCH 可能产生笛卡尔积，
    // 使用 MATCH 链式路径避免此问题
    let result = db
        .query(
            "MATCH (a:Person)-[:FOLLOWS]->(m:Person)<-[:FOLLOWS]-(b:Person) \
             WHERE a.id = 1 AND b.id = 4 \
             RETURN m.name ORDER BY m.name",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "Eve");
}

#[test]
fn user_activity_stats() {
    let db = setup_social_db();

    // 每个发过帖的用户的帖子数
    // Alice:2, Bob:1, Charlie:1, Eve:1, Frank:2, Hank:1, Jack:1, Leo:1, Noah:1, Sam:1
    let result = db
        .query(
            "MATCH (p:Person)-[:POSTED]->(post:Post) \
             RETURN p.name, count(post)",
        )
        .unwrap();

    // 发过帖的人有 10 个
    assert_eq!(result.num_rows(), 10);

    // 收集结果并验证
    let mut post_counts: Vec<(&str, i64)> =
        result.rows().iter().map(|r| (r.get_string(0).unwrap(), r.get_int(1).unwrap())).collect();
    post_counts.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));

    // Alice 和 Frank 各发了 2 个帖子
    assert_eq!(post_counts[0].1, 2);
    assert!(post_counts[0].0 == "Alice" || post_counts[0].0 == "Frank");
    assert_eq!(post_counts[1].1, 2);
    // 其余 8 人各发 1 个帖子
    for pc in &post_counts[2..] {
        assert_eq!(pc.1, 1);
    }

    // 统计被赞数最多的帖子
    let liked_result = db
        .query(
            "MATCH (p:Person)-[:LIKED]->(post:Post) \
             RETURN post.title, count(p)",
        )
        .unwrap();

    // 收集并找最大值
    let mut like_counts: Vec<(&str, i64)> = liked_result
        .rows()
        .iter()
        .map(|r| (r.get_string(0).unwrap(), r.get_int(1).unwrap()))
        .collect();
    like_counts.sort_by(|a, b| b.1.cmp(&a.1));

    // "Rust Tips" 和 "Rust Async" 各有 4 个赞
    assert!(!like_counts.is_empty());
    assert_eq!(like_counts[0].1, 4);
    let top_title = like_counts[0].0;
    assert!(
        top_title == "Rust Tips" || top_title == "Rust Async",
        "top liked post should be Rust Tips or Rust Async, got {}",
        top_title
    );
}

#[test]
fn city_user_ranking() {
    let db = setup_social_db();

    // 每个城市的用户数排名
    // NewYork: 5, Beijing: 4, Tokyo: 4, London: 4, Shanghai: 3
    let result = db
        .query(
            "MATCH (p:Person)-[:LIVES_IN]->(c:City) \
             RETURN c.name, count(p)",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 5);

    // 收集结果到 HashMap 验证
    let mut city_counts: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for row in result.rows() {
        let city = row.get_string(0).unwrap().to_string();
        let count = row.get_int(1).unwrap();
        city_counts.insert(city, count);
    }

    assert_eq!(city_counts["NewYork"], 5);
    assert_eq!(city_counts["Beijing"], 4);
    assert_eq!(city_counts["Tokyo"], 4);
    assert_eq!(city_counts["London"], 4);
    assert_eq!(city_counts["Shanghai"], 3);

    // 验证 NewYork 用户最多
    let max_count = city_counts.values().max().unwrap();
    assert_eq!(*max_count, 5);
    let max_city: Vec<&String> =
        city_counts.iter().filter(|(_, &v)| v == 5).map(|(k, _)| k).collect();
    assert_eq!(max_city.len(), 1);
    assert_eq!(max_city[0], "NewYork");
}

#[test]
fn shortest_path_between_users() {
    let db = setup_social_db();

    // Alice(1) -> Bob(2): 直接关注，最短路径长度 = 1
    let result = db
        .query(
            "MATCH (a:Person), (b:Person), \
             p = shortestPath((a)-[:FOLLOWS*..10]->(b)) \
             WHERE a.id = 1 AND b.id = 2 \
             RETURN length(p) AS len",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 1);

    // Alice(1) -> Diana(4): Alice->Bob(2)->Diana(4), 2 跳
    let result2 = db
        .query(
            "MATCH (a:Person), (b:Person), \
             p = shortestPath((a)-[:FOLLOWS*..10]->(b)) \
             WHERE a.id = 1 AND b.id = 4 \
             RETURN length(p) AS len",
        )
        .unwrap();
    assert_eq!(result2.num_rows(), 1);
    assert_eq!(result2.rows()[0].get_int(0).unwrap(), 2);

    // Alice(1) -> Jack(10):
    // 路径: Alice(1)->Bob(2)->Frank(6)->Jack? Frank 不关注 Jack 直接？
    //   Frank(6) follows Bob(2) and Jack(10)! 所以 Frank->Jack 是 1 跳
    //   但路径是 Alice->Bob->Frank->Jack? 不对，Frank follows Jack 但 Bob follows Frank
    //   Bob(2) follows Alice(1), Diana(4), Frank(6)
    //   Frank(6) follows Bob(2), Jack(10) - 是的！
    //   但 Alice->Bob 是跳 1，Bob->Frank 不行：Bob follows Frank(6) ✓
    //   Alice->Bob->Frank->Jack? 不行因为 Bob follows Frank 不对。
    //   Bob follows: Alice(1), Diana(4), Frank(6)。是的 Bob 关注 Frank！
    //   所以 Alice(1)->Bob(2)->Frank(6)->Jack? 但这需要 Frank follows Jack:
    //   Frank(6) follows: Bob(2), Jack(10)。是的！
    //   所以 Alice->Bob->Frank->Jack = 3 跳
    //
    // 但也有更短路径:
    //   Alice(1)->Eve(5)->? Eve follows Alice(1), Bob(2), Charlie(3) - 不到 Jack
    //   Alice(1)->Charlie(3)->? Charlie follows Alice(1), Bob(2), Grace(7) - 不到 Jack
    // 最短为 3 跳
    let result3 = db
        .query(
            "MATCH (a:Person), (b:Person), \
             p = shortestPath((a)-[:FOLLOWS*..10]->(b)) \
             WHERE a.id = 1 AND b.id = 10 \
             RETURN length(p) AS len",
        )
        .unwrap();
    assert_eq!(result3.num_rows(), 1);
    assert_eq!(result3.rows()[0].get_int(0).unwrap(), 3);
}

#[test]
fn pagerank_influence() {
    let db = setup_social_db();

    // 在 FOLLOWS 关系上运行 PageRank
    let result = db.query("CALL pagerank('FOLLOWS') YIELD node_id, score").unwrap();

    // 应该有 20 个节点的 PageRank 结果
    assert_eq!(result.num_rows(), 20);

    // 找到得分最高的节点
    let rows = result.rows();
    let mut max_score = 0.0f64;
    let mut max_node_id = 0i64;
    for row in rows {
        let nid = row.get_int(0).unwrap();
        let score = row.get_float(1).unwrap();
        if score > max_score {
            max_score = score;
            max_node_id = nid;
        }
    }

    // Alice(id=1) 和 Bob(id=2) 各被 5 人关注，PageRank 最高的应是其中之一
    // node_id 是 offset（Person 表中的插入顺序），Alice=offset 0, Bob=offset 1
    assert!(
        max_node_id == 0 || max_node_id == 1,
        "Alice (offset 0) or Bob (offset 1) should have highest PageRank, got offset {}",
        max_node_id
    );

    // 所有得分之和应约等于 1.0
    let total: f64 = rows.iter().map(|r| r.get_float(1).unwrap()).sum();
    assert!((total - 1.0).abs() < 0.05, "PageRank scores should sum to ~1.0, got {}", total);
}

#[test]
fn degree_centrality_popular() {
    let db = setup_social_db();

    let result =
        db.query("CALL degree_centrality('FOLLOWS') YIELD node_id, in_degree, out_degree").unwrap();

    assert_eq!(result.num_rows(), 20);

    // 找入度最高的节点
    let rows = result.rows();
    let mut max_in_degree = 0i64;
    let mut max_in_nodes = Vec::new();
    for row in rows {
        let nid = row.get_int(0).unwrap();
        let in_deg = row.get_int(1).unwrap();
        if in_deg > max_in_degree {
            max_in_degree = in_deg;
            max_in_nodes = vec![nid];
        } else if in_deg == max_in_degree {
            max_in_nodes.push(nid);
        }
    }

    // Alice(id=1, offset=0) 和 Bob(id=2, offset=1) 各被 5 人关注
    assert_eq!(max_in_degree, 5);
    assert!(max_in_nodes.contains(&0), "Alice (offset 0) should be among top in-degree nodes");
    assert!(max_in_nodes.contains(&1), "Bob (offset 1) should be among top in-degree nodes");

    // 验证 Alice(offset 0) 的出度 = 3（关注 Bob, Charlie, Eve）
    let alice_row = &rows[0];
    assert_eq!(alice_row.get_int(0).unwrap(), 0); // node_id = offset 0
    assert_eq!(alice_row.get_int(2).unwrap(), 3); // out_degree = 3
}

#[test]
fn cascade_delete_user() {
    let db = setup_social_db();

    // 删除前验证 Frank(id=6) 存在且有关系
    let frank = db.query("MATCH (p:Person) WHERE p.id = 6 RETURN p.name").unwrap();
    assert_eq!(frank.num_rows(), 1);

    // Frank 发了 2 个帖子
    let frank_posts = db
        .query("MATCH (p:Person)-[:POSTED]->(post:Post) WHERE p.id = 6 RETURN post.title")
        .unwrap();
    assert_eq!(frank_posts.num_rows(), 2);

    // Frank 有 FOLLOWS 出边（关注 Bob 和 Jack）
    let frank_follows =
        db.query("MATCH (p:Person)-[:FOLLOWS]->(f:Person) WHERE p.id = 6 RETURN f.name").unwrap();
    assert_eq!(frank_follows.num_rows(), 2);

    // DETACH DELETE Frank
    db.execute("MATCH (n:Person) WHERE n.id = 6 DETACH DELETE n").unwrap();

    // Frank 不再存在
    let frank_after = db.query("MATCH (p:Person) WHERE p.id = 6 RETURN p.name").unwrap();
    assert_eq!(frank_after.num_rows(), 0);

    // Person 总数减 1
    let persons = db.query("MATCH (p:Person) RETURN p.id").unwrap();
    assert_eq!(persons.num_rows(), 19);

    // Frank 的 FOLLOWS 出边被删除
    let frank_follows_after =
        db.query("MATCH (p:Person)-[:FOLLOWS]->(f:Person) WHERE p.id = 6 RETURN f.name").unwrap();
    assert_eq!(frank_follows_after.num_rows(), 0);

    // Frank 的 POSTED 关系被删除
    let frank_posted_after = db
        .query("MATCH (p:Person)-[:POSTED]->(post:Post) WHERE p.id = 6 RETURN post.title")
        .unwrap();
    assert_eq!(frank_posted_after.num_rows(), 0);

    // Bob 关注 Frank 的入边也应被删除
    // Bob(2) follows Alice(1), Diana(4), Frank(6) -- Frank 被删，剩 Alice 和 Diana
    let bob_follows_after = db
        .query(
            "MATCH (p:Person)-[:FOLLOWS]->(f:Person) \
             WHERE p.id = 2 RETURN f.name ORDER BY f.name",
        )
        .unwrap();
    assert_eq!(bob_follows_after.num_rows(), 2);
    assert_eq!(bob_follows_after.rows()[0].get_string(0).unwrap(), "Alice");
    assert_eq!(bob_follows_after.rows()[1].get_string(0).unwrap(), "Diana");
}

#[test]
fn update_user_profile() {
    let db = setup_social_db();

    // 更新 Alice 的 age 和 bio
    db.execute("MATCH (p:Person) WHERE p.id = 1 SET p.age = 29").unwrap();
    db.execute("MATCH (p:Person) WHERE p.id = 1 SET p.bio = 'Senior engineer'").unwrap();

    // 验证更新
    let result = db.query("MATCH (p:Person) WHERE p.id = 1 RETURN p.age, p.bio").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 29);
    assert_eq!(result.rows()[0].get_string(1).unwrap(), "Senior engineer");

    // name 未被修改
    let name = db.query("MATCH (p:Person) WHERE p.id = 1 RETURN p.name").unwrap();
    assert_eq!(name.rows()[0].get_string(0).unwrap(), "Alice");
}

#[test]
fn filter_by_age_range() {
    let db = setup_social_db();

    // 年龄在 25-30 之间的用户
    let result = db
        .query(
            "MATCH (p:Person) WHERE p.age >= 25 AND p.age <= 30 \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();

    // 25: Charlie; 26: Ivy, Ruby; 27: Eve, Olivia; 28: Alice, Mia;
    // 29: Grace, Quinn; 30: Diana, Noah = 11 人
    assert_eq!(result.num_rows(), 11);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "Alice");
    assert_eq!(result.rows()[10].get_string(0).unwrap(), "Ruby");

    // 使用 contains() 函数搜索 bio 包含 "engineer" 的用户
    let result2 = db
        .query(
            "MATCH (p:Person) WHERE contains(p.bio, 'engineer') \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();

    // bio 含 "engineer": Alice(Software engineer), Eve(Backend engineer),
    // Frank(DevOps engineer), Mia(Data engineer), Noah(ML engineer),
    // Olivia(QA engineer), Paul(Security engineer), Quinn(Cloud engineer),
    // Sam(Database engineer) = 9 人
    assert_eq!(result2.num_rows(), 9);
}

#[test]
fn optional_match_no_posts() {
    let db = setup_social_db();

    // 找没发过帖子的用户
    // 发过帖的: Alice(1), Bob(2), Charlie(3), Eve(5), Frank(6),
    //           Hank(8), Jack(10), Leo(12), Noah(14), Sam(19) = 10 人
    // 没发过帖的: Diana(4), Grace(7), Ivy(9), Kate(11), Mia(13),
    //             Olivia(15), Paul(16), Quinn(17), Ruby(18), Tina(20) = 10 人
    let result = db
        .query(
            "MATCH (p:Person) \
             OPTIONAL MATCH (p)-[:POSTED]->(post:Post) \
             WHERE post.id IS NULL \
             RETURN p.name ORDER BY p.name",
        )
        .unwrap();

    // OPTIONAL MATCH + WHERE post.id IS NULL: 筛选出没有 POSTED 关系的用户
    assert_eq!(result.num_rows(), 10);

    let names: Vec<&str> = result.rows().iter().map(|r| r.get_string(0).unwrap()).collect();
    assert!(names.contains(&"Diana"));
    assert!(names.contains(&"Tina"));
    assert!(!names.contains(&"Alice")); // Alice 发过帖子
}

#[test]
fn with_pipeline() {
    let db = setup_social_db();

    // 分段管道查询测试
    //
    // 测试 1: CALL { subquery } 实现分段管道
    // 先用 CALL 子查询统计全库帖子数，再与每个 Beijing 用户交叉组合
    let result = db
        .query(
            "CALL { MATCH (n:Person)-[:POSTED]->(post:Post) RETURN count(post) AS total_posts } \
             MATCH (author:Person)-[:LIVES_IN]->(c:City) \
             WHERE c.name = 'Beijing' \
             RETURN author.name, total_posts ORDER BY author.name",
        )
        .unwrap();

    // Beijing 4 个用户 x 1 行 subquery = 4 行
    assert_eq!(result.num_rows(), 4);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "Alice");
    // 全库共 12 个帖子
    assert_eq!(result.rows()[0].get_int(1).unwrap(), 12);
    assert_eq!(result.rows()[3].get_string(0).unwrap(), "Diana");

    // 测试 2: 多步查询实现管道效果
    // 先找 Beijing 的用户 ID，再逐个查他们发的帖子
    let beijing_users = db
        .query(
            "MATCH (p:Person)-[:LIVES_IN]->(c:City) \
             WHERE c.name = 'Beijing' \
             RETURN p.id, p.name ORDER BY p.name",
        )
        .unwrap();
    assert_eq!(beijing_users.num_rows(), 4);
    assert_eq!(beijing_users.rows()[0].get_string(1).unwrap(), "Alice");
    assert_eq!(beijing_users.rows()[1].get_string(1).unwrap(), "Bob");
    assert_eq!(beijing_users.rows()[2].get_string(1).unwrap(), "Charlie");
    assert_eq!(beijing_users.rows()[3].get_string(1).unwrap(), "Diana");

    // 然后查 Beijing 用户中每人发的帖子数
    let mut post_counts: Vec<(&str, i64)> = Vec::new();
    for row in beijing_users.rows() {
        let uid = row.get_int(0).unwrap();
        let name = row.get_string(1).unwrap();
        let posts = db
            .query(&format!(
                "MATCH (p:Person)-[:POSTED]->(post:Post) WHERE p.id = {} RETURN count(post)",
                uid
            ))
            .unwrap();
        let count = posts.rows()[0].get_int(0).unwrap();
        if count > 0 {
            post_counts.push((name, count));
        }
    }

    // Alice:2, Bob:1, Charlie:1, Diana:0(跳过)
    assert_eq!(post_counts.len(), 3);
    post_counts.sort_by(|a, b| a.0.cmp(b.0));
    assert_eq!(post_counts[0].0, "Alice");
    assert_eq!(post_counts[0].1, 2);
    assert_eq!(post_counts[1].0, "Bob");
    assert_eq!(post_counts[1].1, 1);
    assert_eq!(post_counts[2].0, "Charlie");
    assert_eq!(post_counts[2].1, 1);
}

#[test]
fn unwind_batch_follow() {
    let db = Database::in_memory();
    create_schema(&db);

    // 插入少量数据用于 UNWIND 测试
    db.execute("CREATE (p:Person {id: 100, name: 'TestUser', age: 25, bio: 'tester'})").unwrap();
    db.execute("CREATE (p:Person {id: 201, name: 'Target1', age: 26, bio: 'target1'})").unwrap();
    db.execute("CREATE (p:Person {id: 202, name: 'Target2', age: 27, bio: 'target2'})").unwrap();
    db.execute("CREATE (p:Person {id: 203, name: 'Target3', age: 28, bio: 'target3'})").unwrap();

    // 使用 UNWIND 批量让 TestUser 关注多个用户
    db.execute(
        "UNWIND [201, 202, 203] AS tid \
         MATCH (a:Person), (b:Person) WHERE a.id = 100 AND b.id = tid \
         CREATE (a)-[:FOLLOWS]->(b)",
    )
    .unwrap();

    // 验证 TestUser 关注了 3 个人
    let result = db
        .query(
            "MATCH (a:Person)-[:FOLLOWS]->(b:Person) \
             WHERE a.id = 100 RETURN b.name ORDER BY b.name",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 3);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "Target1");
    assert_eq!(result.rows()[1].get_string(0).unwrap(), "Target2");
    assert_eq!(result.rows()[2].get_string(0).unwrap(), "Target3");
}

#[test]
fn tag_driven_content_discovery() {
    let db = setup_social_db();

    let tag_likes = db
        .query(
            "MATCH (p:Person)-[:LIKED]->(post:Post)-[:HAS_TAG]->(tag:Tag) \
             RETURN tag.name, count(p)",
        )
        .unwrap();
    assert!(tag_likes.num_rows() >= 4);

    let mut like_counts = std::collections::HashMap::new();
    for row in tag_likes.rows() {
        like_counts.insert(row.get_string(0).unwrap().to_string(), row.get_int(1).unwrap());
    }

    assert_eq!(like_counts["Rust"], 8);
    assert_eq!(like_counts["Graph"], 3);
    assert_eq!(like_counts["Frontend"], 2);
    assert_eq!(like_counts["Infra"], 3);

    let rust_authors = db
        .query(
            "MATCH (author:Person)-[:POSTED]->(post:Post)-[:HAS_TAG]->(tag:Tag) \
             WHERE tag.name = 'Rust' \
             RETURN author.name ORDER BY author.name",
        )
        .unwrap();
    assert_eq!(rust_authors.num_rows(), 2);
    assert_eq!(rust_authors.rows()[0].get_string(0).unwrap(), "Alice");
    assert_eq!(rust_authors.rows()[1].get_string(0).unwrap(), "Alice");
}
