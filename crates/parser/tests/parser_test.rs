use gqlite_parser::ast::*;
use gqlite_parser::data_type::DataType;
use gqlite_parser::Parser;

fn parse(input: &str) -> Statement {
    Parser::parse_query(input).unwrap()
}

fn parse_err(input: &str) -> String {
    Parser::parse_query(input).unwrap_err().to_string()
}

// ── Expression tests (Plan 016) ─────────────────────────────

#[test]
fn expr_literal_types() {
    let stmt = parse("RETURN 42, 3.14, 'hello', true, false, null");
    let Statement::Query(q) = stmt else {
        panic!("expected query");
    };
    let Clause::Return(ret) = &q.clauses[0] else {
        panic!("expected return");
    };
    assert!(matches!(ret.items[0].expr, Expr::IntLit(42)));
    assert!(matches!(ret.items[1].expr, Expr::FloatLit(_)));
    assert!(matches!(ret.items[2].expr, Expr::StringLit(_)));
    assert!(matches!(ret.items[3].expr, Expr::BoolLit(true)));
    assert!(matches!(ret.items[4].expr, Expr::BoolLit(false)));
    assert!(matches!(ret.items[5].expr, Expr::NullLit));
}

#[test]
fn expr_operator_precedence() {
    // 1 + 2 * 3 → 1 + (2 * 3)
    let stmt = parse("RETURN 1 + 2 * 3");
    let Statement::Query(q) = stmt else {
        panic!();
    };
    let Clause::Return(ret) = &q.clauses[0] else {
        panic!();
    };
    let Expr::BinaryOp { op, .. } = &ret.items[0].expr else {
        panic!();
    };
    assert_eq!(*op, BinOp::Add);
}

#[test]
fn expr_and_or_precedence() {
    // a AND b OR c → (a AND b) OR c
    let stmt = parse("MATCH (n) WHERE n.a = 1 AND n.b = 2 OR n.c = 3 RETURN n");
    let Statement::Query(q) = stmt else {
        panic!();
    };
    let Clause::Where(w) = &q.clauses[1] else {
        panic!();
    };
    assert!(matches!(w.expr, Expr::BinaryOp { op: BinOp::Or, .. }));
}

#[test]
fn expr_is_null() {
    let stmt = parse("MATCH (n) WHERE n.value IS NOT NULL RETURN n");
    let Statement::Query(q) = stmt else {
        panic!();
    };
    let Clause::Where(w) = &q.clauses[1] else {
        panic!();
    };
    let Expr::IsNull { negated, .. } = &w.expr else {
        panic!();
    };
    assert!(*negated);
}

#[test]
fn expr_function_call() {
    let stmt = parse("RETURN count(DISTINCT n.name)");
    let Statement::Query(q) = stmt else {
        panic!();
    };
    let Clause::Return(ret) = &q.clauses[0] else {
        panic!();
    };
    let Expr::FunctionCall { name, distinct, .. } = &ret.items[0].expr else {
        panic!();
    };
    assert_eq!(name, "count");
    assert!(*distinct);
}

#[test]
fn expr_count_star() {
    let stmt = parse("RETURN count(*)");
    let Statement::Query(q) = stmt else {
        panic!();
    };
    let Clause::Return(ret) = &q.clauses[0] else {
        panic!();
    };
    let Expr::FunctionCall { args, .. } = &ret.items[0].expr else {
        panic!();
    };
    assert!(matches!(args[0], Expr::Star));
}

// ── MATCH tests (Plan 017) ──────────────────────────────────

#[test]
fn match_simple_node() {
    let stmt = parse("MATCH (n) RETURN n");
    let Statement::Query(q) = stmt else {
        panic!();
    };
    let Clause::Match(m) = &q.clauses[0] else {
        panic!();
    };
    assert_eq!(m.pattern.paths.len(), 1);
}

#[test]
fn match_labeled_node() {
    let stmt = parse("MATCH (a:Person) RETURN a");
    let Statement::Query(q) = stmt else {
        panic!();
    };
    let Clause::Match(m) = &q.clauses[0] else {
        panic!();
    };
    let PatternElement::Node(n) = &m.pattern.paths[0].elements[0] else {
        panic!();
    };
    assert_eq!(n.alias.as_deref(), Some("a"));
    assert_eq!(n.label.as_deref(), Some("Person"));
}

#[test]
fn match_with_properties() {
    let stmt = parse("MATCH (a:Person {name: 'Alice'}) RETURN a");
    let Statement::Query(q) = stmt else {
        panic!();
    };
    let Clause::Match(m) = &q.clauses[0] else {
        panic!();
    };
    let PatternElement::Node(n) = &m.pattern.paths[0].elements[0] else {
        panic!();
    };
    assert_eq!(n.properties.len(), 1);
    assert_eq!(n.properties[0].0, "name");
}

#[test]
fn match_directed_relationship() {
    let stmt = parse("MATCH (a)-[r:KNOWS]->(b) RETURN a, b");
    let Statement::Query(q) = stmt else {
        panic!();
    };
    let Clause::Match(m) = &q.clauses[0] else {
        panic!();
    };
    let path = &m.pattern.paths[0];
    assert_eq!(path.elements.len(), 3); // node, rel, node
    let PatternElement::Rel(r) = &path.elements[1] else {
        panic!();
    };
    assert_eq!(r.direction, Direction::Right);
    assert_eq!(r.label.as_deref(), Some("KNOWS"));
}

#[test]
fn match_undirected() {
    let stmt = parse("MATCH (a)-[r]-(b) RETURN a");
    let Statement::Query(q) = stmt else {
        panic!();
    };
    let Clause::Match(m) = &q.clauses[0] else {
        panic!();
    };
    let PatternElement::Rel(r) = &m.pattern.paths[0].elements[1] else {
        panic!();
    };
    assert_eq!(r.direction, Direction::Both);
}

#[test]
fn match_multiple_patterns() {
    let stmt = parse("MATCH (a)-[:KNOWS]->(b), (b)-[:LIVES_IN]->(c) RETURN a, b, c");
    let Statement::Query(q) = stmt else {
        panic!();
    };
    let Clause::Match(m) = &q.clauses[0] else {
        panic!();
    };
    assert_eq!(m.pattern.paths.len(), 2);
}

// ── WHERE / RETURN tests (Plan 018) ─────────────────────────

#[test]
fn where_clause() {
    let stmt = parse("MATCH (a:Person) WHERE a.age > 30 AND a.name = 'Alice' RETURN a.name");
    let Statement::Query(q) = stmt else {
        panic!();
    };
    assert_eq!(q.clauses.len(), 3); // MATCH, WHERE, RETURN
    assert!(matches!(q.clauses[1], Clause::Where(_)));
}

#[test]
fn return_with_alias() {
    let stmt = parse("RETURN a.name AS name, count(a) AS cnt");
    let Statement::Query(q) = stmt else {
        panic!();
    };
    let Clause::Return(ret) = &q.clauses[0] else {
        panic!();
    };
    assert_eq!(ret.items[0].alias.as_deref(), Some("name"));
    assert_eq!(ret.items[1].alias.as_deref(), Some("cnt"));
}

#[test]
fn return_star() {
    let stmt = parse("MATCH (n) RETURN *");
    let Statement::Query(q) = stmt else {
        panic!();
    };
    let Clause::Return(ret) = &q.clauses[1] else {
        panic!();
    };
    assert!(ret.return_all);
}

#[test]
fn return_distinct() {
    let stmt = parse("MATCH (n) RETURN DISTINCT n.city");
    let Statement::Query(q) = stmt else {
        panic!();
    };
    let Clause::Return(ret) = &q.clauses[1] else {
        panic!();
    };
    assert!(ret.distinct);
}

// ── CREATE / SET / DELETE tests (Plan 019) ──────────────────

#[test]
fn create_node() {
    let stmt = parse("CREATE (n:Person {name: 'Alice'})");
    let Statement::Query(q) = stmt else {
        panic!();
    };
    let Clause::Create(c) = &q.clauses[0] else {
        panic!();
    };
    let PatternElement::Node(n) = &c.pattern.paths[0].elements[0] else {
        panic!();
    };
    assert_eq!(n.label.as_deref(), Some("Person"));
}

#[test]
fn create_relationship() {
    let stmt = parse("MATCH (a), (b) CREATE (a)-[:KNOWS]->(b)");
    let Statement::Query(q) = stmt else {
        panic!();
    };
    assert_eq!(q.clauses.len(), 2); // MATCH + CREATE
    assert!(matches!(q.clauses[1], Clause::Create(_)));
}

#[test]
fn set_property() {
    let stmt = parse("MATCH (n) SET n.name = 'Bob'");
    let Statement::Query(q) = stmt else {
        panic!();
    };
    let Clause::Set(s) = &q.clauses[1] else {
        panic!();
    };
    assert_eq!(s.items[0].property.variable, "n");
    assert_eq!(s.items[0].property.field, "name");
}

#[test]
fn detach_delete() {
    let stmt = parse("MATCH (n) DETACH DELETE n");
    let Statement::Query(q) = stmt else {
        panic!();
    };
    let Clause::Delete(d) = &q.clauses[1] else {
        panic!();
    };
    assert!(d.detach);
}

// ── DDL tests (Plan 020) ────────────────────────────────────

#[test]
fn create_node_table() {
    let stmt =
        parse("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))");
    let Statement::CreateNodeTable(t) = stmt else {
        panic!("expected CreateNodeTable, got {:?}", stmt);
    };
    assert_eq!(t.name, "Person");
    assert_eq!(t.columns.len(), 3);
    assert_eq!(t.primary_key, "id");
    assert_eq!(t.columns[0].name, "id");
    assert_eq!(t.columns[0].data_type, DataType::Int64);
    assert_eq!(t.columns[1].data_type, DataType::String);
}

#[test]
fn create_rel_table() {
    let stmt = parse("CREATE REL TABLE Knows (FROM Person TO Person, since INT64)");
    let Statement::CreateRelTable(t) = stmt else {
        panic!("expected CreateRelTable, got {:?}", stmt);
    };
    assert_eq!(t.name, "Knows");
    assert_eq!(t.from_table, "Person");
    assert_eq!(t.to_table, "Person");
    assert_eq!(t.columns.len(), 1);
    assert_eq!(t.columns[0].name, "since");
}

#[test]
fn drop_table() {
    let stmt = parse("DROP TABLE Person");
    let Statement::DropTable(t) = stmt else {
        panic!();
    };
    assert_eq!(t.name, "Person");
}

#[test]
fn create_rel_table_no_props() {
    let stmt = parse("CREATE REL TABLE Follows (FROM Person TO Person)");
    let Statement::CreateRelTable(t) = stmt else {
        panic!();
    };
    assert!(t.columns.is_empty());
}

// ── Full query integration ──────────────────────────────────

#[test]
fn full_query_pipeline() {
    let stmt =
        parse("MATCH (a:Person) WHERE a.age > 30 RETURN a.name AS name ORDER BY a.name LIMIT 10");
    let Statement::Query(q) = stmt else {
        panic!();
    };
    assert_eq!(q.clauses.len(), 5); // MATCH, WHERE, RETURN, ORDER BY, LIMIT
}

#[test]
fn parse_error_message() {
    let err = parse_err("MATCH");
    assert!(err.contains("parse error"));
}

// ── OPTIONAL MATCH / UNION / UNWIND / MERGE tests (Plan 038/039) ──

#[test]
fn optional_match() {
    let stmt = parse("OPTIONAL MATCH (n:Person) RETURN n");
    let Statement::Query(q) = stmt else { panic!() };
    let Clause::Match(m) = &q.clauses[0] else { panic!() };
    assert!(m.optional);
}

#[test]
fn union_all() {
    let stmt = parse("MATCH (a:Person) RETURN a UNION ALL MATCH (b:Person) RETURN b");
    assert!(matches!(stmt, Statement::Union { all: true, .. }));
}

#[test]
fn union_distinct() {
    let stmt = parse("MATCH (a:Person) RETURN a UNION MATCH (b:Person) RETURN b");
    assert!(matches!(stmt, Statement::Union { all: false, .. }));
}

#[test]
fn unwind_clause() {
    let stmt = parse("UNWIND [1, 2, 3] AS x RETURN x");
    let Statement::Query(q) = stmt else { panic!() };
    let Clause::Unwind(u) = &q.clauses[0] else { panic!() };
    assert_eq!(u.alias, "x");
    assert!(matches!(u.expr, Expr::ListLit(_)));
}

#[test]
fn list_literal() {
    let stmt = parse("UNWIND [1, 'hello', 3.14] AS item RETURN item");
    let Statement::Query(q) = stmt else { panic!() };
    let Clause::Unwind(u) = &q.clauses[0] else { panic!() };
    let Expr::ListLit(items) = &u.expr else { panic!() };
    assert_eq!(items.len(), 3);
}

#[test]
fn merge_basic() {
    let stmt = parse("MERGE (n:Person {name: 'Alice'})");
    let Statement::Query(q) = stmt else { panic!() };
    assert!(matches!(&q.clauses[0], Clause::Merge(_)));
}

#[test]
fn merge_with_on_create_and_on_match() {
    let stmt =
        parse("MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.age = 30 ON MATCH SET n.age = 31");
    let Statement::Query(q) = stmt else { panic!() };
    let Clause::Merge(m) = &q.clauses[0] else { panic!() };
    assert_eq!(m.on_create.len(), 1);
    assert_eq!(m.on_match.len(), 1);
}

#[test]
fn cast_expression() {
    let stmt = parse("RETURN CAST('42' AS INT64)");
    let Statement::Query(q) = stmt else { panic!() };
    let Clause::Return(r) = &q.clauses[0] else { panic!() };
    let Expr::Cast { target_type, .. } = &r.items[0].expr else { panic!() };
    assert_eq!(*target_type, DataType::Int64);
}

#[test]
fn var_length_path() {
    let stmt = parse("MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN b");
    let Statement::Query(q) = stmt else { panic!() };
    let Clause::Match(m) = &q.clauses[0] else { panic!() };
    let PatternElement::Rel(r) = &m.pattern.paths[0].elements[1] else { panic!() };
    assert_eq!(r.var_length, Some((1, 3)));
    assert_eq!(r.direction, Direction::Right);
}

#[test]
fn var_length_star_only() {
    let stmt = parse("MATCH (a)-[*]->(b) RETURN b");
    let Statement::Query(q) = stmt else { panic!() };
    let Clause::Match(m) = &q.clauses[0] else { panic!() };
    let PatternElement::Rel(r) = &m.pattern.paths[0].elements[1] else { panic!() };
    assert_eq!(r.var_length, Some((1, u32::MAX)));
}

#[test]
fn var_length_max_only() {
    let stmt = parse("MATCH (a)-[:KNOWS*..5]->(b) RETURN b");
    let Statement::Query(q) = stmt else { panic!() };
    let Clause::Match(m) = &q.clauses[0] else { panic!() };
    let PatternElement::Rel(r) = &m.pattern.paths[0].elements[1] else { panic!() };
    assert_eq!(r.var_length, Some((1, 5)));
}

#[test]
fn var_length_exact() {
    let stmt = parse("MATCH (a)-[*2]->(b) RETURN b");
    let Statement::Query(q) = stmt else { panic!() };
    let Clause::Match(m) = &q.clauses[0] else { panic!() };
    let PatternElement::Rel(r) = &m.pattern.paths[0].elements[1] else { panic!() };
    assert_eq!(r.var_length, Some((2, 2)));
}

// ── shortestPath parsing tests ──────────────────────────────

#[test]
fn shortest_path_basic() {
    let stmt =
        parse("MATCH (a:Person), (b:Person), p = shortestPath((a)-[:KNOWS*..10]->(b)) RETURN p");
    let Statement::Query(q) = stmt else { panic!() };
    let Clause::Match(m) = &q.clauses[0] else { panic!() };
    // Two regular path patterns (a:Person) and (b:Person)
    assert_eq!(m.pattern.paths.len(), 2);
    // One shortest-path pattern
    assert_eq!(m.pattern.shortest_paths.len(), 1);
    let sp = &m.pattern.shortest_paths[0];
    assert_eq!(sp.path_variable, "p");
    assert!(!sp.all_paths);
    // Inner pattern: (a)-[:KNOWS*..10]->(b)
    assert_eq!(sp.pattern.elements.len(), 3);
    let PatternElement::Node(src) = &sp.pattern.elements[0] else { panic!() };
    assert_eq!(src.alias.as_deref(), Some("a"));
    let PatternElement::Rel(rel) = &sp.pattern.elements[1] else { panic!() };
    assert_eq!(rel.label.as_deref(), Some("KNOWS"));
    assert_eq!(rel.var_length, Some((1, 10)));
    assert_eq!(rel.direction, Direction::Right);
    let PatternElement::Node(dst) = &sp.pattern.elements[2] else { panic!() };
    assert_eq!(dst.alias.as_deref(), Some("b"));
}

#[test]
fn all_shortest_paths_parse() {
    let stmt =
        parse("MATCH (a:Person), (b:Person), p = allShortestPaths((a)-[:KNOWS*..5]->(b)) RETURN p");
    let Statement::Query(q) = stmt else { panic!() };
    let Clause::Match(m) = &q.clauses[0] else { panic!() };
    assert_eq!(m.pattern.shortest_paths.len(), 1);
    let sp = &m.pattern.shortest_paths[0];
    assert_eq!(sp.path_variable, "p");
    assert!(sp.all_paths);
}

// ── EXISTS expression tests ─────────────────────────────────

#[test]
fn exists_subquery_parse() {
    let stmt =
        parse("MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS]->(:Person) } RETURN p.name");
    let Statement::Query(q) = stmt else { panic!() };
    // Clause 0: MATCH, Clause 1: WHERE, Clause 2: RETURN
    let Clause::Where(w) = &q.clauses[1] else {
        panic!("expected WHERE clause");
    };
    match &w.expr {
        Expr::Exists(inner) => {
            // The inner query should have a MATCH clause
            assert!(!inner.clauses.is_empty());
            let Clause::Match(_) = &inner.clauses[0] else {
                panic!("expected inner MATCH clause");
            };
        }
        _ => panic!("expected Expr::Exists"),
    }
}

#[test]
fn not_exists_subquery_parse() {
    let stmt =
        parse("MATCH (p:Person) WHERE NOT EXISTS { MATCH (p)-[:KNOWS]->(:Person) } RETURN p.name");
    let Statement::Query(q) = stmt else { panic!() };
    let Clause::Where(w) = &q.clauses[1] else {
        panic!("expected WHERE clause");
    };
    // NOT EXISTS is parsed as UnaryOp(Not, Exists(...))
    match &w.expr {
        Expr::UnaryOp { op: UnaryOp::Not, expr: inner } => match inner.as_ref() {
            Expr::Exists(query) => {
                assert!(!query.clauses.is_empty());
            }
            _ => panic!("expected Expr::Exists inside NOT"),
        },
        _ => panic!("expected UnaryOp(Not, Exists(...))"),
    }
}

// ── CALL procedure parsing ─────────────────────────────────

#[test]
fn call_simple_procedure() {
    let stmt = parse("CALL dbms.tables()");
    let Statement::Call { procedure, args, yields } = stmt else {
        panic!("expected Call statement");
    };
    assert_eq!(procedure, "dbms.tables");
    assert!(args.is_empty());
    assert!(yields.is_empty());
}

#[test]
fn call_with_yield() {
    let stmt = parse("CALL dbms.tables() YIELD name, type");
    let Statement::Call { procedure, args, yields } = stmt else {
        panic!("expected Call statement");
    };
    assert_eq!(procedure, "dbms.tables");
    assert!(args.is_empty());
    assert_eq!(yields, vec!["name", "type"]);
}

#[test]
fn call_with_args() {
    let stmt = parse("CALL algo.pagerank('Person', 20)");
    let Statement::Call { procedure, args, yields } = stmt else {
        panic!("expected Call statement");
    };
    assert_eq!(procedure, "algo.pagerank");
    assert_eq!(args.len(), 2);
    assert!(matches!(&args[0], Expr::StringLit(s) if s == "Person"));
    assert!(matches!(&args[1], Expr::IntLit(20)));
    assert!(yields.is_empty());
}

#[test]
fn call_case_insensitive() {
    let stmt = parse("call dbms.tables() yield name");
    let Statement::Call { procedure, yields, .. } = stmt else {
        panic!("expected Call statement");
    };
    assert_eq!(procedure, "dbms.tables");
    assert_eq!(yields, vec!["name"]);
}

#[test]
fn call_dotted_name_three_parts() {
    let stmt = parse("CALL a.b.c()");
    let Statement::Call { procedure, .. } = stmt else {
        panic!("expected Call statement");
    };
    assert_eq!(procedure, "a.b.c");
}

#[test]
fn call_subquery_basic() {
    let stmt = parse("CALL { MATCH (n:Person) RETURN n.name AS name } RETURN name");
    let Statement::Query(q) = stmt else {
        panic!("expected query");
    };
    assert_eq!(q.clauses.len(), 2); // CallSubquery + Return
    let Clause::CallSubquery(sub) = &q.clauses[0] else {
        panic!("expected CallSubquery clause");
    };
    assert_eq!(sub.clauses.len(), 2); // Match + Return
    assert!(matches!(&sub.clauses[0], Clause::Match(_)));
    assert!(matches!(&sub.clauses[1], Clause::Return(_)));
    assert!(matches!(&q.clauses[1], Clause::Return(_)));
}

#[test]
fn call_subquery_with_preceding_match() {
    let stmt = parse(
        "MATCH (a:Person) CALL { MATCH (b:Person) RETURN count(b) AS total } RETURN a.name, total",
    );
    let Statement::Query(q) = stmt else {
        panic!("expected query");
    };
    assert_eq!(q.clauses.len(), 3); // Match + CallSubquery + Return
    assert!(matches!(&q.clauses[0], Clause::Match(_)));
    let Clause::CallSubquery(sub) = &q.clauses[1] else {
        panic!("expected CallSubquery clause");
    };
    assert_eq!(sub.clauses.len(), 2); // Match + Return
    assert!(matches!(&q.clauses[2], Clause::Return(_)));
}

#[test]
fn call_subquery_empty_body_errors() {
    let err = parse_err("CALL { } RETURN 1");
    assert!(err.contains("expected clauses inside CALL { ... }"));
}

// ── parse_all (multi-statement) tests ────────────────────────

#[test]
fn parse_all_single_statement() {
    let stmts = Parser::parse_all("MATCH (n) RETURN n;").unwrap();
    assert_eq!(stmts.len(), 1);
}

#[test]
fn parse_all_multiple_statements() {
    let stmts = Parser::parse_all(
        "CREATE NODE TABLE A(id INT64, PRIMARY KEY(id)); \
         CREATE NODE TABLE B(id INT64, PRIMARY KEY(id));",
    )
    .unwrap();
    assert_eq!(stmts.len(), 2);
}

#[test]
fn parse_all_mixed_ddl_and_dml() {
    let stmts = Parser::parse_all(
        "CREATE NODE TABLE N(id INT64, PRIMARY KEY(id)); \
         CREATE (n:N {id: 1}); \
         MATCH (n:N) RETURN n.id;",
    )
    .unwrap();
    assert_eq!(stmts.len(), 3);
}

#[test]
fn parse_all_no_trailing_semicolon() {
    let stmts = Parser::parse_all(
        "CREATE NODE TABLE A(id INT64, PRIMARY KEY(id)); \
         MATCH (n) RETURN n",
    )
    .unwrap();
    assert_eq!(stmts.len(), 2);
}

#[test]
fn parse_all_extra_semicolons() {
    let stmts = Parser::parse_all(";;; MATCH (n) RETURN n ;;; MATCH (m) RETURN m ;;;").unwrap();
    assert_eq!(stmts.len(), 2);
}

#[test]
fn parse_all_empty_input() {
    let stmts = Parser::parse_all("").unwrap();
    assert_eq!(stmts.len(), 0);
}

#[test]
fn parse_all_only_semicolons() {
    let stmts = Parser::parse_all(";;;").unwrap();
    assert_eq!(stmts.len(), 0);
}

// ── Transaction control statements ──────────────────────────

#[test]
fn parse_begin() {
    let stmt = parse("BEGIN");
    assert!(matches!(stmt, Statement::Begin));
}

#[test]
fn parse_begin_transaction() {
    let stmt = parse("BEGIN TRANSACTION");
    assert!(matches!(stmt, Statement::Begin));
}

#[test]
fn parse_commit() {
    let stmt = parse("COMMIT");
    assert!(matches!(stmt, Statement::Commit));
}

#[test]
fn parse_commit_transaction() {
    let stmt = parse("COMMIT TRANSACTION");
    assert!(matches!(stmt, Statement::Commit));
}

#[test]
fn parse_rollback() {
    let stmt = parse("ROLLBACK");
    assert!(matches!(stmt, Statement::Rollback));
}

#[test]
fn parse_rollback_transaction() {
    let stmt = parse("ROLLBACK TRANSACTION");
    assert!(matches!(stmt, Statement::Rollback));
}

#[test]
fn parse_begin_case_insensitive() {
    let stmt = parse("begin");
    assert!(matches!(stmt, Statement::Begin));
    let stmt = parse("Begin");
    assert!(matches!(stmt, Statement::Begin));
}

#[test]
fn parse_all_with_transaction_stmts() {
    let stmts = Parser::parse_all("BEGIN; CREATE (n:A {id: 1}); COMMIT;").unwrap();
    assert_eq!(stmts.len(), 3);
    assert!(matches!(stmts[0], Statement::Begin));
    assert!(matches!(stmts[1], Statement::Query(_)));
    assert!(matches!(stmts[2], Statement::Commit));
}
