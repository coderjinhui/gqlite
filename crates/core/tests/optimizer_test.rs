use gqlite_core::binder::Binder;
use gqlite_core::catalog::{Catalog, ColumnDef};
use gqlite_core::parser::ast::{BinOp, Direction, Expr};
use gqlite_core::parser::parser::Parser;
use gqlite_core::planner::logical::{LogicalOperator, Planner};
use gqlite_core::planner::optimizer::{
    collect_plan_aliases, combine_conjuncts, optimize, referenced_aliases, split_conjuncts,
};
use gqlite_core::types::data_type::DataType;

fn test_catalog() -> Catalog {
    let mut catalog = Catalog::new();
    catalog
        .create_node_table(
            "Person",
            vec![
                ColumnDef {
                    column_id: 0,
                    name: "id".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnDef {
                    column_id: 1,
                    name: "name".into(),
                    data_type: DataType::String,
                    nullable: true,
                },
                ColumnDef {
                    column_id: 2,
                    name: "age".into(),
                    data_type: DataType::Int64,
                    nullable: true,
                },
            ],
            "id",
        )
        .unwrap();
    catalog
        .create_node_table(
            "City",
            vec![
                ColumnDef {
                    column_id: 0,
                    name: "id".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnDef {
                    column_id: 1,
                    name: "name".into(),
                    data_type: DataType::String,
                    nullable: true,
                },
            ],
            "id",
        )
        .unwrap();
    catalog.create_rel_table("KNOWS", "Person", "Person", vec![]).unwrap();
    catalog.create_rel_table("LIVES_IN", "Person", "City", vec![]).unwrap();
    catalog
}

fn plan_and_optimize(catalog: &Catalog, query: &str) -> LogicalOperator {
    let stmt = Parser::parse_query(query).unwrap();
    let mut binder = Binder::new(catalog);
    let bound = binder.bind(&stmt).unwrap();
    let planner = Planner::new(catalog);
    let logical = planner.plan(&bound).unwrap();
    optimize(logical)
}

// ── Filter push-down tests ─────────────────────────────────

#[test]
fn filter_pushdown_single_table_predicate() {
    let catalog = test_catalog();
    let plan = plan_and_optimize(
        &catalog,
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.age > 30 RETURN a.name, b.name",
    );
    if let LogicalOperator::Projection { input, .. } = &plan {
        if let LogicalOperator::Expand { input: expand_in, .. } = input.as_ref() {
            assert!(
                matches!(expand_in.as_ref(), LogicalOperator::Filter { .. }),
                "expected Filter below Expand, got {:?}",
                expand_in
            );
            if let LogicalOperator::Filter { input: filter_in, .. } = expand_in.as_ref() {
                assert!(
                    matches!(filter_in.as_ref(), LogicalOperator::ScanNode { .. }),
                    "expected ScanNode below pushed Filter, got {:?}",
                    filter_in
                );
            }
        } else {
            panic!("expected Expand under Projection, got {:?}", input);
        }
    } else {
        panic!("expected Projection at top, got {:?}", plan);
    }
}

#[test]
fn filter_pushdown_cross_table_stays() {
    let catalog = test_catalog();
    let plan = plan_and_optimize(
        &catalog,
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.id = b.id RETURN a, b",
    );
    if let LogicalOperator::Projection { input, .. } = &plan {
        assert!(
            matches!(input.as_ref(), LogicalOperator::Filter { .. }),
            "cross-table predicate should stay above Expand, got {:?}",
            input
        );
    } else {
        panic!("expected Projection at top, got {:?}", plan);
    }
}

#[test]
fn filter_pushdown_mixed_conjuncts() {
    let catalog = test_catalog();
    let plan = plan_and_optimize(
        &catalog,
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.age > 30 AND a.id = b.id RETURN a, b",
    );
    if let LogicalOperator::Projection { input, .. } = &plan {
        if let LogicalOperator::Filter { input: join_filter_in, .. } = input.as_ref() {
            if let LogicalOperator::Expand { input: expand_in, .. } = join_filter_in.as_ref() {
                assert!(
                    matches!(expand_in.as_ref(), LogicalOperator::Filter { .. }),
                    "single-table predicate should be pushed below Expand, got {:?}",
                    expand_in
                );
            } else {
                panic!("expected Expand below cross-table filter, got {:?}", join_filter_in);
            }
        } else {
            panic!("expected Filter (cross-table) under Projection, got {:?}", input);
        }
    } else {
        panic!("expected Projection at top, got {:?}", plan);
    }
}

#[test]
fn filter_pushdown_no_filter() {
    let catalog = test_catalog();
    let plan = plan_and_optimize(&catalog, "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, b");
    if let LogicalOperator::Projection { input, .. } = &plan {
        assert!(
            matches!(input.as_ref(), LogicalOperator::Expand { .. }),
            "no filter to push, Expand should be directly under Projection"
        );
    } else {
        panic!("expected Projection at top");
    }
}

// ── Projection push-down tests ─────────────────────────────

#[test]
fn projection_pushdown_collects_columns() {
    let catalog = test_catalog();
    let plan = plan_and_optimize(&catalog, "MATCH (n:Person) RETURN n.name");
    assert!(matches!(plan, LogicalOperator::Projection { .. }));
}

// ── Helper tests ───────────────────────────────────────────

#[test]
fn split_and_combine_conjuncts() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::IntLit(1)),
            op: BinOp::And,
            right: Box::new(Expr::IntLit(2)),
        }),
        op: BinOp::And,
        right: Box::new(Expr::IntLit(3)),
    };
    let parts = split_conjuncts(expr);
    assert_eq!(parts.len(), 3);
    let combined = combine_conjuncts(parts).unwrap();
    // Should be AND tree
    assert!(matches!(combined, Expr::BinaryOp { op: BinOp::And, .. }));
}

#[test]
fn referenced_aliases_property() {
    let expr = Expr::Property(Box::new(Expr::Ident("a".into())), "age".into());
    let aliases = referenced_aliases(&expr);
    assert_eq!(aliases, vec!["a".to_string()]);
}

#[test]
fn referenced_aliases_binary_cross() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Property(Box::new(Expr::Ident("a".into())), "id".into())),
        op: BinOp::Eq,
        right: Box::new(Expr::Property(Box::new(Expr::Ident("b".into())), "id".into())),
    };
    let aliases = referenced_aliases(&expr);
    assert_eq!(aliases, vec!["a".to_string(), "b".to_string()]);
}

#[test]
fn collect_aliases_from_expand() {
    let plan = LogicalOperator::Expand {
        input: Box::new(LogicalOperator::ScanNode {
            table_name: "Person".into(),
            table_id: 0,
            columns: vec![],
            alias: "a".into(),
        }),
        rel_table_name: "KNOWS".into(),
        rel_table_id: 1,
        direction: Direction::Right,
        src_alias: "a".into(),
        dst_alias: "b".into(),
        rel_alias: Some("r".into()),
        dst_table_name: Some("Person".into()),
        dst_table_id: Some(0),
        optional: false,
    };
    let aliases = collect_plan_aliases(&plan);
    assert!(aliases.contains(&"a".to_string()));
    assert!(aliases.contains(&"b".to_string()));
    assert!(aliases.contains(&"r".to_string()));
}
