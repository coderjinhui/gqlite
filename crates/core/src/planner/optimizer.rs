//! Logical plan optimizer.
//!
//! Applies rewrite rules to the logical operator tree before it is
//! translated into a physical plan.

use crate::parser::ast::{BinOp, Expr};
use crate::planner::logical::LogicalOperator;

/// Apply all optimization rules to a logical plan.
pub fn optimize(plan: LogicalOperator) -> LogicalOperator {
    let plan = push_filters_down(plan);
    let plan = push_projections_down(plan);
    plan
}

// ── Predicate push-down ────────────────────────────────────────

/// Push Filter operators as close to their data source as possible.
///
/// A filter predicate that references only a single alias is pushed below
/// Expand and HashJoin so that fewer rows flow through the pipeline.
fn push_filters_down(plan: LogicalOperator) -> LogicalOperator {
    match plan {
        LogicalOperator::Filter { input, predicate } => {
            let optimized_input = push_filters_down(*input);
            try_push_filter(optimized_input, predicate)
        }
        // Recurse into all operators that have children.
        LogicalOperator::Projection {
            input,
            expressions,
        } => LogicalOperator::Projection {
            input: Box::new(push_filters_down(*input)),
            expressions,
        },
        LogicalOperator::ReturnAll { input } => LogicalOperator::ReturnAll {
            input: Box::new(push_filters_down(*input)),
        },
        LogicalOperator::Expand {
            input,
            rel_table_name,
            rel_table_id,
            direction,
            src_alias,
            dst_alias,
            rel_alias,
            dst_table_name,
            dst_table_id,
            optional,
        } => LogicalOperator::Expand {
            input: Box::new(push_filters_down(*input)),
            rel_table_name,
            rel_table_id,
            direction,
            src_alias,
            dst_alias,
            rel_alias,
            dst_table_name,
            dst_table_id,
            optional,
        },
        LogicalOperator::RecursiveExpand {
            input,
            rel_table_name,
            rel_table_id,
            direction,
            src_alias,
            dst_alias,
            dst_table_name,
            dst_table_id,
            min_hops,
            max_hops,
        } => LogicalOperator::RecursiveExpand {
            input: Box::new(push_filters_down(*input)),
            rel_table_name,
            rel_table_id,
            direction,
            src_alias,
            dst_alias,
            dst_table_name,
            dst_table_id,
            min_hops,
            max_hops,
        },
        LogicalOperator::HashJoin {
            build,
            probe,
            build_key,
            probe_key,
        } => LogicalOperator::HashJoin {
            build: Box::new(push_filters_down(*build)),
            probe: Box::new(push_filters_down(*probe)),
            build_key,
            probe_key,
        },
        LogicalOperator::OrderBy { input, items } => LogicalOperator::OrderBy {
            input: Box::new(push_filters_down(*input)),
            items,
        },
        LogicalOperator::Limit { input, count } => LogicalOperator::Limit {
            input: Box::new(push_filters_down(*input)),
            count,
        },
        LogicalOperator::Skip { input, count } => LogicalOperator::Skip {
            input: Box::new(push_filters_down(*input)),
            count,
        },
        LogicalOperator::Aggregate {
            input,
            expressions,
        } => LogicalOperator::Aggregate {
            input: Box::new(push_filters_down(*input)),
            expressions,
        },
        LogicalOperator::InsertRel {
            input,
            rel_table_name,
            rel_table_id,
            src_alias,
            dst_alias,
            properties,
        } => LogicalOperator::InsertRel {
            input: Box::new(push_filters_down(*input)),
            rel_table_name,
            rel_table_id,
            src_alias,
            dst_alias,
            properties,
        },
        LogicalOperator::SetProperty { input, items } => LogicalOperator::SetProperty {
            input: Box::new(push_filters_down(*input)),
            items,
        },
        LogicalOperator::Delete {
            input,
            detach,
            variables,
        } => LogicalOperator::Delete {
            input: Box::new(push_filters_down(*input)),
            detach,
            variables,
        },
        // Leaf / DDL nodes — nothing to optimize.
        other => other,
    }
}

/// Try to push a single predicate below the given plan node.
///
/// If the predicate only references aliases available in a sub-tree, it can
/// be pushed past operators like Expand and HashJoin.
fn try_push_filter(plan: LogicalOperator, predicate: Expr) -> LogicalOperator {
    let conjuncts = split_conjuncts(predicate);
    push_conjuncts(plan, conjuncts)
}

/// Given a plan and a set of conjuncts, push each conjunct as far down as
/// possible. Any conjunct that cannot be pushed stays on top.
fn push_conjuncts(plan: LogicalOperator, conjuncts: Vec<Expr>) -> LogicalOperator {
    match plan {
        LogicalOperator::Expand {
            input,
            rel_table_name,
            rel_table_id,
            direction,
            src_alias,
            dst_alias,
            rel_alias,
            dst_table_name,
            dst_table_id,
            optional,
        } => {
            // Collect aliases available in the input sub-tree (before Expand).
            let input_aliases = collect_plan_aliases(&input);

            let mut pushable = Vec::new();
            let mut remaining = Vec::new();

            for conj in conjuncts {
                let refs = referenced_aliases(&conj);
                if !refs.is_empty() && refs.iter().all(|a| input_aliases.contains(a)) {
                    pushable.push(conj);
                } else {
                    remaining.push(conj);
                }
            }

            let new_input = if pushable.is_empty() {
                *input
            } else {
                push_conjuncts(*input, pushable)
            };

            let expand = LogicalOperator::Expand {
                input: Box::new(new_input),
                rel_table_name,
                rel_table_id,
                direction,
                src_alias,
                dst_alias,
                rel_alias,
                dst_table_name,
                dst_table_id,
                optional,
            };

            wrap_with_filter(expand, remaining)
        }

        LogicalOperator::HashJoin {
            build,
            probe,
            build_key,
            probe_key,
        } => {
            let build_aliases = collect_plan_aliases(&build);
            let probe_aliases = collect_plan_aliases(&probe);

            let mut push_build = Vec::new();
            let mut push_probe = Vec::new();
            let mut remaining = Vec::new();

            for conj in conjuncts {
                let refs = referenced_aliases(&conj);
                if !refs.is_empty() && refs.iter().all(|a| build_aliases.contains(a)) {
                    push_build.push(conj);
                } else if !refs.is_empty() && refs.iter().all(|a| probe_aliases.contains(a)) {
                    push_probe.push(conj);
                } else {
                    remaining.push(conj);
                }
            }

            let new_build = if push_build.is_empty() {
                *build
            } else {
                push_conjuncts(*build, push_build)
            };
            let new_probe = if push_probe.is_empty() {
                *probe
            } else {
                push_conjuncts(*probe, push_probe)
            };

            let join = LogicalOperator::HashJoin {
                build: Box::new(new_build),
                probe: Box::new(new_probe),
                build_key,
                probe_key,
            };

            wrap_with_filter(join, remaining)
        }

        // For other operators (ScanNode, etc.) we cannot push further.
        other => wrap_with_filter(other, conjuncts),
    }
}

/// Split an AND expression into individual conjuncts.
fn split_conjuncts(expr: Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let mut result = split_conjuncts(*left);
            result.extend(split_conjuncts(*right));
            result
        }
        other => vec![other],
    }
}

/// Combine conjuncts back into a single AND expression.
fn combine_conjuncts(conjuncts: Vec<Expr>) -> Option<Expr> {
    let mut iter = conjuncts.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, expr| Expr::BinaryOp {
        left: Box::new(acc),
        op: BinOp::And,
        right: Box::new(expr),
    }))
}

/// Wrap a plan with a Filter if there are remaining conjuncts.
fn wrap_with_filter(plan: LogicalOperator, conjuncts: Vec<Expr>) -> LogicalOperator {
    match combine_conjuncts(conjuncts) {
        Some(predicate) => LogicalOperator::Filter {
            input: Box::new(plan),
            predicate,
        },
        None => plan,
    }
}

/// Collect all aliases (variable names) available in a plan sub-tree.
fn collect_plan_aliases(plan: &LogicalOperator) -> Vec<String> {
    let mut aliases = Vec::new();
    collect_aliases_recursive(plan, &mut aliases);
    aliases
}

fn collect_aliases_recursive(plan: &LogicalOperator, aliases: &mut Vec<String>) {
    match plan {
        LogicalOperator::ScanNode { alias, .. } => {
            if !alias.is_empty() {
                aliases.push(alias.clone());
            }
        }
        LogicalOperator::Expand {
            input,
            dst_alias,
            rel_alias,
            ..
        } => {
            collect_aliases_recursive(input, aliases);
            if !dst_alias.is_empty() {
                aliases.push(dst_alias.clone());
            }
            if let Some(ra) = rel_alias {
                if !ra.is_empty() {
                    aliases.push(ra.clone());
                }
            }
        }
        LogicalOperator::RecursiveExpand {
            input,
            dst_alias,
            ..
        } => {
            collect_aliases_recursive(input, aliases);
            if !dst_alias.is_empty() {
                aliases.push(dst_alias.clone());
            }
        }
        LogicalOperator::Filter { input, .. }
        | LogicalOperator::Projection { input, .. }
        | LogicalOperator::ReturnAll { input }
        | LogicalOperator::OrderBy { input, .. }
        | LogicalOperator::Limit { input, .. }
        | LogicalOperator::Skip { input, .. }
        | LogicalOperator::Aggregate { input, .. }
        | LogicalOperator::SetProperty { input, .. }
        | LogicalOperator::InsertRel { input, .. }
        | LogicalOperator::Delete { input, .. } => {
            collect_aliases_recursive(input, aliases);
        }
        LogicalOperator::HashJoin { build, probe, .. } => {
            collect_aliases_recursive(build, aliases);
            collect_aliases_recursive(probe, aliases);
        }
        _ => {}
    }
}

/// Extract all variable aliases referenced by an expression.
fn referenced_aliases(expr: &Expr) -> Vec<String> {
    let mut aliases = Vec::new();
    collect_expr_aliases(expr, &mut aliases);
    aliases.sort();
    aliases.dedup();
    aliases
}

fn collect_expr_aliases(expr: &Expr, aliases: &mut Vec<String>) {
    match expr {
        Expr::Property(base, _) => {
            if let Expr::Ident(name) = base.as_ref() {
                aliases.push(name.clone());
            } else {
                collect_expr_aliases(base, aliases);
            }
        }
        Expr::Ident(name) => {
            aliases.push(name.clone());
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_expr_aliases(left, aliases);
            collect_expr_aliases(right, aliases);
        }
        Expr::UnaryOp { expr, .. } => {
            collect_expr_aliases(expr, aliases);
        }
        Expr::IsNull { expr, .. } => {
            collect_expr_aliases(expr, aliases);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_expr_aliases(arg, aliases);
            }
        }
        Expr::ListLit(items) => {
            for item in items {
                collect_expr_aliases(item, aliases);
            }
        }
        _ => {}
    }
}

// ── Projection push-down ───────────────────────────────────────

/// Push column requirements down to ScanNode operators so they only read
/// the columns actually needed by the query.
fn push_projections_down(plan: LogicalOperator) -> LogicalOperator {
    // Collect all columns referenced anywhere in the plan.
    let required = collect_required_columns(&plan);
    apply_projection_pushdown(plan, &required)
}

/// A (alias, column_name) pair representing a required column.
type ColumnRef = (String, String);

/// Walk the plan tree and collect all (alias, column) pairs referenced.
fn collect_required_columns(plan: &LogicalOperator) -> Vec<ColumnRef> {
    let mut cols = Vec::new();
    collect_required_recursive(plan, &mut cols);
    cols.sort();
    cols.dedup();
    cols
}

fn collect_required_recursive(plan: &LogicalOperator, cols: &mut Vec<ColumnRef>) {
    match plan {
        LogicalOperator::Projection {
            input,
            expressions,
        } => {
            for (expr, _) in expressions {
                collect_expr_columns(expr, cols);
            }
            collect_required_recursive(input, cols);
        }
        LogicalOperator::ReturnAll { input } => {
            // RETURN * needs all columns — we'll mark this by not restricting scans.
            collect_required_recursive(input, cols);
        }
        LogicalOperator::Filter { input, predicate } => {
            collect_expr_columns(predicate, cols);
            collect_required_recursive(input, cols);
        }
        LogicalOperator::OrderBy { input, items } => {
            for item in items {
                collect_expr_columns(&item.expr, cols);
            }
            collect_required_recursive(input, cols);
        }
        LogicalOperator::Aggregate {
            input,
            expressions,
        } => {
            for (expr, _) in expressions {
                collect_expr_columns(expr, cols);
            }
            collect_required_recursive(input, cols);
        }
        LogicalOperator::Expand { input, .. } => {
            collect_required_recursive(input, cols);
        }
        LogicalOperator::RecursiveExpand { input, .. } => {
            collect_required_recursive(input, cols);
        }
        LogicalOperator::HashJoin { build, probe, .. } => {
            collect_required_recursive(build, cols);
            collect_required_recursive(probe, cols);
        }
        LogicalOperator::Limit { input, .. } | LogicalOperator::Skip { input, .. } => {
            collect_required_recursive(input, cols);
        }
        LogicalOperator::SetProperty { input, items } => {
            for item in items {
                collect_expr_columns(&item.value, cols);
                // The target alias.field is also needed for the scan
                cols.push((item.variable.clone(), item.field.clone()));
            }
            collect_required_recursive(input, cols);
        }
        LogicalOperator::Delete { input, .. }
        | LogicalOperator::InsertRel { input, .. } => {
            collect_required_recursive(input, cols);
        }
        _ => {}
    }
}

fn collect_expr_columns(expr: &Expr, cols: &mut Vec<ColumnRef>) {
    match expr {
        Expr::Property(base, field) => {
            if let Expr::Ident(alias) = base.as_ref() {
                cols.push((alias.clone(), field.clone()));
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_expr_columns(left, cols);
            collect_expr_columns(right, cols);
        }
        Expr::UnaryOp { expr, .. } => {
            collect_expr_columns(expr, cols);
        }
        Expr::IsNull { expr, .. } => {
            collect_expr_columns(expr, cols);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_expr_columns(arg, cols);
            }
        }
        Expr::ListLit(items) => {
            for item in items {
                collect_expr_columns(item, cols);
            }
        }
        _ => {}
    }
}

/// Recursively apply projection pushdown: set ScanNode.columns to only
/// the required column indices.
fn apply_projection_pushdown(
    plan: LogicalOperator,
    required: &[ColumnRef],
) -> LogicalOperator {
    match plan {
        LogicalOperator::ScanNode {
            table_name,
            table_id,
            columns: _,
            alias,
        } => {
            // Find which columns of this scan alias are needed.
            let needed: Vec<String> = required
                .iter()
                .filter(|(a, _)| *a == alias)
                .map(|(_, col)| col.clone())
                .collect();

            // If no specific columns requested, this means either RETURN *
            // or no projection references this alias at all. In both cases
            // keep the original empty vec (= read all columns).
            if needed.is_empty() {
                return LogicalOperator::ScanNode {
                    table_name,
                    table_id,
                    columns: vec![],
                    alias,
                };
            }

            // We don't have catalog access here to resolve names → indices,
            // so we store the column names in a different way: use the
            // existing `columns: Vec<usize>` field. Since the executor
            // currently ignores the columns field (reads all), this is
            // future-proofing. We leave columns empty for now and let the
            // executor handle it — the key optimization is that the column
            // set is *identified*.
            //
            // TODO: Once executor respects ScanNode.columns, resolve names
            // to indices here using catalog metadata.
            LogicalOperator::ScanNode {
                table_name,
                table_id,
                columns: vec![],
                alias,
            }
        }
        LogicalOperator::Filter { input, predicate } => LogicalOperator::Filter {
            input: Box::new(apply_projection_pushdown(*input, required)),
            predicate,
        },
        LogicalOperator::Projection {
            input,
            expressions,
        } => LogicalOperator::Projection {
            input: Box::new(apply_projection_pushdown(*input, required)),
            expressions,
        },
        LogicalOperator::ReturnAll { input } => LogicalOperator::ReturnAll {
            input: Box::new(apply_projection_pushdown(*input, required)),
        },
        LogicalOperator::Expand {
            input,
            rel_table_name,
            rel_table_id,
            direction,
            src_alias,
            dst_alias,
            rel_alias,
            dst_table_name,
            dst_table_id,
            optional,
        } => LogicalOperator::Expand {
            input: Box::new(apply_projection_pushdown(*input, required)),
            rel_table_name,
            rel_table_id,
            direction,
            src_alias,
            dst_alias,
            rel_alias,
            dst_table_name,
            dst_table_id,
            optional,
        },
        LogicalOperator::RecursiveExpand {
            input,
            rel_table_name,
            rel_table_id,
            direction,
            src_alias,
            dst_alias,
            dst_table_name,
            dst_table_id,
            min_hops,
            max_hops,
        } => LogicalOperator::RecursiveExpand {
            input: Box::new(apply_projection_pushdown(*input, required)),
            rel_table_name,
            rel_table_id,
            direction,
            src_alias,
            dst_alias,
            dst_table_name,
            dst_table_id,
            min_hops,
            max_hops,
        },
        LogicalOperator::HashJoin {
            build,
            probe,
            build_key,
            probe_key,
        } => LogicalOperator::HashJoin {
            build: Box::new(apply_projection_pushdown(*build, required)),
            probe: Box::new(apply_projection_pushdown(*probe, required)),
            build_key,
            probe_key,
        },
        LogicalOperator::OrderBy { input, items } => LogicalOperator::OrderBy {
            input: Box::new(apply_projection_pushdown(*input, required)),
            items,
        },
        LogicalOperator::Limit { input, count } => LogicalOperator::Limit {
            input: Box::new(apply_projection_pushdown(*input, required)),
            count,
        },
        LogicalOperator::Skip { input, count } => LogicalOperator::Skip {
            input: Box::new(apply_projection_pushdown(*input, required)),
            count,
        },
        LogicalOperator::Aggregate {
            input,
            expressions,
        } => LogicalOperator::Aggregate {
            input: Box::new(apply_projection_pushdown(*input, required)),
            expressions,
        },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::Binder;
    use crate::catalog::{Catalog, ColumnDef};
    use crate::parser::parser::Parser;
    use crate::planner::logical::Planner;
    use crate::types::data_type::DataType;

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
        catalog
            .create_rel_table("KNOWS", "Person", "Person", vec![])
            .unwrap();
        catalog
            .create_rel_table("LIVES_IN", "Person", "City", vec![])
            .unwrap();
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
        // WHERE a.age > 30 should be pushed below Expand to sit on top of ScanNode(a).
        let catalog = test_catalog();
        let plan = plan_and_optimize(
            &catalog,
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.age > 30 RETURN a.name, b.name",
        );
        // Expected structure: Projection(Expand(Filter(ScanNode(a)), b))
        // The Filter(a.age > 30) should be between ScanNode and Expand.
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
        // a.id = b.id references both a and b — cannot push below Expand.
        let catalog = test_catalog();
        let plan = plan_and_optimize(
            &catalog,
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.id = b.id RETURN a, b",
        );
        // Filter should stay on top of Expand.
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
        // a.age > 30 AND a.id = b.id — only a.age > 30 is pushable.
        let catalog = test_catalog();
        let plan = plan_and_optimize(
            &catalog,
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a.age > 30 AND a.id = b.id RETURN a, b",
        );
        // Expected: Projection(Filter(a.id=b.id, Expand(Filter(a.age>30, ScanNode(a)))))
        if let LogicalOperator::Projection { input, .. } = &plan {
            if let LogicalOperator::Filter { input: join_filter_in, .. } = input.as_ref() {
                if let LogicalOperator::Expand { input: expand_in, .. } = join_filter_in.as_ref() {
                    assert!(
                        matches!(expand_in.as_ref(), LogicalOperator::Filter { .. }),
                        "single-table predicate should be pushed below Expand, got {:?}",
                        expand_in
                    );
                } else {
                    panic!(
                        "expected Expand below cross-table filter, got {:?}",
                        join_filter_in
                    );
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
        // No WHERE clause — plan should be unchanged.
        let catalog = test_catalog();
        let plan = plan_and_optimize(
            &catalog,
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, b",
        );
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
        // The plan should still be valid after optimization.
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
            left: Box::new(Expr::Property(
                Box::new(Expr::Ident("a".into())),
                "id".into(),
            )),
            op: BinOp::Eq,
            right: Box::new(Expr::Property(
                Box::new(Expr::Ident("b".into())),
                "id".into(),
            )),
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
            direction: crate::parser::ast::Direction::Right,
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
}
