use gqlite_core::executor::pipeline::split_into_pipelines;
use gqlite_core::parser::ast::Expr;
use gqlite_core::planner::logical::JoinKey;
use gqlite_core::planner::physical::PhysicalPlan;

#[test]
fn single_scan_is_one_pipeline() {
    let plan = PhysicalPlan::SeqScan {
        table_name: "Person".into(),
        table_id: 0,
        columns: vec![0, 1],
        alias: "n".into(),
    };
    let graph = split_into_pipelines(&plan);
    assert_eq!(graph.pipelines.len(), 1);
    assert!(graph.pipelines[0].depends_on.is_empty());
}

#[test]
fn filter_projection_is_one_pipeline() {
    let scan = PhysicalPlan::SeqScan {
        table_name: "Person".into(),
        table_id: 0,
        columns: vec![0, 1],
        alias: "n".into(),
    };
    let plan = PhysicalPlan::Projection {
        input: Box::new(PhysicalPlan::Filter {
            input: Box::new(scan),
            predicate: Expr::BoolLit(true),
        }),
        expressions: vec![],
    };
    let graph = split_into_pipelines(&plan);
    assert_eq!(graph.pipelines.len(), 1);
}

#[test]
fn hash_join_creates_three_pipelines() {
    let build = PhysicalPlan::SeqScan {
        table_name: "A".into(),
        table_id: 0,
        columns: vec![0],
        alias: "a".into(),
    };
    let probe = PhysicalPlan::SeqScan {
        table_name: "B".into(),
        table_id: 1,
        columns: vec![0],
        alias: "b".into(),
    };
    let plan = PhysicalPlan::HashJoin {
        build: Box::new(build),
        probe: Box::new(probe),
        build_key: JoinKey { alias: "a".into(), column: "id".into() },
        probe_key: JoinKey { alias: "b".into(), column: "id".into() },
    };
    let graph = split_into_pipelines(&plan);
    // build pipeline, probe pipeline, join pipeline
    assert_eq!(graph.pipelines.len(), 3);

    // Join pipeline depends on both build and probe
    let join = &graph.pipelines[2];
    assert_eq!(join.depends_on.len(), 2);
}

#[test]
fn execution_order_respects_dependencies() {
    let build = PhysicalPlan::SeqScan {
        table_name: "A".into(),
        table_id: 0,
        columns: vec![0],
        alias: "a".into(),
    };
    let probe = PhysicalPlan::SeqScan {
        table_name: "B".into(),
        table_id: 1,
        columns: vec![0],
        alias: "b".into(),
    };
    let plan = PhysicalPlan::HashJoin {
        build: Box::new(build),
        probe: Box::new(probe),
        build_key: JoinKey { alias: "a".into(), column: "id".into() },
        probe_key: JoinKey { alias: "b".into(), column: "id".into() },
    };
    let graph = split_into_pipelines(&plan);
    let order = graph.execution_order();

    // The join pipeline (id=2) must come after build (id=0) and probe (id=1)
    let join_pos = order.iter().position(|&id| id == 2).unwrap();
    let build_pos = order.iter().position(|&id| id == 0).unwrap();
    let probe_pos = order.iter().position(|&id| id == 1).unwrap();
    assert!(build_pos < join_pos);
    assert!(probe_pos < join_pos);
}

#[test]
fn orderby_creates_two_pipelines() {
    let scan = PhysicalPlan::SeqScan {
        table_name: "Person".into(),
        table_id: 0,
        columns: vec![0, 1],
        alias: "n".into(),
    };
    let plan = PhysicalPlan::OrderBy {
        input: Box::new(scan),
        items: vec![],
    };
    let graph = split_into_pipelines(&plan);
    assert_eq!(graph.pipelines.len(), 2);
    assert_eq!(graph.pipelines[1].depends_on, vec![0]);
}

#[test]
fn aggregate_creates_two_pipelines() {
    let scan = PhysicalPlan::SeqScan {
        table_name: "Person".into(),
        table_id: 0,
        columns: vec![0, 1],
        alias: "n".into(),
    };
    let plan = PhysicalPlan::Aggregate {
        input: Box::new(scan),
        expressions: vec![],
    };
    let graph = split_into_pipelines(&plan);
    assert_eq!(graph.pipelines.len(), 2);
    assert_eq!(graph.pipelines[1].depends_on, vec![0]);
}
