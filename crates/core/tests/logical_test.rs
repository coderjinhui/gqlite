use gqlite_core::binder::Binder;
use gqlite_core::catalog::{Catalog, ColumnDef};
use gqlite_core::parser::parser::Parser;
use gqlite_core::planner::logical::{LogicalOperator, Planner};
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
    catalog.create_rel_table("KNOWS", "Person", "Person", vec![]).unwrap();
    catalog
}

fn plan_query(catalog: &Catalog, query: &str) -> LogicalOperator {
    let stmt = Parser::parse_query(query).unwrap();
    let mut binder = Binder::new(catalog);
    let bound = binder.bind(&stmt).unwrap();
    let planner = Planner::new(catalog);
    planner.plan(&bound).unwrap()
}

#[test]
fn scan_node_projection() {
    let catalog = test_catalog();
    let plan = plan_query(&catalog, "MATCH (n:Person) RETURN n.name");
    assert!(matches!(plan, LogicalOperator::Projection { .. }));
    if let LogicalOperator::Projection { input, .. } = &plan {
        assert!(matches!(**input, LogicalOperator::ScanNode { .. }));
    }
}

#[test]
fn scan_with_filter() {
    let catalog = test_catalog();
    let plan = plan_query(&catalog, "MATCH (n:Person) WHERE n.age > 30 RETURN n");
    match &plan {
        LogicalOperator::Projection { input, .. } => {
            assert!(matches!(**input, LogicalOperator::Filter { .. }));
        }
        _ => panic!("expected Projection, got {:?}", plan),
    }
}

#[test]
fn scan_with_expand() {
    let catalog = test_catalog();
    let plan = plan_query(&catalog, "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, b");
    match &plan {
        LogicalOperator::Projection { input, .. } => {
            assert!(matches!(**input, LogicalOperator::Expand { .. }));
        }
        _ => panic!("expected Projection, got {:?}", plan),
    }
}

#[test]
fn create_node() {
    let catalog = test_catalog();
    let plan = plan_query(&catalog, "CREATE (n:Person {id: 1, name: 'Alice'})");
    assert!(matches!(plan, LogicalOperator::InsertNode { .. }));
}

#[test]
fn ddl_create_node_table() {
    let catalog = Catalog::new();
    let stmt =
        Parser::parse_query("CREATE NODE TABLE Movie (id INT64, title STRING, PRIMARY KEY (id))")
            .unwrap();
    let mut binder = Binder::new(&catalog);
    let bound = binder.bind(&stmt).unwrap();
    let planner = Planner::new(&catalog);
    let plan = planner.plan(&bound).unwrap();
    assert!(matches!(plan, LogicalOperator::CreateNodeTable { .. }));
}
