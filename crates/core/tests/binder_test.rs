use gqlite_core::binder::{Binder, BoundStatement};
use gqlite_core::catalog::{Catalog, ColumnDef};
use gqlite_core::parser::parser::Parser;
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

#[test]
fn bind_simple_match_return() {
    let catalog = test_catalog();
    let stmt = Parser::parse_query("MATCH (n:Person) RETURN n.name").unwrap();
    let mut binder = Binder::new(&catalog);
    let bound = binder.bind(&stmt).unwrap();
    assert!(matches!(bound, BoundStatement::Query(_)));
}

#[test]
fn bind_unknown_label() {
    let catalog = test_catalog();
    let stmt = Parser::parse_query("MATCH (n:Unknown) RETURN n").unwrap();
    let mut binder = Binder::new(&catalog);
    assert!(binder.bind(&stmt).is_err());
}

#[test]
fn bind_undefined_variable() {
    let catalog = test_catalog();
    let stmt = Parser::parse_query("MATCH (n:Person) RETURN x.name").unwrap();
    let mut binder = Binder::new(&catalog);
    assert!(binder.bind(&stmt).is_err());
}

#[test]
fn bind_relationship() {
    let catalog = test_catalog();
    let stmt = Parser::parse_query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, b").unwrap();
    let mut binder = Binder::new(&catalog);
    let bound = binder.bind(&stmt).unwrap();
    assert!(matches!(bound, BoundStatement::Query(_)));
}

#[test]
fn bind_unknown_rel_table() {
    let catalog = test_catalog();
    let stmt = Parser::parse_query("MATCH (a:Person)-[r:LIKES]->(b:Person) RETURN a").unwrap();
    let mut binder = Binder::new(&catalog);
    assert!(binder.bind(&stmt).is_err());
}

#[test]
fn bind_ddl_create_node_table() {
    let catalog = Catalog::new();
    let stmt =
        Parser::parse_query("CREATE NODE TABLE Movie (id INT64, title STRING, PRIMARY KEY (id))")
            .unwrap();
    let mut binder = Binder::new(&catalog);
    let bound = binder.bind(&stmt).unwrap();
    assert!(matches!(bound, BoundStatement::CreateNodeTable { .. }));
}

#[test]
fn bind_ddl_bad_pk() {
    let catalog = Catalog::new();
    let stmt = Parser::parse_query("CREATE NODE TABLE Movie (id INT64, PRIMARY KEY (nonexistent))")
        .unwrap();
    let mut binder = Binder::new(&catalog);
    assert!(binder.bind(&stmt).is_err());
}
