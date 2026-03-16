use gqlite_core::Database;
use gqlite_core::types::value::Value;

#[test]
fn list_comprehension_map_only() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    let result = db
        .query("MATCH (n:N) RETURN [x IN [1, 2, 3] | x * 2] AS doubled")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let val = &result.rows()[0].values[0];
    match val {
        Value::List(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Value::Int(2));
            assert_eq!(items[1], Value::Int(4));
            assert_eq!(items[2], Value::Int(6));
        }
        _ => panic!("expected list, got {:?}", val),
    }
}

#[test]
fn list_comprehension_filter_only() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    let result = db
        .query("MATCH (n:N) RETURN [x IN [1, 2, 3, 4, 5] WHERE x > 3] AS big")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let val = &result.rows()[0].values[0];
    match val {
        Value::List(items) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], Value::Int(4));
            assert_eq!(items[1], Value::Int(5));
        }
        _ => panic!("expected list, got {:?}", val),
    }
}

#[test]
fn list_comprehension_filter_and_map() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    let result = db
        .query("MATCH (n:N) RETURN [x IN [1, 2, 3, 4, 5] WHERE x > 2 | x * 10] AS r")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let val = &result.rows()[0].values[0];
    match val {
        Value::List(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Value::Int(30));
            assert_eq!(items[1], Value::Int(40));
            assert_eq!(items[2], Value::Int(50));
        }
        _ => panic!("expected list, got {:?}", val),
    }
}

#[test]
fn list_comprehension_identity() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    // No filter, no map — identity
    let result = db
        .query("MATCH (n:N) RETURN [x IN [10, 20, 30]] AS r")
        .unwrap();
    let val = &result.rows()[0].values[0];
    match val {
        Value::List(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Value::Int(10));
            assert_eq!(items[1], Value::Int(20));
            assert_eq!(items[2], Value::Int(30));
        }
        _ => panic!("expected list, got {:?}", val),
    }
}

#[test]
fn list_comprehension_with_property() {
    // Use a list comprehension that references a node property
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, val INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE (n:N {id: 1, val: 10})").unwrap();
    let result = db
        .query("MATCH (n:N) RETURN [x IN [1, 2, 3] | x + n.val] AS r")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let val = &result.rows()[0].values[0];
    match val {
        Value::List(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Value::Int(11));
            assert_eq!(items[1], Value::Int(12));
            assert_eq!(items[2], Value::Int(13));
        }
        _ => panic!("expected list, got {:?}", val),
    }
}

#[test]
fn list_comprehension_null_list() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    let result = db
        .query("MATCH (n:N) RETURN [x IN null | x * 2] AS r")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let val = &result.rows()[0].values[0];
    assert_eq!(*val, Value::Null);
}

#[test]
fn list_comprehension_empty_list() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    let result = db
        .query("MATCH (n:N) RETURN [x IN [] | x * 2] AS r")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let val = &result.rows()[0].values[0];
    match val {
        Value::List(items) => {
            assert_eq!(items.len(), 0);
        }
        _ => panic!("expected list, got {:?}", val),
    }
}
