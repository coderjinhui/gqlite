use std::collections::HashMap;
use gqlite_core::Database;
use gqlite_core::executor::engine::Engine;
use gqlite_core::types::value::Value;

#[test]
fn ddl_create_and_drop() {
    let db = Database::in_memory();
    db.execute(
        "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))",
    )
    .unwrap();
    let result = db
        .execute("CREATE NODE TABLE Movie (id INT64, title STRING, PRIMARY KEY (id))")
        .unwrap();
    assert!(result.is_empty());

    db.execute("DROP TABLE Movie").unwrap();
}

#[test]
fn insert_and_scan() {
    let db = Database::in_memory();
    db.execute(
        "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))",
    )
    .unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice'})")
        .unwrap();
    db.execute("CREATE (n:Person {id: 2, name: 'Bob'})")
        .unwrap();

    let result = db.query("MATCH (n:Person) RETURN n.name").unwrap();
    assert_eq!(result.num_rows(), 2);
    let names: Vec<&str> = result
        .rows()
        .iter()
        .map(|r| r.get_string(0).unwrap())
        .collect();
    assert!(names.contains(&"Alice"));
    assert!(names.contains(&"Bob"));
}

#[test]
fn filter_predicate() {
    let db = Database::in_memory();
    db.execute(
        "CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))",
    )
    .unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice', age: 30})")
        .unwrap();
    db.execute("CREATE (n:Person {id: 2, name: 'Bob', age: 25})")
        .unwrap();
    db.execute("CREATE (n:Person {id: 3, name: 'Charlie', age: 35})")
        .unwrap();

    let result = db
        .query("MATCH (n:Person) WHERE n.age > 28 RETURN n.name")
        .unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[test]
fn return_all() {
    let db = Database::in_memory();
    db.execute(
        "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))",
    )
    .unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice'})")
        .unwrap();

    let result = db.query("MATCH (n:Person) RETURN *").unwrap();
    assert_eq!(result.num_rows(), 1);
    // Should include all columns: n, n.id, n.name
    assert!(result.column_names().len() >= 3);
}

#[test]
fn relationship_expand() {
    let db = Database::in_memory();
    db.execute(
        "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))",
    )
    .unwrap();
    db.execute("CREATE REL TABLE KNOWS (FROM Person TO Person)")
        .unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice'})")
        .unwrap();
    db.execute("CREATE (n:Person {id: 2, name: 'Bob'})")
        .unwrap();

    // Create relationship
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[r:KNOWS]->(b)",
    )
    .unwrap();

    // Query relationships
    let result = db
        .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_string(0), Some("Alice"));
    assert_eq!(result.rows()[0].get_string(1), Some("Bob"));
}

#[test]
fn set_property() {
    let db = Database::in_memory();
    db.execute(
        "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))",
    )
    .unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice'})")
        .unwrap();

    db.execute("MATCH (n:Person) WHERE n.id = 1 SET n.name = 'Updated'")
        .unwrap();

    let result = db
        .query("MATCH (n:Person) WHERE n.id = 1 RETURN n.name")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_string(0), Some("Updated"));
}

#[test]
fn delete_node() {
    let db = Database::in_memory();
    db.execute(
        "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))",
    )
    .unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice'})")
        .unwrap();
    db.execute("CREATE (n:Person {id: 2, name: 'Bob'})")
        .unwrap();

    db.execute("MATCH (n:Person) WHERE n.id = 1 DELETE n")
        .unwrap();

    let result = db.query("MATCH (n:Person) RETURN n.name").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_string(0), Some("Bob"));
}

#[test]
fn expression_arithmetic() {
    let db = Database::in_memory();
    db.execute(
        "CREATE NODE TABLE Num (id INT64, val INT64, PRIMARY KEY (id))",
    )
    .unwrap();
    db.execute("CREATE (n:Num {id: 1, val: 10})").unwrap();

    let result = db
        .query("MATCH (n:Num) RETURN n.val + 5")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_int(0), Some(15));
}

#[test]
fn scalar_function_in_projection() {
    let db = Database::in_memory();
    db.execute(
        "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))",
    )
    .unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice'})")
        .unwrap();

    let result = db
        .query("MATCH (n:Person) RETURN upper(n.name)")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_string(0), Some("ALICE"));
}

// ── ORDER BY tests ──────────────────────────────────────────

fn setup_persons(db: &Database) {
    db.execute(
        "CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))",
    )
    .unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice', age: 30})")
        .unwrap();
    db.execute("CREATE (n:Person {id: 2, name: 'Bob', age: 25})")
        .unwrap();
    db.execute("CREATE (n:Person {id: 3, name: 'Charlie', age: 35})")
        .unwrap();
    db.execute("CREATE (n:Person {id: 4, name: 'Diana', age: 28})")
        .unwrap();
}

#[test]
fn order_by_asc() {
    let db = Database::in_memory();
    setup_persons(&db);

    let result = db
        .query("MATCH (n:Person) RETURN n.name ORDER BY n.age")
        .unwrap();
    assert_eq!(result.num_rows(), 4);
    assert_eq!(result.rows()[0].get_string(0), Some("Bob"));
    assert_eq!(result.rows()[1].get_string(0), Some("Diana"));
    assert_eq!(result.rows()[2].get_string(0), Some("Alice"));
    assert_eq!(result.rows()[3].get_string(0), Some("Charlie"));
}

#[test]
fn order_by_desc() {
    let db = Database::in_memory();
    setup_persons(&db);

    let result = db
        .query("MATCH (n:Person) RETURN n.name ORDER BY n.age DESC")
        .unwrap();
    assert_eq!(result.num_rows(), 4);
    assert_eq!(result.rows()[0].get_string(0), Some("Charlie"));
    assert_eq!(result.rows()[1].get_string(0), Some("Alice"));
    assert_eq!(result.rows()[2].get_string(0), Some("Diana"));
    assert_eq!(result.rows()[3].get_string(0), Some("Bob"));
}

#[test]
fn order_by_string() {
    let db = Database::in_memory();
    setup_persons(&db);

    let result = db
        .query("MATCH (n:Person) RETURN n.name ORDER BY n.name")
        .unwrap();
    assert_eq!(result.num_rows(), 4);
    assert_eq!(result.rows()[0].get_string(0), Some("Alice"));
    assert_eq!(result.rows()[1].get_string(0), Some("Bob"));
    assert_eq!(result.rows()[2].get_string(0), Some("Charlie"));
    assert_eq!(result.rows()[3].get_string(0), Some("Diana"));
}

// ── LIMIT tests ─────────────────────────────────────────────

#[test]
fn limit_results() {
    let db = Database::in_memory();
    setup_persons(&db);

    let result = db
        .query("MATCH (n:Person) RETURN n.name LIMIT 2")
        .unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[test]
fn limit_larger_than_result() {
    let db = Database::in_memory();
    setup_persons(&db);

    let result = db
        .query("MATCH (n:Person) RETURN n.name LIMIT 100")
        .unwrap();
    assert_eq!(result.num_rows(), 4);
}

// ── SKIP tests ──────────────────────────────────────────────

#[test]
fn skip_results() {
    let db = Database::in_memory();
    setup_persons(&db);

    let result = db
        .query("MATCH (n:Person) RETURN n.name ORDER BY n.age SKIP 2")
        .unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.rows()[0].get_string(0), Some("Alice"));
    assert_eq!(result.rows()[1].get_string(0), Some("Charlie"));
}

#[test]
fn skip_and_limit() {
    let db = Database::in_memory();
    setup_persons(&db);

    let result = db
        .query("MATCH (n:Person) RETURN n.name ORDER BY n.age SKIP 1 LIMIT 2")
        .unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.rows()[0].get_string(0), Some("Diana"));
    assert_eq!(result.rows()[1].get_string(0), Some("Alice"));
}

// ── Aggregate tests ─────────────────────────────────────────

#[test]
fn count_star() {
    let db = Database::in_memory();
    setup_persons(&db);

    let result = db
        .query("MATCH (n:Person) RETURN count(*)")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_int(0), Some(4));
}

#[test]
fn count_expression() {
    let db = Database::in_memory();
    setup_persons(&db);

    let result = db
        .query("MATCH (n:Person) RETURN count(n)")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_int(0), Some(4));
}

#[test]
fn sum_and_avg() {
    let db = Database::in_memory();
    setup_persons(&db);

    let result = db
        .query("MATCH (n:Person) RETURN sum(n.age), avg(n.age)")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    // sum = 30 + 25 + 35 + 28 = 118
    assert_eq!(result.rows()[0].get_int(0), Some(118));
    // avg = 118 / 4 = 29.5
    assert_eq!(result.rows()[0].get_float(1), Some(29.5));
}

#[test]
fn min_and_max() {
    let db = Database::in_memory();
    setup_persons(&db);

    let result = db
        .query("MATCH (n:Person) RETURN min(n.age), max(n.age)")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_int(0), Some(25));
    assert_eq!(result.rows()[0].get_int(1), Some(35));
}

#[test]
fn group_by_with_count() {
    let db = Database::in_memory();
    db.execute(
        "CREATE NODE TABLE Person (id INT64, name STRING, city STRING, PRIMARY KEY (id))",
    )
    .unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice', city: 'NYC'})")
        .unwrap();
    db.execute("CREATE (n:Person {id: 2, name: 'Bob', city: 'LA'})")
        .unwrap();
    db.execute("CREATE (n:Person {id: 3, name: 'Charlie', city: 'NYC'})")
        .unwrap();
    db.execute("CREATE (n:Person {id: 4, name: 'Diana', city: 'LA'})")
        .unwrap();
    db.execute("CREATE (n:Person {id: 5, name: 'Eve', city: 'NYC'})")
        .unwrap();

    let result = db
        .query("MATCH (n:Person) RETURN n.city, count(n)")
        .unwrap();
    assert_eq!(result.num_rows(), 2);

    // Find which row is NYC and which is LA
    let rows = result.rows();
    for row in rows {
        let city = row.get_string(0).unwrap();
        let count = row.get_int(1).unwrap();
        match city {
            "NYC" => assert_eq!(count, 3),
            "LA" => assert_eq!(count, 2),
            _ => panic!("unexpected city: {}", city),
        }
    }
}

#[test]
fn collect_aggregate() {
    let db = Database::in_memory();
    db.execute(
        "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))",
    )
    .unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {id: 2, name: 'Bob'})").unwrap();

    let result = db
        .query("MATCH (n:Person) RETURN collect(n.name)")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    // The result should be a list
    let val = &result.rows()[0].values[0];
    match val {
        Value::List(items) => {
            assert_eq!(items.len(), 2);
        }
        _ => panic!("expected List, got {:?}", val),
    }
}

// ── OPTIONAL MATCH / UNION / UNWIND / MERGE tests ──────────

#[test]
fn optional_match_with_no_relationship() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
        .unwrap();
    db.execute("CREATE REL TABLE Knows (FROM Person TO Person)")
        .unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {id: 2, name: 'Bob'})").unwrap();
    // Only Alice knows Bob, not vice versa
    db.execute("MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:Knows]->(b)")
        .unwrap();

    // OPTIONAL MATCH: Bob has no outgoing KNOWS, should still appear with NULLs
    let result = db
        .query("MATCH (a:Person) OPTIONAL MATCH (a)-[:Knows]->(b:Person) RETURN a.name, b.name ORDER BY a.name")
        .unwrap();
    assert_eq!(result.num_rows(), 2);
    // Alice -> Bob
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "Alice");
    assert_eq!(result.rows()[0].get_string(1).unwrap(), "Bob");
    // Bob -> NULL
    assert_eq!(result.rows()[1].get_string(0).unwrap(), "Bob");
    assert!(result.rows()[1].values[1].is_null());
}

#[test]
fn union_all_combines_results() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
        .unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {id: 2, name: 'Bob'})").unwrap();

    let result = db
        .query("MATCH (a:Person) RETURN a.name UNION ALL MATCH (b:Person) RETURN b.name")
        .unwrap();
    // 2 + 2 = 4 rows (duplicates preserved)
    assert_eq!(result.num_rows(), 4);
}

#[test]
fn union_distinct_deduplicates() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
        .unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {id: 2, name: 'Bob'})").unwrap();

    let result = db
        .query("MATCH (a:Person) RETURN a.name UNION MATCH (b:Person) RETURN b.name")
        .unwrap();
    // Deduplicated: 2 unique names
    assert_eq!(result.num_rows(), 2);
}

#[test]
fn unwind_list_literal() {
    let db = Database::in_memory();
    let result = db.query("UNWIND [1, 2, 3] AS x RETURN x").unwrap();
    assert_eq!(result.num_rows(), 3);
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 1);
    assert_eq!(result.rows()[1].get_int(0).unwrap(), 2);
    assert_eq!(result.rows()[2].get_int(0).unwrap(), 3);
}

#[test]
fn merge_creates_when_not_exists() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
        .unwrap();

    db.execute("MERGE (n:Person {id: 1, name: 'Alice'}) ON CREATE SET n.age = 25")
        .unwrap();

    let result = db.query("MATCH (n:Person) RETURN n.name, n.age").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "Alice");
    assert_eq!(result.rows()[0].get_int(1).unwrap(), 25);
}

#[test]
fn merge_matches_when_exists() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
        .unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice', age: 25})").unwrap();

    db.execute("MERGE (n:Person {id: 1, name: 'Alice'}) ON MATCH SET n.age = 30")
        .unwrap();

    let result = db.query("MATCH (n:Person) RETURN n.name, n.age").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "Alice");
    assert_eq!(result.rows()[0].get_int(1).unwrap(), 30);
}

#[test]
fn serial_auto_increment() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id SERIAL, name STRING, PRIMARY KEY (id))")
        .unwrap();

    db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (n:Person {name: 'Charlie'})").unwrap();

    let result = db.query("MATCH (n:Person) RETURN n.id, n.name ORDER BY n.id ASC").unwrap();
    assert_eq!(result.num_rows(), 3);
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 0);
    assert_eq!(result.rows()[0].get_string(1).unwrap(), "Alice");
    assert_eq!(result.rows()[1].get_int(0).unwrap(), 1);
    assert_eq!(result.rows()[1].get_string(1).unwrap(), "Bob");
    assert_eq!(result.rows()[2].get_int(0).unwrap(), 2);
    assert_eq!(result.rows()[2].get_string(1).unwrap(), "Charlie");
}

#[test]
fn serial_with_explicit_value() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id SERIAL, name STRING, PRIMARY KEY (id))")
        .unwrap();

    // Explicitly provide id — should use the provided value
    db.execute("CREATE (n:Person {id: 100, name: 'Alice'})").unwrap();
    // Next auto should still start from counter (0), not from 100
    db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();

    let result = db.query("MATCH (n:Person) RETURN n.id, n.name ORDER BY n.id ASC").unwrap();
    assert_eq!(result.num_rows(), 2);
    // Bob gets id=0 (auto), Alice has id=100 (explicit)
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 0);
    assert_eq!(result.rows()[0].get_string(1).unwrap(), "Bob");
    assert_eq!(result.rows()[1].get_int(0).unwrap(), 100);
    assert_eq!(result.rows()[1].get_string(1).unwrap(), "Alice");
}

#[test]
fn alter_table_add_column() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
        .unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();

    // Add a new column
    db.execute("ALTER TABLE Person ADD age INT64").unwrap();

    // New column should be NULL for existing rows
    let result = db.query("MATCH (n:Person) RETURN n.id, n.name, n.age").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 1);
    assert_eq!(result.rows()[0].get_string(1).unwrap(), "Alice");
    assert!(result.rows()[0].values[2].is_null());
}

#[test]
fn alter_table_drop_column() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
        .unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice', age: 30})").unwrap();

    // Drop the age column
    db.execute("ALTER TABLE Person DROP COLUMN age").unwrap();

    // Should still be able to query remaining columns
    let result = db.query("MATCH (n:Person) RETURN n.id, n.name").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 1);
    assert_eq!(result.rows()[0].get_string(1).unwrap(), "Alice");
}

#[test]
fn alter_table_rename_table() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
        .unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();

    db.execute("ALTER TABLE Person RENAME TO People").unwrap();

    // Old name should fail
    let result = db.query("MATCH (n:Person) RETURN n.name");
    assert!(result.is_err());

    // New name should work
    let result = db.query("MATCH (n:People) RETURN n.name").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "Alice");
}

#[test]
fn alter_table_rename_column() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
        .unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();

    db.execute("ALTER TABLE Person RENAME COLUMN name TO fullname").unwrap();

    // Old column name should not return data
    let result = db.query("MATCH (n:Person) RETURN n.fullname").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "Alice");
}

#[test]
fn alter_table_drop_pk_column_fails() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
        .unwrap();

    let result = db.execute("ALTER TABLE Person DROP COLUMN id");
    assert!(result.is_err());
}

#[test]
fn copy_from_csv() {
    use std::io::Write;

    let dir = std::env::temp_dir().join("gqlite_test_csv");
    std::fs::create_dir_all(&dir).ok();
    let csv_path = dir.join("persons.csv");

    // Write a test CSV file
    {
        let mut f = std::fs::File::create(&csv_path).unwrap();
        writeln!(f, "id,name,age").unwrap();
        writeln!(f, "1,Alice,30").unwrap();
        writeln!(f, "2,Bob,25").unwrap();
        writeln!(f, "3,Charlie,35").unwrap();
    }

    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
        .unwrap();

    let csv_str = csv_path.to_str().unwrap();
    db.execute(&format!("COPY Person FROM '{}'", csv_str)).unwrap();

    let result = db.query("MATCH (n:Person) RETURN n.id, n.name, n.age ORDER BY n.id ASC").unwrap();
    assert_eq!(result.num_rows(), 3);
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 1);
    assert_eq!(result.rows()[0].get_string(1).unwrap(), "Alice");
    assert_eq!(result.rows()[0].get_int(2).unwrap(), 30);
    assert_eq!(result.rows()[2].get_int(0).unwrap(), 3);
    assert_eq!(result.rows()[2].get_string(1).unwrap(), "Charlie");

    std::fs::remove_file(&csv_path).ok();
}

#[test]
fn copy_to_csv_table() {
    let dir = std::env::temp_dir().join("gqlite_test_csv");
    std::fs::create_dir_all(&dir).ok();
    let csv_path = dir.join("export.csv");

    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
        .unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {id: 2, name: 'Bob'})").unwrap();

    let csv_str = csv_path.to_str().unwrap();
    db.execute(&format!("COPY Person TO '{}'", csv_str)).unwrap();

    let content = std::fs::read_to_string(&csv_path).unwrap();
    let lines: Vec<&str> = content.trim().lines().collect();
    assert_eq!(lines[0], "id,name");
    assert!(lines.len() >= 3); // header + 2 rows

    std::fs::remove_file(&csv_path).ok();
}

#[test]
fn copy_from_csv_with_nulls() {
    use std::io::Write;

    let dir = std::env::temp_dir().join("gqlite_test_csv");
    std::fs::create_dir_all(&dir).ok();
    let csv_path = dir.join("nulls.csv");

    {
        let mut f = std::fs::File::create(&csv_path).unwrap();
        writeln!(f, "id,name,age").unwrap();
        writeln!(f, "1,Alice,30").unwrap();
        writeln!(f, "2,,NULL").unwrap(); // empty name, NULL age
    }

    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
        .unwrap();

    let csv_str = csv_path.to_str().unwrap();
    db.execute(&format!("COPY Person FROM '{}'", csv_str)).unwrap();

    let result = db.query("MATCH (n:Person) RETURN n.id, n.name, n.age ORDER BY n.id ASC").unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.rows()[1].get_int(0).unwrap(), 2);
    assert!(result.rows()[1].values[1].is_null()); // empty name → NULL
    assert!(result.rows()[1].values[2].is_null()); // "NULL" → NULL

    std::fs::remove_file(&csv_path).ok();
}

#[test]
fn copy_from_tsv() {
    use std::io::Write;

    let dir = std::env::temp_dir().join("gqlite_test_csv");
    std::fs::create_dir_all(&dir).ok();
    let tsv_path = dir.join("persons.tsv");

    {
        let mut f = std::fs::File::create(&tsv_path).unwrap();
        writeln!(f, "id\tname").unwrap();
        writeln!(f, "1\tAlice").unwrap();
        writeln!(f, "2\tBob").unwrap();
    }

    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
        .unwrap();

    let tsv_str = tsv_path.to_str().unwrap();
    db.execute(&format!("COPY Person FROM '{}' (DELIMITER '\t')", tsv_str)).unwrap();

    let result = db.query("MATCH (n:Person) RETURN n.id, n.name ORDER BY n.id ASC").unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.rows()[0].get_int(0).unwrap(), 1);
    assert_eq!(result.rows()[0].get_string(1).unwrap(), "Alice");

    std::fs::remove_file(&tsv_path).ok();
}

#[test]
fn prepared_statement_with_params() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))").unwrap();
    db.execute("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();
    db.execute("CREATE (n:Person {id: 2, name: 'Bob'})").unwrap();

    let conn = db.connect();
    let stmt = conn.prepare("MATCH (n:Person) WHERE n.id = $id RETURN n.name").unwrap();

    let mut params = HashMap::new();
    params.insert("id".to_string(), Value::Int(1));
    let result = stmt.execute(params).unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "Alice");

    let mut params2 = HashMap::new();
    params2.insert("id".to_string(), Value::Int(2));
    let result2 = stmt.execute(params2).unwrap();
    assert_eq!(result2.num_rows(), 1);
    assert_eq!(result2.rows()[0].get_string(0).unwrap(), "Bob");
}

#[test]
fn recursive_expand_variable_length() {
    // Build a chain: A -> B -> C -> D
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person (name STRING, PRIMARY KEY(name))")
        .unwrap();
    db.execute("CREATE REL TABLE KNOWS (FROM Person TO Person)")
        .unwrap();
    db.execute("CREATE (p:Person {name: 'A'})").unwrap();
    db.execute("CREATE (p:Person {name: 'B'})").unwrap();
    db.execute("CREATE (p:Person {name: 'C'})").unwrap();
    db.execute("CREATE (p:Person {name: 'D'})").unwrap();
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.name = 'A' AND b.name = 'B' \
         CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.name = 'B' AND b.name = 'C' \
         CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.name = 'C' AND b.name = 'D' \
         CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();

    // 1 hop from A: should get B
    let result = db
        .execute(
            "MATCH (a:Person)-[:KNOWS*1..1]->(b:Person) \
             WHERE a.name = 'A' RETURN b.name",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "B");

    // 1..2 hops from A: should get B, C
    let result = db
        .execute(
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) \
             WHERE a.name = 'A' RETURN b.name ORDER BY b.name",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "B");
    assert_eq!(result.rows()[1].get_string(0).unwrap(), "C");

    // 1..3 hops from A: should get B, C, D
    let result = db
        .execute(
            "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) \
             WHERE a.name = 'A' RETURN b.name ORDER BY b.name",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 3);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "B");
    assert_eq!(result.rows()[1].get_string(0).unwrap(), "C");
    assert_eq!(result.rows()[2].get_string(0).unwrap(), "D");

    // Exactly 2 hops from A: should get C only
    let result = db
        .execute(
            "MATCH (a:Person)-[:KNOWS*2..2]->(b:Person) \
             WHERE a.name = 'A' RETURN b.name",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "C");
}

// ── MVCC integration tests ──────────────────────────────────

#[test]
fn mvcc_write_invisible_to_concurrent_read() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Item (id INT64, val STRING, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE (i:Item {id: 1, val: 'original'})").unwrap();

    // Phase 1: verify the row exists
    let r1 = db.execute("MATCH (i:Item) RETURN i.val").unwrap();
    assert_eq!(r1.num_rows(), 1);

    // Phase 2: use low-level API to test snapshot isolation
    // Start a read-only transaction (captures snapshot BEFORE the write)
    let db_inner = db.inner.clone();
    let mut read_txn = db_inner.txn_manager.begin_read_only();
    let read_start_ts = read_txn.start_ts;

    // Now do a write that inserts a new row
    db.execute("CREATE (i:Item {id: 2, val: 'new_item'})").unwrap();

    // The read transaction should NOT see the new row
    let engine = Engine::with_snapshot(read_start_ts, HashMap::new());
    let physical = {
        let catalog = db_inner.catalog.read().unwrap();
        let mut binder = gqlite_core::binder::Binder::new(&catalog);
        let stmt = gqlite_core::parser::parser::Parser::parse_query(
            "MATCH (i:Item) RETURN i.val"
        ).unwrap();
        let bound = binder.bind(&stmt).unwrap();
        let planner = gqlite_core::planner::logical::Planner::new(&catalog);
        let logical = planner.plan(&bound).unwrap();
        let logical = gqlite_core::planner::optimizer::optimize(logical);
        gqlite_core::planner::physical::to_physical(&logical)
    };
    let result = engine.execute_plan_parallel(&physical, &db_inner, read_txn.id).unwrap();
    assert_eq!(result.num_rows(), 1, "concurrent read should only see 1 row");
    assert_eq!(result.rows()[0].get_string(0).unwrap(), "original");

    db_inner.txn_manager.commit(&mut read_txn);

    // Phase 3: a NEW read after commit should see both rows
    let r3 = db.execute("MATCH (i:Item) RETURN i.val ORDER BY i.val").unwrap();
    assert_eq!(r3.num_rows(), 2);
}

#[test]
fn mvcc_write_visible_after_commit() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Thing (id INT64, name STRING, PRIMARY KEY(id))")
        .unwrap();

    // Before any data insert, read sees nothing
    let r1 = db.execute("MATCH (t:Thing) RETURN t.name").unwrap();
    assert_eq!(r1.num_rows(), 0);

    // Write and commit
    db.execute("CREATE (t:Thing {id: 1, name: 'alpha'})").unwrap();

    // New read after commit sees the data
    let r2 = db.execute("MATCH (t:Thing) RETURN t.name").unwrap();
    assert_eq!(r2.num_rows(), 1);
    assert_eq!(r2.rows()[0].get_string(0).unwrap(), "alpha");
}

#[test]
fn mvcc_delete_invisible_to_concurrent_read() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Item (id INT64, val STRING, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE (i:Item {id: 1, val: 'keep'})").unwrap();
    db.execute("CREATE (i:Item {id: 2, val: 'delete_me'})").unwrap();

    // Start a read-only transaction (captures snapshot)
    let db_inner = db.inner.clone();
    let mut read_txn = db_inner.txn_manager.begin_read_only();
    let read_start_ts = read_txn.start_ts;

    // Delete a row in a write transaction
    db.execute("MATCH (i:Item) WHERE i.id = 2 DELETE i").unwrap();

    // The read transaction should still see both rows (delete happened after snapshot)
    let engine = Engine::with_snapshot(read_start_ts, HashMap::new());
    let physical = {
        let catalog = db_inner.catalog.read().unwrap();
        let mut binder = gqlite_core::binder::Binder::new(&catalog);
        let stmt = gqlite_core::parser::parser::Parser::parse_query(
            "MATCH (i:Item) RETURN i.val ORDER BY i.val"
        ).unwrap();
        let bound = binder.bind(&stmt).unwrap();
        let planner = gqlite_core::planner::logical::Planner::new(&catalog);
        let logical = planner.plan(&bound).unwrap();
        let logical = gqlite_core::planner::optimizer::optimize(logical);
        gqlite_core::planner::physical::to_physical(&logical)
    };
    let result = engine.execute_plan_parallel(&physical, &db_inner, read_txn.id).unwrap();
    assert_eq!(result.num_rows(), 2, "concurrent read should still see 2 rows");
    db_inner.txn_manager.commit(&mut read_txn);

    // After committing the read txn, new reads should see only 1 row
    let r3 = db.execute("MATCH (i:Item) RETURN i.val").unwrap();
    assert_eq!(r3.num_rows(), 1);
    assert_eq!(r3.rows()[0].get_string(0).unwrap(), "keep");
}

#[test]
fn mvcc_gc_old_versions() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Item (id INT64, PRIMARY KEY(id))")
        .unwrap();
    db.execute("CREATE (i:Item {id: 1})").unwrap();
    db.execute("CREATE (i:Item {id: 2})").unwrap();

    // Delete id=1
    db.execute("MATCH (i:Item) WHERE i.id = 1 DELETE i").unwrap();

    // Run GC with a safe timestamp beyond all transactions
    let last_committed = db.inner.txn_manager.last_committed_id();
    let mut storage = db.inner.storage.write().unwrap();
    let mut total_purged = 0u64;
    for nt in storage.node_tables.values_mut() {
        total_purged += nt.gc(last_committed + 1);
    }
    assert!(total_purged > 0, "GC should purge at least one deleted row");
}
