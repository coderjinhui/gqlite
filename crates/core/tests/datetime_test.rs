use gqlite_core::functions::datetime::*;
use gqlite_core::types::value::Value;
use gqlite_core::Database;

#[test]
fn datetime_date_parse() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    let result = db.query("MATCH (n:N) RETURN date('2024-01-15')").unwrap();
    assert_eq!(result.num_rows(), 1);
    let val = result.rows()[0].get(0);
    assert_eq!(val.to_string(), "2024-01-15");
}

#[test]
fn datetime_datetime_parse() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    let result = db.query("MATCH (n:N) RETURN datetime('2024-01-15T10:30:00')").unwrap();
    assert_eq!(result.num_rows(), 1);
    let s = result.rows()[0].get(0).to_string();
    assert!(s.contains("2024-01-15"), "Expected date in output: {}", s);
    assert!(s.contains("10:30:00"), "Expected time in output: {}", s);
}

#[test]
fn datetime_timestamp_returns_int() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    let result = db.query("MATCH (n:N) RETURN timestamp()").unwrap();
    assert_eq!(result.num_rows(), 1);
    let ts = result.rows()[0].get_int(0).unwrap();
    assert!(ts > 1700000000, "timestamp should be a Unix timestamp: {}", ts);
}

#[test]
fn datetime_date_no_args_returns_today() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    let result = db.query("MATCH (n:N) RETURN date()").unwrap();
    assert_eq!(result.num_rows(), 1);
    // Verify it returns something in YYYY-MM-DD format (10 chars)
    let s = result.rows()[0].get(0).to_string();
    assert!(s.len() == 10, "Expected YYYY-MM-DD format: {}", s);
}

#[test]
fn datetime_invalid_format_errors() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    let result = db.query("MATCH (n:N) RETURN date('not-a-date')");
    assert!(result.is_err());
}

#[test]
fn datetime_null_handling() {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE N(id INT64, name STRING, PRIMARY KEY(id))").unwrap();
    db.execute("CREATE (n:N {id: 1})").unwrap();
    let result = db.query("MATCH (n:N) RETURN date(n.name)").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert!(result.rows()[0].get(0).is_null());
}

// Unit-level function tests
#[test]
fn fn_date_unit() {
    let d = fn_date(&[Value::String("2024-06-15".into())]).unwrap();
    assert_eq!(d.to_string(), "2024-06-15");
    assert!(d.as_date().is_some());

    // No-arg returns today
    let today = fn_date(&[]).unwrap();
    assert!(today.as_date().is_some());

    // Null propagation
    let null = fn_date(&[Value::Null]).unwrap();
    assert!(null.is_null());

    // Invalid format
    assert!(fn_date(&[Value::String("bad".into())]).is_err());
}

#[test]
fn fn_datetime_unit() {
    let dt = fn_datetime(&[Value::String("2024-06-15T14:30:00".into())]).unwrap();
    assert_eq!(dt.to_string(), "2024-06-15T14:30:00");
    assert!(dt.as_datetime().is_some());

    // No-arg returns now
    let now = fn_datetime(&[]).unwrap();
    assert!(now.as_datetime().is_some());

    // Null propagation
    let null = fn_datetime(&[Value::Null]).unwrap();
    assert!(null.is_null());
}

#[test]
fn fn_timestamp_unit() {
    // No args -> current unix timestamp
    let ts = fn_timestamp(&[]).unwrap();
    let v = ts.as_int().unwrap();
    assert!(v > 1700000000);

    // From a datetime
    let dt = fn_datetime(&[Value::String("2024-01-01T00:00:00".into())]).unwrap();
    let ts2 = fn_timestamp(&[dt]).unwrap();
    assert_eq!(ts2.as_int().unwrap(), 1704067200);

    // Null propagation
    let null = fn_timestamp(&[Value::Null]).unwrap();
    assert!(null.is_null());
}
