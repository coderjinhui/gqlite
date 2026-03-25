//! Core query benchmarks for gqlite.
//!
//! Run: `cargo bench -p gqlite-core`

use criterion::{criterion_group, criterion_main, Criterion};
use gqlite_core::Database;

fn setup_small_db() -> Database {
    let db = Database::in_memory();
    db.execute("CREATE NODE TABLE Person(id INT64, name STRING, age INT64, PRIMARY KEY(id))")
        .unwrap();
    for i in 0..1000 {
        db.execute(&format!(
            "CREATE (n:Person {{id: {}, name: 'user_{}', age: {}}})",
            i,
            i,
            20 + i % 50
        ))
        .unwrap();
    }
    db
}

fn bench_scan(c: &mut Criterion) {
    let db = setup_small_db();
    c.bench_function("scan_1k_rows", |b| {
        b.iter(|| {
            let r = db.query("MATCH (n:Person) RETURN n.id").unwrap();
            assert_eq!(r.num_rows(), 1000);
        })
    });
}

fn bench_filter(c: &mut Criterion) {
    let db = setup_small_db();
    c.bench_function("filter_eq_1k", |b| {
        b.iter(|| {
            let r = db.query("MATCH (n:Person) WHERE n.age = 25 RETURN n.id").unwrap();
            assert!(r.num_rows() > 0);
        })
    });
}

fn bench_aggregate(c: &mut Criterion) {
    let db = setup_small_db();
    c.bench_function("count_1k", |b| {
        b.iter(|| {
            let r = db.query("MATCH (n:Person) RETURN COUNT(n)").unwrap();
            assert_eq!(r.num_rows(), 1);
        })
    });
}

fn bench_order_by(c: &mut Criterion) {
    let db = setup_small_db();
    c.bench_function("order_by_1k", |b| {
        b.iter(|| {
            let r = db.query("MATCH (n:Person) RETURN n.id ORDER BY n.age").unwrap();
            assert_eq!(r.num_rows(), 1000);
        })
    });
}

fn bench_insert(c: &mut Criterion) {
    c.bench_function("insert_single", |b| {
        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE T(id INT64, PRIMARY KEY(id))").unwrap();
        let mut id = 0i64;
        b.iter(|| {
            db.execute(&format!("CREATE (n:T {{id: {}}})", id)).unwrap();
            id += 1;
        })
    });
}

fn bench_parse(c: &mut Criterion) {
    c.bench_function("parse_simple_match", |b| {
        b.iter(|| {
            let _ = gqlite_parser::Parser::parse_query(
                "MATCH (n:Person) WHERE n.age > 25 RETURN n.name, n.age ORDER BY n.age LIMIT 10",
            )
            .unwrap();
        })
    });
}

criterion_group!(
    benches,
    bench_scan,
    bench_filter,
    bench_aggregate,
    bench_order_by,
    bench_insert,
    bench_parse,
);
criterion_main!(benches);
