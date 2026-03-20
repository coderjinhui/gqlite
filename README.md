# gqlite

## What Is gqlite?

gqlite is an embedded graph database written in Rust, with:

- embedded deployment
- single-file storage
- a Rust-native API
- a Cypher / GQL-inspired query surface
- local transactions, WAL, checkpoint, and recovery support

Typical use cases include:

- embedding graph storage directly into a Rust application
- shipping graph-powered desktop or local-first tools
- running graph queries without operating a separate database server
- prototyping graph workloads with a small deployment footprint

## How To Use gqlite

### As a Rust library

```rust
use gqlite_core::Database;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open("example.graph")?;

    db.execute("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id))")?;
    db.execute("CREATE REL TABLE KNOWS(FROM Person TO Person)")?;

    db.execute("CREATE (p:Person {id: 1, name: 'Alice'})")?;
    db.execute("CREATE (p:Person {id: 2, name: 'Bob'})")?;
    db.execute(
        "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 \
         CREATE (a)-[:KNOWS]->(b)",
    )?;

    let result = db.query(
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
    )?;

    println!("rows: {}", result.num_rows());
    Ok(())
}
```

### From the CLI

```bash
cargo run --bin gqlite -- example.graph
```

### More documentation

- Quick start: [docs/quickstart.md](docs/quickstart.md)
- Architecture: [docs/architecture.md](docs/architecture.md)
- Known issues: [docs/known-issues.md](docs/known-issues.md)

## Features

- Embedded database engine with no standalone server process
- Single-file `.graph` database format
- Schema-based property graph model with node tables and relationship tables
- Cypher / GQL-inspired query support for `MATCH`, `CREATE`, `MERGE`, `DELETE`, `WITH`, `UNWIND`, `EXPLAIN`, and more
- Transactions, WAL, checkpoint, reopen, and recovery flows
- Graph traversal and built-in graph algorithms such as PageRank, WCC, shortest path, and centrality procedures
- CSV import / export support
- CLI for interactive querying
- Desktop GUI workspace for local database exploration and management

For full syntax and implementation details, see [docs/quickstart.md](docs/quickstart.md) and the documents under [docs/](docs).

## Project Structure

This repository is a Rust workspace with a few main packages:

- `crates/core` — `gqlite-core`, the main embedded database engine: catalog, storage, planner, executor, transactions, and graph algorithms
- `crates/parser` — `gqlite-parser`, the query parser crate used by the engine
- `crates/cli` — `gqlite-cli`, the command-line interface and REPL
- `crates/gui` — the frontend assets for the desktop GUI
- `crates/gui/src-tauri` — the Tauri desktop shell that connects the GUI to `gqlite-core`
- `docs` — architecture notes, roadmap, design docs, and project references
- `examples` — small usage examples

If you only want to embed gqlite into your own application, `gqlite-core` is the primary package to use.

## Roadmap

The project roadmap is documented in [docs/roadmap-production.md](docs/roadmap-production.md). At a high level, the current direction is:

1. Improve correctness in transactions, MVCC behavior, and recovery
2. Harden the storage engine and file format
3. Expand performance, testing, tooling, and operational visibility
4. Stabilize APIs, file format compatibility, and release readiness

This README intentionally stays high level. See the roadmap document for the detailed production plan.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))

at your option.
