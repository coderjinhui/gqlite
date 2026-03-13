# gqlite

**A lightweight, embeddable graph database for Rust.**

gqlite is to graph databases what SQLite is to relational databases — a zero-config, serverless, single-file graph database engine that you can embed directly into your application.

---

**gqlite —— 轻量级嵌入式图数据库**

gqlite 之于图数据库，正如 SQLite 之于关系型数据库 —— 零配置、无服务器、单文件的图数据库引擎，可直接嵌入应用程序。

## Features / 特性

- **Embedded** — link as a library, no separate server process
- **Single-file storage** — one `.graph` file holds the entire database
- **GQL query language** — subset of the ISO GQL / openCypher standard
- **CSR adjacency** — Compressed Sparse Row for fast graph traversal
- **Columnar properties** — efficient property storage and filtering

## Quick Start / 快速开始

```rust
use gqlite_core::Database;

fn main() {
    let db = Database::open("my.graph").unwrap();
    let result = db.query("MATCH (n:Person) RETURN n").unwrap();
    println!("{} rows", result.rows.len());
}
```

### CLI

```bash
cargo run --bin gqlite -- my.graph
```

```
gqlite v0.1.0
Connected to: my.graph
gqlite> MATCH (n) RETURN n
(empty result set)
gqlite> .quit
```

## Architecture / 架构概览

```
┌──────────────────────────────────────────┐
│               gqlite-cli                 │  Interactive REPL
├──────────────────────────────────────────┤
│               gqlite-core                │  Core library
│  ┌─────────┐ ┌─────────┐ ┌───────────┐  │
│  │ Parser  │→│ Planner │→│ Executor  │  │
│  └─────────┘ └─────────┘ └─────┬─────┘  │
│                                 │        │
│  ┌─────────┐ ┌─────────────────┴──────┐  │
│  │ Catalog │ │       Storage          │  │
│  │         │ │  CSR + Column + Pager  │  │
│  └─────────┘ └────────────────────────┘  │
├──────────────────────────────────────────┤
│            .graph file (disk)            │
└──────────────────────────────────────────┘
```

See [`docs/architecture.md`](docs/architecture.md) for the detailed design document.

## Project Structure / 项目结构

```
crates/
├── core/    # gqlite-core — storage, parser, planner, executor, catalog
│   └── tests/   # Integration tests
└── cli/     # gqlite-cli  — interactive REPL
examples/    # Usage examples
docs/        # Design documents
```

## Roadmap / 路线图

- [x] Project skeleton & module structure
- [ ] Storage engine — page manager, .graph file format
- [ ] GQL parser — lexer, recursive descent parser
- [ ] CSR adjacency index
- [ ] Columnar property storage
- [ ] Query planner (logical → physical)
- [ ] Basic query execution (MATCH + RETURN)
- [ ] CREATE / DELETE mutations
- [ ] WHERE clause filtering
- [ ] REPL improvements (history, completion)
- [ ] Transactions & WAL
- [ ] Benchmarks

## License / 许可证

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))

at your option.
