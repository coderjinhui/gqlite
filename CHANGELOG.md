# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0-beta.1] - 2026-03-20

### Added
- **Core Engine**: Cypher subset query language with MATCH/WHERE/RETURN/WITH/ORDER BY/LIMIT/SKIP
- **Write Operations**: CREATE/SET/DELETE/DETACH DELETE/MERGE/UNWIND
- **Schema Management**: CREATE/DROP/ALTER NODE TABLE and REL TABLE
- **Transactions**: BEGIN/COMMIT/ROLLBACK explicit transaction support via `execute_script()`
- **WAL**: Write-ahead logging with buffered writes and atomic commit
- **Checkpoint**: Manual and auto-checkpoint with configurable threshold
- **Recovery**: Two-phase recovery (main file + WAL incremental replay)
- **File Locking**: Cross-process file locking (exclusive write, shared read)
- **MVCC**: Snapshot isolation with create_ts/delete_ts/update_ts per row
- **Page Checksums**: CRC32 checksum per page in v2 file format
- **Format Upgrader**: Automatic v1→v2 format upgrade with backup
- **EXPLAIN**: Query execution plan display
- **Dump/Restore**: Full database export as Cypher script
- **Integrity Check**: `Database::check()` for catalog/storage consistency verification
- **Graph Algorithms**: PageRank, Dijkstra, WCC, degree centrality, betweenness, LPA, triangle count (8 algorithms via CALL)
- **Functions**: 15+ string functions, math functions, aggregate functions, date/time functions
- **COPY FROM/TO**: CSV import/export
- **Prepared Statements**: Parameterized query execution
- **CLI**: Interactive REPL with tab completion and multi-statement execution
- **GUI**: Desktop management tool (Tauri v2 + Svelte 5 + G6 graph visualization)
- **Benchmarks**: criterion-based performance benchmarks
- **Parser Fuzz**: cargo-fuzz targets for parser robustness testing
- **Soak Test**: Sustained mixed workload stress test
- **Fault Injection**: Testing framework for failure scenario simulation
- **CI**: GitHub Actions pipeline (fmt + clippy + test)
- **Structured Logging**: `log` crate integration for query/checkpoint/recovery events

### File Format
- Format version: 2
- Page size: 4096 bytes
- Page-level CRC32 checksums
- Backward compatible with v1 (auto-upgrade)

### Known Limitations
- Storage uses bincode whole-database serialization (startup time scales with data size)
- Checkpoint writes full snapshot (not incremental dirty pages)
- SWMR model: single writer at a time
- No SAVEPOINT / nested transactions
- UPDATE uses in-place modification (not multi-version)
- MATCH property filters `{id: 1}` not supported (use WHERE instead)
