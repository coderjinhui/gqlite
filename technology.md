# 技术栈索引

开发前请先查阅此文件，优先复用已有技术方案。

## Rust 依赖

| 功能 | Crate | 版本 | 用途 |
|------|-------|------|------|
| 错误处理 | thiserror | ^1 | derive Error 宏 |
| 序列化 | serde + bincode | ^1 | Catalog/Storage 持久化 |
| UUID | uuid | ^1 (v4) | 数据库文件唯一标识 |
| 位图 | bitvec | ^1 | NULL 位图 |
| 词法分析 | logos | ^0.14 | Token 生成器 |
| 哈希 | ahash | ^0.8 | 高性能 HashMap |
| 并发锁 | parking_lot | ^0.12 | SWMR 事务管理 |
| CRC 校验 | crc32fast | ^1 | WAL 记录 + 页级 checksum |
| 并行执行 | rayon | ^1 | HashJoin/Union 并行 |
| 正则匹配 | regex | ^1 | Cypher `=~` 操作符 |
| 日期时间 | chrono | ^0.4 | Date/DateTime/Duration 类型 |
| 文件锁 | fs2 | ^0.4 | 跨进程 exclusive/shared flock |
| 日志 | log | ^0.4 | 结构化日志 facade（零开销） |
| REPL | rustyline | ^14 | CLI Tab 补全 + 灰色提示 |
| 基准测试 | criterion | ^0.5 | 性能回归基线 (dev-dependency) |
| 桌面框架 | tauri | ^2 | GUI → 详见 [crates/gui/technology.md](crates/gui/technology.md) |

## 核心数据结构

| 结构 | 位置 | 用途 |
|------|------|------|
| Value | `types/value.rs` | 统一值类型 |
| DataType | `parser/src/data_type.rs` | 类型枚举（含 Serial/Date/DateTime/Duration） |
| ColumnChunk | `storage/column_chunk.rs` | 定长列块，v2 页级 I/O |
| NodeTable | `storage/table.rs` | 节点表，含 MVCC create_ts/delete_ts/update_ts |
| RelTable | `storage/table.rs` | 关系表，CSR 前向/后向索引 |
| Catalog | `catalog/mod.rs` | 表元数据 + v2 分页持久化 |
| WriteSet | `transaction/write_set.rs` | 事务写集缓冲（DDL/DML 操作） |
| FaultInjector | `testing/fault_injection.rs` | 故障注入（WAL/checkpoint/storage） |
| ErrorCode | `error.rs` | 稳定错误码枚举（1xxx-5xxx） |
| BufferPool | `storage/buffer_manager.rs` | LRU 页缓存 + 命中率统计 |
| PageType | `storage/format.rs` | v2 页类型枚举 + checksum 工具 |

## 查询管道

```
Parser::parse_query(gql) → Statement (含 BEGIN/COMMIT/ROLLBACK/EXPLAIN)
  → Binder::bind(&stmt) → BoundStatement
  → Planner::plan(&bound) → LogicalOperator → optimize()
  → physical::to_physical(&logical) → PhysicalPlan (含 explain_text())
  → Engine::execute_plan(&plan, &db) → QueryResult
```

## 编码规范

| 项目 | 约定 |
|------|------|
| 格式化 | `rustfmt.toml`: max_width=100 |
| 静态检查 | `cargo clippy -- -D warnings` 零警告 |
| 错误类型 | `GqliteError` 枚举 + `ErrorCode` 数字码 |
| 锁顺序 | catalog → storage（避免死锁） |
| 测试 | `tests/` 目录，`<module>_test.rs` |
| CI | `bash scripts/check.sh --strict`（fmt + clippy + test） |
| MSRV | Rust 1.89 |
