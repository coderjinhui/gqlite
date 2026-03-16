# 技术栈索引

开发前请先查阅此文件，优先复用已有技术方案。

## Rust 依赖

| 功能 | Crate | 版本 | 用途 |
|------|-------|------|------|
| 错误处理 | thiserror | ^1 | derive Error 宏 |
| 序列化 | serde + bincode | ^1 | Catalog 持久化 |
| UUID | uuid | ^1 (v4) | 唯一标识生成 |
| 位图 | bitvec | ^1 | NULL 位图 |
| 词法分析 | logos | ^0.14 | Token 生成器 |
| 哈希 | ahash | ^0.8 | 高性能 HashMap |
| 并发锁 | parking_lot | ^0.12 | SWMR 事务管理 |
| CRC 校验 | crc32fast | ^1 | WAL 记录校验和 |
| REPL | rustyline | ^14 | CLI 交互式编辑 |

## 核心数据结构

| 结构 | 位置 | 用途 |
|------|------|------|
| Value | `types/value.rs` | 统一值类型 (Int64/Float64/Bool/String/InternalId/Null) |
| DataType | `types/data_type.rs` | 类型枚举 |
| InternalId | `types/graph.rs` | 节点/边内部 ID (table_id, offset) |
| ColumnChunk | `storage/column_chunk.rs` | 定长列块，支持 flush/load |
| NodeGroup | `storage/node_group.rs` | 节点组 (2048 行 × N 列) |
| CsrIndex | `storage/csr.rs` | CSR 邻接索引 |
| Catalog | `catalog/mod.rs` | 表元数据 (NodeTableEntry/RelTableEntry) |
| Intermediate | `executor/engine.rs` | 执行中间结果 (columns + rows) |
| TransactionManager | `transaction/mod.rs` | SWMR 事务管理器 |

## 查询管道

```
Parser::parse_query(gql) → Statement
  → Binder::bind(&stmt) → BoundStatement
  → Planner::plan(&bound) → LogicalOperator
  → physical::to_physical(&logical) → PhysicalPlan
  → Engine::execute_plan(&plan, &db) → QueryResult
```

## 编码规范

| 项目 | 约定 |
|------|------|
| 格式化 | `rustfmt.toml`: max_width=100 |
| 错误类型 | 统一 `GqliteError` 枚举 |
| 列命名 | SeqScan 输出: `[alias, alias.col1, alias.col2, ...]` |
| 锁顺序 | catalog → storage（避免死锁） |
| 测试 | 放在 `tests/` 目录，文件命名 `<module>_test.rs` |
