# gqlite — 轻量级嵌入式图数据库

## 项目概述

Rust 实现的嵌入式图数据库，定位类似 SQLite 之于关系型数据库。单文件存储（`.graph`），支持 Cypher-like 查询语言，可作为库嵌入应用。

## 架构总览

```
Query: GQL String
  → Parser (logos lexer + recursive descent)
  → Binder (语义绑定，解析表名/列名)
  → Planner (逻辑计划 → 物理计划)
  → Executor (物化执行，Pull 模型)
  → QueryResult (列式结果 + 迭代器)
```

核心并发模型：`Arc<DatabaseInner>` + `RwLock<Catalog>` + `RwLock<Storage>` + `TransactionManager`(parking_lot SWMR)

## Workspace 结构

| Crate | 路径 | 说明 |
|-------|------|------|
| gqlite-core | `crates/core/` | 引擎核心：存储、解析、规划、执行、事务 |
| gqlite-cli | `crates/cli/` | 交互式 REPL (rustyline) |

## 核心模块索引 (crates/core/src/)

| 模块 | 路径 | 职责 |
|------|------|------|
| types | `types/` | DataType, Value, InternalId |
| storage | `storage/` | ColumnChunk, NodeGroup, CSR, Pager, NodeTable, RelTable |
| catalog | `catalog/` | 表元数据管理，bincode 序列化 |
| parser | `parser/` | logos Lexer, AST, 递归下降 Parser |
| binder | `binder/` | 语义绑定：变量解析、表名/列名匹配 |
| planner | `planner/` | 逻辑计划生成 + 物理计划转换 |
| executor | `executor/` | Engine (物化执行), DataChunk |
| functions | `functions/` | FunctionRegistry, 标量函数, 聚合函数 |
| transaction | `transaction/` | TransactionManager (SWMR), WAL (write/read/replay/checkpoint) |
| error | `error.rs` | GqliteError 统一错误类型 |

## 设计与计划文档

| 文档 | 路径 | 内容 |
|------|------|------|
| 实现计划索引 | `research/plan/000-index.md` | 60 个任务的优先级和状态追踪 |
| 设计文档 (8 篇) | `research/design/0{1-8}-*.md` | 存储/查询/事务/导入等设计方案 |
| 任务计划 (60 篇) | `research/plan/0{01-060}-*.md` | 每个功能点的详细实现计划 |
| Kuzu 调研 | `research/kuzu/` | 参考 Kuzu 图数据库的调研笔记 |

## 实现进度

- **P0 基础层 (001-007)**: ✅ 全部完成
- **P0 存储层 (008-013)**: ✅ 全部完成
- **P0 解析层 (014-020)**: ✅ 全部完成
- **P0 查询处理 (021-027)**: ✅ 全部完成
- **P0 集成层 (028-032)**: ✅ 全部完成
- **P0 事务 (033-036)**: ✅ 全部完成 (SWMR + WAL + Recovery + Checkpoint)
- **P1 (037-060)**: 未开始

## 关键设计决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 存储格式 | 列式 (ColumnChunk + NodeGroup) | 属性过滤高效 |
| 图遍历 | CSR (Compressed Sparse Row) | 邻接查询 O(degree) |
| 执行模型 | 物化 (Intermediate 全量返回) | 实现简单，后续可改流式 |
| 并发模型 | SWMR (parking_lot) | 单写多读，类似 SQLite |
| 序列化 | bincode | 高性能二进制，用于 Catalog 持久化 |
| 查询语言 | Cypher 子集 | MATCH/WHERE/RETURN/CREATE/SET/DELETE + DDL |

## 测试

163 个测试（156 单元 + 2 基础集成 + 5 持久化集成），零 warning。运行：`cargo test`
