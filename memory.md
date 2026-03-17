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
| gqlite-parser | `crates/parser/` | 独立 Cypher 解析器：Lexer, AST, Parser, DataType |
| gqlite-core | `crates/core/` | 引擎核心：存储、规划、执行、事务（依赖 gqlite-parser） |
| gqlite-cli | `crates/cli/` | 交互式 REPL (rustyline)，Tab 补全 + 多语句执行 |
| gqlite-gui | `crates/gui/` | 桌面 GUI 管理工具 (Tauri v2 + Svelte 5 + G6 图可视化) |
|  | | → [memory.md](crates/gui/memory.md) / [technology.md](crates/gui/technology.md) |

每个 crate 必须包含：`Cargo.toml` + `src/` + `tests/` + `doc/`

## gqlite-parser 模块索引 (crates/parser/)

| 模块 | 路径 | 职责 |
|------|------|------|
| token | `src/token.rs` | logos Lexer，Cypher 词法分析 |
| ast | `src/ast.rs` | AST 节点类型定义 |
| parser | `src/parser.rs` | 递归下降 Parser（parse_query + parse_all 多语句） |
| data_type | `src/data_type.rs` | DataType 枚举（含 Date/DateTime/Duration） |
| doc | `doc/design.md` | 语法支持文档 |
| tests | `tests/` | parser_test.rs, token_test.rs, data_type_test.rs |

## gqlite-core 模块索引 (crates/core/)

| 模块 | 路径 | 职责 |
|------|------|------|
| types | `src/types/` | Value, InternalId（DataType 从 gqlite-parser re-export） |
| storage | `src/storage/` | ColumnChunk, NodeGroup, CSR, Pager, NodeTable, RelTable |
| catalog | `src/catalog/` | 表元数据管理，bincode 序列化 |
| parser | `src/parser/` | re-export gqlite-parser（兼容层） |
| binder | `src/binder/` | 语义绑定：变量解析、表名/列名匹配 |
| planner | `src/planner/` | 逻辑计划生成 + 物理计划转换 |
| executor | `src/executor/` | Engine (物化执行), DataChunk |
| functions | `src/functions/` | FunctionRegistry, 标量/聚合/数学/日期时间函数 |
| procedure | `src/procedure/` | CALL 过程框架 + 图算法（PageRank, Dijkstra, WCC 等） |
| transaction | `src/transaction/` | TransactionManager (SWMR), WAL |
| error | `src/error.rs` | GqliteError 统一错误类型 |
| doc | `doc/design.md` | 架构设计文档 |
| tests | `tests/` | 45 个集成测试文件（`<module>_test.rs`） |

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
- **Post-plan**: ✅ Checkpoint (.graph 主文件 + WAL 两阶段恢复) + Auto-checkpoint + CLI 补全
- **P1 查询增强 (037-044)**: ✅ 全部完成 (WITH/ORDER BY/LIMIT/SKIP + Aggregate + Sort + OPTIONAL MATCH/UNION + MERGE/UNWIND + 优化)
- **P1 存储增强 (045-048)**: ✅ 全部完成 (Bit-Packing + Buffer Pool + SERIAL + ALTER TABLE)
- **P1 导入导出 (049-051)**: ✅ 全部完成 (COPY FROM/TO CSV)
- **P1 函数+接口 (052-056)**: ✅ 全部完成 (字符串函数 + 列表函数 + CAST + PreparedStatement + DatabaseConfig)
- **P1 并行+路径 (057-060)**: ✅ 全部完成 (Pipeline切分 + rayon并行 + 可变长路径 + MVCC)
- **P2 图查询算法 (061a-061r)**: ✅ 全部完成 (CASE/IN/EXISTS/正则/列表推导 + 图算法 8 种 + 数学函数 + 日期时间 + 子查询)
- **Post-plan 增强**: ✅ CLI 多语句执行（分号分隔）+ 各 crate `doc/` 文档
- **GUI 桌面工具**: ✅ Tauri v2 + Svelte 5 + CodeMirror + @antv/G6 图可视化

## 关键设计决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 存储格式 | 列式 (ColumnChunk + NodeGroup) | 属性过滤高效 |
| 图遍历 | CSR (Compressed Sparse Row) | 邻接查询 O(degree) |
| 执行模型 | 物化 (Intermediate 全量返回) | 实现简单，后续可改流式 |
| 并发模型 | SWMR (parking_lot) + MVCC | 单写多读 + 快照隔离，`create_ts`/`delete_ts` per row |
| 序列化 | bincode | 高性能二进制，用于 Catalog 持久化 |
| 查询语言 | Cypher 子集 | MATCH/WHERE/RETURN/CREATE/SET/DELETE + WITH/ORDER BY/LIMIT + CASE/IN/EXISTS + CALL + UNION/MERGE/UNWIND |

## 测试

423 个测试（parser 3 文件 + core 45 文件 + cli 1 文件），全部通过，零 warning。运行：`cargo test`
