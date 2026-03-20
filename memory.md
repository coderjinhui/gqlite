# gqlite — 轻量级嵌入式图数据库

## 项目概述

Rust 实现的嵌入式图数据库，定位类似 SQLite 之于关系型数据库。单文件存储（`.graph`），支持 Cypher-like 查询语言，可作为库嵌入应用。版本 `0.1.0-beta.1`。

## 架构总览

```
Query: GQL String
  → Parser (logos lexer + recursive descent)
  → Binder (语义绑定，解析表名/列名)
  → Planner (逻辑计划 → 物理计划 → 优化)
  → Executor (物化执行，Pull 模型，WAL 缓冲写入)
  → QueryResult (列式结果 + 迭代器)
```

并发模型：`Arc<DatabaseInner>` + `RwLock<Catalog>` + `RwLock<Storage>` + `TransactionManager`(SWMR) + 跨进程 `fs2` 文件锁

## Workspace 结构

| Crate | 路径 | 说明 |
|-------|------|------|
| gqlite-parser | `crates/parser/` | 独立 Cypher 解析器：Lexer, AST, Parser, DataType |
| gqlite-core | `crates/core/` | 引擎核心：存储、规划、执行、事务 |
| gqlite-cli | `crates/cli/` | 交互式 REPL (rustyline)，Tab 补全 + 多语句 |
| gqlite-gui | `crates/gui/` | 桌面 GUI (Tauri v2 + Svelte 5 + G6) → [memory.md](crates/gui/memory.md) |

每个 crate 必须包含：`Cargo.toml` + `src/` + `tests/` + `doc/`

## gqlite-core 模块索引

| 模块 | 路径 | 职责 |
|------|------|------|
| types | `src/types/` | Value, InternalId, DataType |
| storage | `src/storage/` | ColumnChunk, NodeGroup, CSR, Pager, NodeTable, RelTable, BufferPool, **Upgrader** |
| catalog | `src/catalog/` | 表元数据，bincode + v2 页级持久化 |
| binder | `src/binder/` | 语义绑定 |
| planner | `src/planner/` | 逻辑/物理计划 + 优化器 |
| executor | `src/executor/` | Engine (WAL 缓冲写入), DataChunk, 并行执行 |
| functions | `src/functions/` | 标量/聚合/数学/日期时间函数 |
| procedure | `src/procedure/` | CALL 过程框架 + 8 种图算法 |
| transaction | `src/transaction/` | TransactionManager, WAL, **WriteSet** |
| testing | `src/testing/` | **FaultInjector** (故障注入框架) |
| error | `src/error.rs` | GqliteError + **ErrorCode** 错误码体系 |

## 生产化路线图

任务索引：`docs/tasks/000-index.md`（38 个任务，4 个里程碑，全部完成）

| 里程碑 | 内容 | 状态 |
|--------|------|------|
| M0 数据正确性 | WAL 缓冲写入, 显式事务, 文件锁, MVCC update_ts | ✅ 10/10 |
| M1 存储重构 | v2 页格式, CRC32 checksum, 格式升级器, dump/restore | ✅ 10/10 |
| M2 工程化 | clippy 零警告, CI, benchmark, EXPLAIN, 日志, fuzz, soak | ✅ 10/10 |
| M3 商业化 | 错误码, API 冻结, 格式兼容, 产品文档, beta 准备 | ✅ 8/8 |

## 关键设计决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 存储格式 | 列式 (ColumnChunk + NodeGroup) | 属性过滤高效 |
| 图遍历 | CSR (Compressed Sparse Row) | 邻接查询 O(degree) |
| 执行模型 | 物化 (Intermediate 全量返回) | 实现简单 |
| 并发 | SWMR + MVCC (create_ts/delete_ts/update_ts) | 单写多读 + 快照隔离 |
| 事务 | WAL 缓冲写入 + 原子 commit | 失败不留脏数据 |
| 文件格式 | v2: page header + CRC32 checksum | 损坏检测 |
| 日志 | `log` crate (零开销 facade) | 用户可选 backend |

## 测试

510 个测试（parser + core 53 文件 + cli），全部通过，clippy -D warnings 零警告。

运行：`cargo test` 或 `bash scripts/check.sh --strict`
