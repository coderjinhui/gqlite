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
| 并行执行 | rayon | ^1 | HashJoin/Union 并行 |
| 正则匹配 | regex | ^1 | Cypher `=~` 操作符 |
| 日期时间 | chrono | ^0.4 (serde) | Date/DateTime/Duration 类型 |
| REPL | rustyline | ^14 | CLI 交互式编辑，Tab 补全 + 灰色提示 (derive feature) |
| 桌面框架 | tauri | ^2 | GUI 跨平台桌面应用（gqlite-gui） |
| 文件对话框 | tauri-plugin-dialog | ^2 | Tauri 文件对话框插件 |
| Shell | tauri-plugin-shell | ^2 | Tauri Shell 插件 |

## 前端依赖 (gqlite-gui)

| 功能 | 包名 | 版本 | 用途 |
|------|------|------|------|
| 前端框架 | svelte | ^5 | GUI 前端 UI 框架 |
| 构建工具 | vite | ^6 | 前端开发/构建 |
| CSS 框架 | tailwindcss | ^4 | 原子化 CSS |
| IPC | @tauri-apps/api | ^2 | Tauri 前后端通信 |
| 查询编辑器 | @codemirror/* | ^6 | GQL 查询编辑器 (view/state/language/lang-sql/commands) |
| 图可视化 | @antv/g6 | ^5 | 图数据力导向/层级布局可视化 |
| 文件对话框 | @tauri-apps/plugin-dialog | ^2 | 前端调用文件对话框 |

## 核心数据结构

| 结构 | 位置 | 用途 |
|------|------|------|
| Value | `types/value.rs` | 统一值类型 (Int64/Double/Bool/String/InternalId/Null/List/Date/DateTime/Duration) |
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
