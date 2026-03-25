# gqlite-core 设计文档

gqlite 的核心引擎库，提供存储引擎、查询规划、执行器、事务管理和持久化支持。

## 架构概览

```
                    ┌─────────────────────────────────────────────┐
                    │              Database / Connection           │
                    │  (公开 API 层 — 事务自动管理)                  │
                    └──────────────────┬──────────────────────────┘
                                       │
        ┌──────────┬──────────┬────────┴────────┬──────────┐
        ▼          ▼          ▼                 ▼          ▼
   ┌─────────┐ ┌────────┐ ┌─────────┐   ┌──────────┐ ┌─────────┐
   │ Parser  │ │ Binder │ │ Planner │   │ Executor │ │Functions│
   │(re-exp) │ │(语义绑定)│ │(逻辑+物理)│   │ (Pull模型) │ │ (标量+聚合)│
   └─────────┘ └────────┘ └─────────┘   └──────────┘ └─────────┘
                    │                         │
              ┌─────┴─────┐            ┌──────┴──────┐
              ▼           ▼            ▼             ▼
         ┌─────────┐ ┌─────────┐ ┌──────────┐ ┌──────────┐
         │ Catalog │ │ Storage │ │  WAL     │ │Procedure │
         │ (元数据)  │ │ (列存储)  │ │(预写日志)  │ │ (图算法)  │
         └─────────┘ └─────────┘ └──────────┘ └──────────┘
```

## 查询处理管线

```
SQL/Cypher 字符串
    │
    ▼
┌──────────┐  Statement
│  Parser  │ ──────────►
└──────────┘
    │
    ▼
┌──────────┐  BoundStatement
│  Binder  │ ──────────────►  (名称解析、类型检查)
└──────────┘
    │
    ▼
┌──────────┐  LogicalOperator
│ Planner  │ ──────────────►  (逻辑算子树)
└──────────┘
    │
    ▼
┌──────────┐  PhysicalPlan
│Optimizer │ ──────────────►  (谓词下推、投影裁剪)
└──────────┘
    │
    ▼
┌──────────┐  QueryResult
│ Executor │ ──────────────►  (Pull 模型逐行产出)
└──────────┘
```

## 模块说明

| 模块 | 路径 | 职责 |
|------|------|------|
| `catalog` | `src/catalog/` | 表元数据管理（节点表、关系表定义） |
| `storage` | `src/storage/` | 列式存储引擎（ColumnChunk、NodeGroup、CSR） |
| `binder` | `src/binder/` | 语义绑定和名称解析 |
| `planner` | `src/planner/` | 逻辑规划 + 物理规划 + 优化器 |
| `executor` | `src/executor/` | Pull 模型执行引擎 + 并行执行 |
| `functions` | `src/functions/` | 标量函数 + 聚合函数 + 日期时间函数 |
| `procedure` | `src/procedure/` | CALL 过程框架 + 图算法 |
| `transaction` | `src/transaction/` | SWMR 事务管理 + MVCC + WAL |
| `types` | `src/types/` | Value、DataType、InternalId |
| `error` | `src/error.rs` | 统一错误类型 GqliteError |
| `parser` | `src/parser/` | 对 gqlite-parser 的重导出 |

## 公开 API

### Database

主入口，`Clone + Send + Sync`（内部 `Arc<DatabaseInner>`）。

```rust
use gqlite_core::{Database, DatabaseConfig};

// 打开/创建数据库
let db = Database::open("mydb.graph")?;

// 自定义配置
let db = Database::open_with_config("mydb.graph", DatabaseConfig {
    buffer_pool_size: 512 * 1024 * 1024,  // 512 MB
    read_only: false,
    auto_checkpoint: true,
    checkpoint_threshold: 10_000,
})?;

// 内存数据库（无文件，无 WAL）
let db = Database::in_memory();

// 执行语句
let result = db.execute("MATCH (n:Person) RETURN n.name")?;

// 执行多条语句（分号分隔，遇错即停）
db.execute_script(
    "CREATE NODE TABLE T(id INT64, PRIMARY KEY(id)); \
     CREATE (n:T {id: 1})"
)?;

// 手动 checkpoint
db.checkpoint()?;

// 元数据查询
let tables = db.node_table_names();
let schema = db.table_schema("Person");
```

### Connection

每个线程使用独立的 Connection。

```rust
let conn = db.connect();

// 基本执行
let result = conn.execute("MATCH (n) RETURN n")?;

// 参数化查询
use std::collections::HashMap;
let mut params = HashMap::new();
params.insert("name".to_string(), Value::String("Alice".into()));
let result = conn.execute_with_params(
    "MATCH (n:Person) WHERE n.name = $name RETURN n",
    params,
)?;

// 预编译语句
let stmt = conn.prepare("MATCH (n:Person) WHERE n.id = $id RETURN n.name")?;
let result = stmt.execute(params)?;
```

### QueryResult

```rust
let result = db.execute("MATCH (n:Person) RETURN n.name, n.age")?;

// 元信息
result.num_rows();              // 行数
result.is_empty();              // 是否为空
result.column_names();          // ["n.name", "n.age"]

// 行访问
for row in result.rows() {
    let name = row.get_string(0);   // Option<&str>
    let age = row.get_int(1);       // Option<i64>
    let val = row.get(0);           // &Value
}

// 迭代器
for row in result {
    println!("{}", row);            // "Alice | 30"
}
```

### DatabaseConfig

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `buffer_pool_size` | 256 MB | 缓冲池大小 |
| `read_only` | `false` | 只读模式（拒绝写操作） |
| `auto_checkpoint` | `true` | 写事务后自动 checkpoint |
| `checkpoint_threshold` | 10,000 | 触发自动 checkpoint 的 WAL 记录数 |

### GqliteError

```rust
pub enum GqliteError {
    Io(std::io::Error),       // 文件 I/O 错误
    Parse(String),            // 解析错误
    Storage(String),          // 存储层错误
    Execution(String),        // 执行错误
    Transaction(String),      // 事务错误
    Other(String),            // 其他错误
}
```

## 存储引擎

### 列式存储

```
NodeTable
  └── Vec<ChunkedNodeGroup>
        └── NodeGroup (2048 行 × N 列)
              └── ColumnChunk
                    ├── data: Vec<u8>     (定长列) 或 Vec<String> (变长列)
                    └── null_mask: BitVec  (NULL 位图)
```

- **NodeGroup** 固定 2048 行，按列存储
- **ColumnChunk** 存储单列数据 + NULL 位图
- **ChunkedNodeGroup** 带元数据（已插入行数、删除计数）的 NodeGroup 包装

### CSR（Compressed Sparse Row）

关系表使用 CSR 格式存储邻接结构，支持高效图遍历：

```
CSRHeader: offset 数组（按节点 ID 索引）
CSRNodeGroup: 存储边的属性列（dst、edge 属性等）
PendingEdge: 未合并到 CSR 的新边（compact 后合并）
```

### 文件格式

```
.graph 文件布局:
┌──────────────┐  Page 0 (128 bytes)
│  FileHeader  │  magic, version, page_size, catalog/storage 偏移, checkpoint_ts
├──────────────┤  Page 1..N
│   Catalog    │  bincode 序列化（8 字节长度前缀 + payload）
├──────────────┤  Page N+1..M
│   Storage    │  bincode 序列化（同上）
└──────────────┘

.graph.wal 文件:
┌──────────────┐
│ WalRecord 1  │  txn_id + payload (DDL/DML/Commit)
├──────────────┤
│ WalRecord 2  │
├──────────────┤
│     ...      │
└──────────────┘
```

## 事务模型

### SWMR（Single Writer Multiple Readers）

- 读事务：不加锁，使用快照时间戳 `start_ts` 实现 MVCC
- 写事务：获取全局写锁（`parking_lot::RwLock`），同一时刻只有一个写事务
- 锁获取顺序：catalog → storage（防止死锁）

### MVCC

每行记录包含版本元数据：
- `create_ts` — 创建该行的事务 ID
- `delete_ts` — 删除该行的事务 ID（0 表示未删除）

读事务根据 `start_ts` 判断行可见性：`create_ts <= start_ts && (delete_ts == 0 || delete_ts > start_ts)`

### WAL（Write-Ahead Log）

所有写操作先写入 WAL，事务提交时追加 `TxnCommit` 记录。

**恢复流程（两阶段）**：
1. 从 `.graph` 主文件加载 Catalog + Storage（如果有效）
2. 增量回放 WAL（仅回放 `txn_id > checkpoint_ts` 的已提交记录）

**Checkpoint（崩溃安全）**：
1. 序列化 Catalog + Storage 到 `.graph.tmp`
2. 原子 rename `.graph.tmp` → `.graph`
3. 清空 WAL

## 内置函数

### 标量函数

| 类别 | 函数 |
|------|------|
| 字符串 | `lower`, `upper`, `trim`, `ltrim`, `rtrim`, `length`/`size`, `concat`, `contains`, `starts_with`, `ends_with`, `substring`, `replace`, `reverse`, `left`, `right`, `lpad`, `rpad`, `repeat` |
| 数学 | `abs`, `ceil`, `floor`, `round`, `sqrt`, `log`, `log10`, `sign`, `rand`, `pi`, `e` |
| 类型转换 | `to_string`, `toInteger`, `toFloat`, `CAST(expr AS type)` |
| 列表 | `list_len`, `list_extract`, `list_append`, `list_prepend`, `list_concat`, `list_contains`, `list_reverse`, `list_sort`, `list_distinct`, `range` |
| 路径 | `nodes` |
| 日期时间 | `date`, `datetime`, `timestamp` |
| 其他 | `coalesce` |

### 聚合函数

`count`, `sum`, `avg`, `min`, `max`, `collect`

## 图算法（CALL 过程）

| 过程名 | 说明 | 参数 |
|--------|------|------|
| `dbms.tables` | 列出所有表 | 无 |
| `gds.degree_centrality` | 度中心性 | `node_table`, `rel_table` |
| `gds.wcc` | 弱连通分量 | `node_table`, `rel_table` |
| `gds.dijkstra` | Dijkstra 最短路径 | `node_table`, `rel_table`, `source_id`, `target_id`, `weight_property` |
| `gds.pagerank` | PageRank | `node_table`, `rel_table` |
| `gds.label_propagation` | 标签传播（社区检测） | `node_table`, `rel_table` |
| `gds.triangle_count` | 三角形计数 | `node_table`, `rel_table` |
| `gds.betweenness` | 介数中心性（Brandes） | `node_table`, `rel_table` |

用法示例：
```cypher
CALL gds.pagerank('Person', 'Knows') YIELD node_id, score
```

## 并行执行

- 使用 rayon 对 HashJoin 和 Union 的独立子树并行执行
- Pipeline 切分支持分布式执行规划
- 变长路径遍历使用 BFS + 深度限制 + 环检测

## 依赖关系

| 依赖 | 用途 |
|------|------|
| `gqlite-parser` | Cypher 解析器 |
| `serde` + `bincode` | 序列化（Catalog、Storage 持久化） |
| `parking_lot` | 高性能互斥锁 |
| `bitvec` | NULL 位图 |
| `ahash` | 快速哈希（HashMap） |
| `rayon` | 并行执行 |
| `crc32fast` | WAL 记录校验 |
| `chrono` | 日期时间类型 |
| `regex` | 正则匹配 (`=~`) |
| `uuid` | ID 生成 |

## 测试

```bash
cargo test -p gqlite-core
```

45 个测试文件，覆盖：
- 核心功能：`basic_test`, `engine_test`, `logical_test`, `optimizer_test`, `binder_test`
- 存储层：`pager_test`, `column_chunk_test`, `node_group_test`, `csr_test`, `table_test`
- 查询处理：`scalar_test`, `aggregate_test`, `case_expr_test`, `in_expr_test`, `regex_test`
- 事务持久化：`transaction_test`, `wal_test`, `persistence_test`
- 图算法：`shortest_path_test`, `pagerank_test`, `dijkstra_test`, `wcc_test`, `lpa_test`, `betweenness_test`, `triangle_count_test`
- 高级特性：`exists_test`, `subquery_test`, `datetime_test`, `list_comprehension_test`, `parallel_test`
