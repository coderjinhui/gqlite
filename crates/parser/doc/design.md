# gqlite-parser 设计文档

独立的 Cypher 查询语言解析器，无存储引擎依赖，可单独使用。

## 架构概览

```
Input String
    │
    ▼
┌──────────┐   Vec<Token>   ┌──────────┐   Statement (AST)
│  Lexer   │ ─────────────► │  Parser  │ ──────────────────►
│ (logos)  │                │ (递归下降) │
└──────────┘                └──────────┘
```

**三阶段处理**：
1. **词法分析** (`token.rs`) — logos 驱动的 Lexer，将输入字符串拆分为 Token 流
2. **语法分析** (`parser.rs`) — 递归下降解析器，将 Token 流构建为 AST
3. **AST** (`ast.rs`) — 类型化的语法树节点，供下游 binder/planner 消费

## 模块说明

| 模块 | 文件 | 职责 |
|------|------|------|
| `token` | `src/token.rs` | Token 定义 + `tokenize()` 函数 |
| `parser` | `src/parser.rs` | 递归下降解析器 |
| `ast` | `src/ast.rs` | AST 节点定义 |
| `data_type` | `src/data_type.rs` | 数据类型枚举 |

## 公开 API

```rust
use gqlite_parser::{Parser, DataType, ParseError};

// 解析单条语句
let stmt = Parser::parse_query("MATCH (n:Person) RETURN n.name")?;

// 解析多条语句（分号分隔）
let stmts = Parser::parse_all(
    "CREATE NODE TABLE T(id INT64, PRIMARY KEY(id)); \
     CREATE (n:T {id: 1})"
)?;
```

### Parser 方法

| 方法 | 说明 |
|------|------|
| `Parser::parse_query(input)` | 解析单条语句，返回 `Result<Statement, ParseError>` |
| `Parser::parse_all(input)` | 解析多条分号分隔语句，返回 `Result<Vec<Statement>, ParseError>` |

### ParseError

```rust
pub enum ParseError {
    Parse(String),  // 语法错误
    Lex(String),    // 词法错误
}
```

## 支持的语法

### 数据类型

| 类型 | Rust 表示 | 字节大小 |
|------|----------|---------|
| `BOOL` | `bool` | 1 |
| `INT64` | `i64` | 8 |
| `DOUBLE` | `f64` | 8 |
| `STRING` | `String` | 变长 |
| `SERIAL` | `i64`（自增） | 8 |
| `DATE` | `i32`（CE 纪元天数） | 4 |
| `DATETIME` | `i64`（Unix 毫秒） | 8 |
| `DURATION` | `i64`（毫秒） | 8 |
| `INTERNAL_ID` | `u32 + u64` | 12 |

### DDL 语句

```cypher
-- 创建节点表
CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY(id))

-- 创建关系表
CREATE REL TABLE Knows (FROM Person TO Person, since INT64)

-- 删除表
DROP TABLE Person

-- 修改表
ALTER TABLE Person ADD COLUMN age INT64
ALTER TABLE Person DROP COLUMN age
ALTER TABLE Person RENAME TO People
ALTER TABLE Person RENAME COLUMN name TO fullname
```

### DML 语句

```cypher
-- 查询
MATCH (n:Person) WHERE n.age > 18 RETURN n.name, n.age ORDER BY n.age DESC LIMIT 10

-- 可选匹配
OPTIONAL MATCH (n:Person)-[:Knows]->(m) RETURN n.name, m.name

-- 创建节点
CREATE (n:Person {id: 1, name: 'Alice'})

-- 更新属性
MATCH (n:Person) WHERE n.id = 1 SET n.name = 'Bob'

-- 删除节点
MATCH (n:Person) WHERE n.id = 1 DELETE n
MATCH (n:Person) WHERE n.id = 1 DETACH DELETE n

-- MERGE（存在则匹配，不存在则创建）
MERGE (n:Person {id: 1}) ON CREATE SET n.name = 'Alice' ON MATCH SET n.name = 'Updated'

-- UNWIND（展开列表）
UNWIND [1, 2, 3] AS x RETURN x

-- WITH（管道化中间结果）
MATCH (n:Person) WITH n.name AS name WHERE name STARTS WITH 'A' RETURN name

-- UNION
MATCH (n:Person) RETURN n.name UNION ALL MATCH (m:City) RETURN m.name

-- COPY 导入导出
COPY Person FROM 'data.csv' (HEADER=true, DELIMITER=',')
COPY Person TO 'output.csv'

-- CALL 过程调用
CALL dbms.tables() YIELD name, type

-- CALL { 子查询 }
MATCH (n:Person) CALL { WITH n MATCH (n)-[:Knows]->(m) RETURN count(m) AS cnt } RETURN n.name, cnt
```

### 表达式

| 类别 | 语法示例 |
|------|---------|
| 字面量 | `42`, `3.14`, `'text'`, `TRUE`, `FALSE`, `NULL` |
| 参数 | `$param_name` |
| 属性访问 | `n.name` |
| 算术 | `+`, `-`, `*`, `/`, `%` |
| 比较 | `=`, `<>`, `!=`, `<`, `>`, `<=`, `>=` |
| 逻辑 | `AND`, `OR`, `NOT` |
| NULL 检查 | `IS NULL`, `IS NOT NULL` |
| 正则匹配 | `n.name =~ 'A.*'` |
| IN 表达式 | `n.id IN [1, 2, 3]` |
| CASE | `CASE WHEN ... THEN ... ELSE ... END` |
| CAST | `CAST(n.id AS STRING)` |
| EXISTS | `EXISTS { MATCH (n)-[:Knows]->() }` |
| 列表 | `[1, 2, 3]` |
| 列表推导 | `[x IN list WHERE x > 0 \| x * 2]` |
| 函数调用 | `count(*)`, `sum(n.age)`, `lower(n.name)` |

### 路径模式

```cypher
-- 固定长度
(a)-[:KNOWS]->(b)
(a)<-[:KNOWS]-(b)
(a)-[:KNOWS]-(b)        -- 双向

-- 变长路径
(a)-[:KNOWS*1..3]->(b)  -- 1到3跳
(a)-[:KNOWS*]->(b)      -- 任意跳数
(a)-[:KNOWS*..5]->(b)   -- 最多5跳

-- 最短路径
p = shortestPath((a)-[:KNOWS*]->(b))
p = allShortestPaths((a)-[:KNOWS*]->(b))
```

## Token 系统

基于 [logos](https://docs.rs/logos) 的零拷贝词法分析器。

**特性**：
- 大小写不敏感的关键字匹配（`MATCH` = `match` = `Match`）
- 单引号字符串，支持转义：`\n`, `\t`, `\\`, `\'`, `\"`
- 行注释 `//` 和块注释 `/* */`
- 自动跳过空白字符

**Token 分类**：
- 关键字：48 个 Cypher 关键字
- 类型关键字：`INT64`, `DOUBLE`, `STRING`, `BOOL`, `SERIAL`
- 字面量：整数、浮点数、字符串、参数（`$name`）、标识符
- 符号：`(`, `)`, `[`, `]`, `{`, `}`, `->`, `<-`, `=~` 等

## 测试

```bash
cargo test -p gqlite-parser
```

测试文件：
- `tests/parser_test.rs` — 语句解析测试 + `parse_all` 多语句测试
- `tests/token_test.rs` — 词法分析测试
- `tests/data_type_test.rs` — 数据类型测试
