# gqlite-cli 设计文档

gqlite 的交互式命令行 REPL，基于 rustyline 构建。

## 架构概览

```
┌─────────────────────────────────────────┐
│              main.rs                     │
│  ┌──────────┐  ┌──────────┐  ┌────────┐ │
│  │ 参数解析  │  │ REPL 循环 │  │Dot 命令│ │
│  └──────────┘  └──────────┘  └────────┘ │
│                     │                    │
│              ┌──────┴──────┐             │
│              │  helper.rs  │             │
│              │ (补全+提示)  │             │
│              └─────────────┘             │
└──────────────────┬──────────────────────┘
                   │
                   ▼
            ┌──────────────┐
            │  gqlite-core │
            │  (Database)  │
            └──────────────┘
```

## 使用方式

```bash
# 打开或创建数据库
gqlite mydb                    # → mydb.graph
gqlite mydb.graph              # 同上

# 只读模式
gqlite --read-only mydb

# 帮助和版本
gqlite --help
gqlite --version
```

### 命令行参数

| 参数 | 说明 |
|------|------|
| `<DATABASE>` | 数据库文件路径（必填，自动补 `.graph` 扩展名） |
| `--read-only` | 只读模式打开 |
| `-h`, `--help` | 显示帮助 |
| `-V`, `--version` | 显示版本号 |

## REPL 交互

### 提示符

```
gqlite> CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY(id));
OK (0.001s)

gqlite> MATCH (n:Person)
   ...> RETURN n.name;
n.name
------
Alice
Bob
(2 rows, 0.002s)
```

- `gqlite> ` — 等待新输入
- `   ...> ` — 多行续行（输入未以 `;` 结尾时）

### 多语句执行

单行输入多条语句，用分号分隔：

```
gqlite> CREATE NODE TABLE A(id INT64, PRIMARY KEY(id)); CREATE NODE TABLE B(id INT64, PRIMARY KEY(id));
OK (0.001s)
OK (0.001s)
```

每条语句独立执行，分别输出结果或错误。字符串中的分号不会被误拆分。

### 结果输出格式

查询结果以对齐列宽的表格形式输出：

```
name  | age
------+----
Alice | 30
Bob   | 25
(2 rows, 0.001s)
```

- DDL/DML 操作输出 `OK (耗时)`
- 查询输出表头 + 分隔线 + 数据行 + 行数统计

## Dot 命令

Dot 命令以 `.` 开头，仅在输入缓冲区为空时（新输入开始）生效。

| 命令 | 说明 |
|------|------|
| `.help` | 显示帮助信息和 GQL 示例 |
| `.quit` / `.exit` | 退出 gqlite |
| `.tables` | 列出所有表（节点表和关系表） |
| `.schema [TABLE]` | 显示表结构（省略表名则显示全部） |
| `.database` | 显示当前数据库信息（路径、只读状态、表数量） |
| `.open <PATH>` | 切换到另一个数据库文件 |
| `.checkpoint` | 手动触发 WAL checkpoint |

### 示例

```
gqlite> .tables
  Person (node)
  City (node)
  Knows (rel)

gqlite> .schema Person
Table: Person
  id INT64
  name STRING
  age INT64

gqlite> .database
Path:        /path/to/mydb.graph
Read-only:   false
Node tables: 2
Rel tables:  1
```

## Tab 补全和提示

基于 rustyline 的 `Completer` + `Hinter` + `Highlighter` 实现。

### 补全规则

| 输入上下文 | 补全来源 |
|-----------|---------|
| 以 `.` 开头 | Dot 命令（`.checkpoint`, `.database` 等） |
| 其他文本 | Cypher 关键字（`MATCH`, `CREATE`, `RETURN` 等） |

- 按 `Tab` 键显示候选列表
- 自动提示以灰色文本（ANSI `\x1b[90m`）显示在光标之后

### 支持的关键字

补全覆盖 48 个 Cypher 关键字，包括：
`ADD`, `ALL`, `ALTER`, `AND`, `AS`, `ASC`, `BEGIN`, `BOOL`, `BY`, `CALL`, `CASE`, `CAST`, `COLUMN`, `COMMIT`, `COPY`, `CREATE`, `DELETE`, `DELIMITER`, `DESC`, `DETACH`, `DISTINCT`, `DOUBLE`, `DROP`, `ELSE`, `END`, `EXISTS`, `FALSE`, `FROM`, `HEADER`, `IN`, `INT64`, `IS`, `KEY`, `LIMIT`, `MATCH`, `MERGE`, `NODE`, `NOT`, `NULL`, `ON`, `OPTIONAL`, `OR`, `ORDER`, `PRIMARY`, `REL`, `RENAME`, `RETURN`, `ROLLBACK`, `SERIAL`, `SET`, `SKIP`, `STRING`, `TABLE`, `THEN`, `TO`, `TRUE`, `UNION`, `UNWIND`, `WHEN`, `WHERE`, `WITH`, `YIELD`

## 历史记录

- 存储在 `~/.gqlite_history`
- 自动加载和保存
- 每条完整的语句（去除末尾分号）记录一条历史

## 模块说明

| 文件 | 职责 |
|------|------|
| `src/main.rs` | CLI 入口：参数解析、REPL 循环、Dot 命令处理、结果格式化 |
| `src/lib.rs` | 库入口，重导出 `helper` 模块 |
| `src/helper.rs` | rustyline Helper：Tab 补全 + 行内提示 + 提示高亮 |

## 依赖

| 依赖 | 用途 |
|------|------|
| `gqlite-core` | 数据库引擎 |
| `rustyline` (v14, derive) | 交互式行编辑器（readline 替代） |

## 测试

```bash
cargo test -p gqlite-cli
```

- `tests/helper_test.rs` — 补全匹配逻辑测试
