# gqlite-gui — 桌面 GUI 管理工具

## 模块职责

跨平台桌面 GUI，提供数据库管理、GQL 查询、表浏览、图可视化功能。基于 Tauri v2 (Rust 后端 IPC) + Svelte 5 (前端 UI)。

## 架构

```
用户操作 → Svelte 前端 → invoke() IPC → Rust Tauri 命令 → gqlite-core Database API → 返回 JSON
```

前端与后端通过 Tauri IPC (`@tauri-apps/api/core` 的 `invoke()`) 通信，Rust 侧用 `#[tauri::command]` 注册命令。

## 目录结构

| 目录 | 说明 |
|------|------|
| `src-tauri/src/` | Rust 后端：Tauri 命令、应用状态 |
| `src/components/` | Svelte UI 组件 |
| `src/pages/` | 页面组件（Query, Tables, Graph） |
| `src/lib/` | API 封装、类型定义、国际化 |
| `src/stores/` | Svelte store（全局状态） |

## Rust 后端 (src-tauri/)

### 状态管理

`AppState`（`state.rs`）：`Mutex<Option<Database>>` + `Mutex<Option<String>>`，通过 `tauri::State` 注入到所有命令。

### Tauri 命令

| 命令 | 文件 | 功能 |
|------|------|------|
| `open_database` | `commands/database.rs` | 打开/创建数据库，返回 DbInfo |
| `close_database` | `commands/database.rs` | 关闭数据库 |
| `get_database_info` | `commands/database.rs` | 获取当前数据库信息 |
| `checkpoint` | `commands/database.rs` | 手动 checkpoint |
| `execute_query` | `commands/query.rs` | 执行 GQL 语句，返回 QueryResponse |
| `get_tables` | `commands/schema.rs` | 获取所有表（node + rel），含行数 |
| `get_table_schema` | `commands/schema.rs` | 获取表列定义 |
| `get_table_data` | `commands/schema.rs` | 分页查询表数据（SKIP/LIMIT） |
| `get_graph_data` | `commands/graph.rs` | 查询图数据（节点+边），返回 GraphData + PK |

### 关键函数

`value_to_json()`（`commands/query.rs`）：将 `gqlite_core::Value` 转为 `serde_json::Value`，被多个命令复用。

### 已知限制

- gqlite 不支持 `RETURN` 关系变量（如 `RETURN r`），边查询只能 `RETURN a, b`
- `RETURN n` 只返回 `Value::InternalId`，需用 `RETURN n.col1, n.col2` 获取属性值
- `tauri-plugin-dialog`（rfd）在 macOS 崩溃，已改用文本输入框替代原生文件对话框
- 访问 catalog PK 信息需直接操作 `db.inner.catalog.read()`

## Svelte 前端 (src/)

### 页面

| 页面 | 文件 | 功能 |
|------|------|------|
| Query | `pages/Query.svelte` | 多 Tab GQL 编辑器 + 结果表格 |
| Tables | `pages/Tables.svelte` | 表数据浏览（schema + 分页数据） |
| Graph | `pages/Graph.svelte` | 图可视化（选择节点表/关系表） |

### 组件

| 组件 | 文件 | 说明 |
|------|------|------|
| Layout | `components/Layout.svelte` | 主布局：Sidebar + Tab 栏 + 内容区 + StatusBar |
| Sidebar | `components/Sidebar.svelte` | 数据库管理、表列表、语言/主题切换 |
| QueryEditor | `components/QueryEditor.svelte` | CodeMirror 6 编辑器（SQL 高亮、Ctrl+Enter 执行） |
| ResultTable | `components/ResultTable.svelte` | 查询结果表格 |
| TableBrowser | `components/TableBrowser.svelte` | 左栏表列表 + 右栏 schema/data 视图 |
| GraphView | `components/GraphView.svelte` | @antv/G6 力导向图 + 节点详情面板 |
| StatusBar | `components/StatusBar.svelte` | 连接状态、表统计 |

### 数据流

- `stores/database.ts`：全局 store — `dbInfo`, `tables`, `currentPage`, `errorMessage`
- `lib/api.ts`：封装所有 `invoke()` 调用，统一类型
- `lib/types.ts`：TypeScript 接口定义，与 Rust 结构体一一对应
- `lib/i18n.ts`：中英双语翻译 + `locale` / `darkMode` store + `toggleLocale()` / `toggleTheme()`

### 主题系统

CSS 变量定义在 `app.css`，通过 `.dark` class 切换深色模式。组件用 `style="color: var(--text-primary)"` 引用。

## 开发命令

```bash
cd crates/gui
npm run tauri dev     # 启动开发服务器（前端 HMR + Rust 热编译）
npm run tauri build   # 打包桌面应用
cargo check -p gqlite-gui  # 仅检查 Rust 编译
```
