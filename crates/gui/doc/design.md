# gqlite-gui 设计文档

## 概述

gqlite-gui 是 gqlite 图数据库的跨平台桌面 GUI 管理工具，基于 Tauri v2 构建，提供 GQL 查询编辑器、数据库管理、表浏览、图可视化等功能。

## 技术架构

```
┌─────────────────────────────────────────────┐
│              Svelte 5 Frontend              │
│  ┌─────────┐ ┌──────────┐ ┌─────────────┐  │
│  │ Query   │ │ Tables   │ │ Graph View  │  │
│  │ Editor  │ │ Browser  │ │ (@antv/g6)  │  │
│  │(CodeMir)│ │          │ │             │  │
│  └────┬────┘ └────┬─────┘ └──────┬──────┘  │
│       └───────────┼──────────────┘          │
│              Tauri IPC (invoke)              │
├─────────────────────────────────────────────┤
│              Rust Backend (Tauri v2)         │
│  ┌──────────┐ ┌────────┐ ┌──────┐ ┌──────┐ │
│  │ database │ │ query  │ │schema│ │graph │ │
│  │ commands │ │commands│ │ cmds │ │ cmds │ │
│  └────┬─────┘ └───┬────┘ └──┬───┘ └──┬───┘ │
│       └───────────┼─────────┘        │      │
│              AppState (Mutex<DB>)     │      │
│                    │                  │      │
│              gqlite-core              │      │
└─────────────────────────────────────────────┘
```

## 技术栈

| 组件 | 技术 | 版本 |
|------|------|------|
| 桌面框架 | Tauri | v2 |
| 前端框架 | Svelte | v5 |
| 构建工具 | Vite | v6 |
| CSS 框架 | Tailwind CSS | v4 |
| 查询编辑器 | CodeMirror | v6 |
| 图可视化 | @antv/G6 | v5 |
| 前端语言 | TypeScript | v5 |

## 目录结构

```
crates/gui/
├── Cargo.toml (空，非 Rust crate)
├── package.json
├── vite.config.ts
├── svelte.config.js
├── tsconfig.json
├── index.html
├── src-tauri/           # Rust 后端
│   ├── Cargo.toml
│   ├── tauri.conf.json
│   ├── capabilities/
│   └── src/
│       ├── main.rs
│       ├── lib.rs
│       ├── state.rs     # AppState (数据库连接管理)
│       └── commands/    # Tauri IPC 命令
│           ├── database.rs  # 打开/关闭/信息/检查点
│           ├── query.rs     # GQL 执行
│           ├── schema.rs    # 表结构查询
│           └── graph.rs     # 图数据查询
├── src/                 # Svelte 前端
│   ├── App.svelte
│   ├── app.css          # 全局样式 + CSS 变量主题
│   ├── main.ts
│   ├── lib/
│   │   ├── api.ts       # Tauri IPC 调用封装
│   │   ├── i18n.ts      # 国际化 + 主题切换
│   │   └── types.ts     # TypeScript 类型定义
│   ├── stores/
│   │   └── database.ts  # 全局状态管理
│   ├── components/
│   │   ├── Layout.svelte       # 主布局
│   │   ├── Sidebar.svelte      # 侧边栏（数据库/表列表）
│   │   ├── StatusBar.svelte    # 底部状态栏
│   │   ├── QueryEditor.svelte  # CodeMirror 编辑器
│   │   ├── ResultTable.svelte  # 查询结果表格
│   │   ├── TableBrowser.svelte # 表数据浏览
│   │   └── GraphView.svelte    # G6 图可视化
│   └── pages/
│       ├── Query.svelte   # 查询页面（多 Tab）
│       ├── Tables.svelte  # 表管理页面
│       └── Graph.svelte   # 图可视化页面
├── tests/
├── doc/
└── static/
```

## Rust 后端命令

### database 命令
- `open_database(path)` → 打开或创建数据库
- `close_database()` → 关闭数据库
- `get_database_info()` → 获取数据库信息
- `checkpoint()` → 手动检查点

### query 命令
- `execute_query(query)` → 执行 GQL 查询，返回 JSON 结果
- Value → JSON 转换：Int→number, Float→number, String→string, Bool→boolean, Null→null, InternalId→"table_id:offset", Date/DateTime→ISO string

### schema 命令
- `get_tables()` → 获取所有表信息（节点表/关系表）
- `get_table_schema(table_name)` → 获取表结构
- `get_table_data(table_name, limit, offset)` → 分页获取表数据

### graph 命令
- `get_graph_data(node_table, rel_table, limit)` → 获取图数据（节点+边）

## 前端功能

### 查询页面
- CodeMirror 6 编辑器 + SQL 语法高亮
- Ctrl/Cmd+Enter 快捷执行
- 多 Tab 查询编辑器
- 查询结果表格（列名+类型、行号）
- 执行耗时和行数统计

### 表浏览页面
- 左侧表列表（N=节点, R=关系）
- 表结构查看（列名、类型）
- 数据分页浏览（SKIP/LIMIT）

### 图可视化页面
- @antv/G6 v5 力导向布局
- 节点按表名着色
- 边带箭头和关系标签
- 拖拽、缩放、点击选择交互
- 迷你地图
- 节点表/关系表筛选器
- 节点数量限制（默认200）

### 通用功能
- 中英双语切换
- 深色/浅色主题
- 数据库打开/创建/关闭
- 检查点操作
- 文件对话框（Tauri dialog 插件）

## 开发命令

```bash
cd crates/gui
npm install           # 安装前端依赖
npm run tauri dev     # 启动开发服务器
npm run tauri build   # 构建生产版本
cargo check -p gqlite-gui  # 检查 Rust 编译
```
