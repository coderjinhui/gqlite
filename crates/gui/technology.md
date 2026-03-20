# gqlite-gui 技术栈索引

开发前请先查阅此文件，优先复用已有技术方案。

## Rust 依赖 (src-tauri/Cargo.toml)

| 功能 | Crate | 版本 | 用途 |
|------|-------|------|------|
| 图数据库引擎 | gqlite-core | path | 核心数据库操作（`Database::open/execute/checkpoint`） |
| 桌面框架 | tauri | ^2 | Tauri v2 应用框架 + IPC 命令 |
| Shell 插件 | tauri-plugin-shell | ^2 | 打开外部链接 |
| 序列化 | serde + serde_json | ^1 | 命令返回值 JSON 序列化 |

> `tauri-plugin-dialog` 已移除（rfd macOS 崩溃），使用文本输入框替代

## 前端依赖 (package.json)

| 功能 | 包名 | 版本 | 用途 |
|------|------|------|------|
| UI 框架 | svelte | ^5 | Svelte 5 runes（`$state`, `$derived`, `$effect`） |
| 构建 | vite | ^6 | 开发服务器 + 打包 |
| CSS | tailwindcss | ^4 | 原子化 CSS（通过 `@tailwindcss/vite` 插件） |
| IPC 通信 | @tauri-apps/api | ^2 | `invoke()` 调用 Rust 命令 |
| 查询编辑器 | @codemirror/view, state, language, lang-sql, commands | ^6 | CodeMirror 6 编辑器 |
| 图可视化 | @antv/g6 | ^5 | 力导向布局、minimap（动态 import 延迟加载） |

## 已有模式

| 模式 | 位置 | 说明 |
|------|------|------|
| Value → JSON 转换 | `commands/query.rs:value_to_json()` | 所有需要将 gqlite Value 转 JSON 的地方复用 |
| Tauri IPC 封装 | `src/lib/api.ts` | 所有 `invoke()` 调用统一在此，新增命令在此添加 |
| 类型定义 | `src/lib/types.ts` | 前端接口与 Rust 结构体对应，新增结构体同步添加 |
| 国际化 | `src/lib/i18n.ts` | 新增文案在 `translations` 对象的 en/zh 中添加 |
| 全局状态 | `src/stores/database.ts` | Svelte writable store，新增全局状态在此添加 |
| 主题 | `src/app.css` | CSS 变量，组件用 `var(--xxx)` 引用 |
| 表数据查询 | `commands/schema.rs:get_table_data` | 用 `table_schema()` 展开列名，避免 `RETURN n` 只返回 InternalId |
| 图节点 PK | `commands/graph.rs` | 通过 `db.inner.catalog.read()` 获取 `primary_key_idx` |
