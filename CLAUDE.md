# CLAUDE.md — gqlite 项目配置

## 项目记忆入口

项目详细信息按模块分层存储：

| 记忆文件 | 技术索引 | 内容 |
|----------|----------|------|
| [memory.md](memory.md) | [technology.md](technology.md) | 项目架构、模块索引、进度追踪 |

**开发前请先阅读 memory.md 了解上下文，查阅 technology.md 确认可复用技术。**

## 开发约定

- 语言：Rust 2021 edition，最低 1.89
- 构建：`cargo build`，测试：`cargo test`
- 验收：`bash scripts/check.sh --strict`（fmt + clippy -D warnings + test）
- 错误处理：统一使用 `GqliteError` + `ErrorCode`，新增变体在 `error.rs`
- 锁获取顺序：catalog 先于 storage
- 生产化任务索引：`docs/tasks/000-index.md`（38 个任务，全部完成）

## Crate 结构规范

每个 crate（`crates/` 下的包）必须包含以下结构：

```
crates/<name>/
├── Cargo.toml    # 包定义与依赖
├── src/          # 源代码
├── tests/        # 集成测试（独立测试文件，通过公开 API 测试）
└── doc/          # 包相关文档（设计说明、API 文档、使用指南等）
```

- 测试代码放在 `tests/` 目录，不要内联在 `src/` 中
- 测试文件命名：`<module>_test.rs`
- 包相关文档维护在包内的 `doc/` 目录中，而非项目根目录

## 记忆更新策略

完成功能开发后，同步更新对应层级的记忆文件。

| 变更类型 | 更新文件 |
|----------|----------|
| 新增模块/依赖 | `memory.md` + `technology.md` |
| 任务完成 | `docs/tasks/000-index.md` 状态 |
| 架构变更 | `memory.md` 架构总览 |

## 技术复用优先

编写代码前，**必须**先检查 `technology.md` 是否有可复用方案。引入新技术后及时更新。
