# Agent 08 - 扩展系统与API接口

你需要调研 Kuzu 嵌入式图数据库的**扩展系统与API接口**，然后把调研结果写成中文 markdown 文档保存到 `/Users/I520239/Desktop/codes/ai/graphdb/research/08-extensions-and-api.md`。

调研重点：
1. 嵌入式使用方式 - 作为库嵌入应用程序
2. 语言绑定
   - C/C++ API
   - Python API
   - Node.js API
   - Java API
   - Rust API (如果有)
3. 核心 API 设计
   - Database 类 - 数据库创建和配置
   - Connection 类 - 连接管理
   - PreparedStatement - 预编译语句
   - QueryResult - 结果集遍历
4. 扩展系统 (Extension System)
   - 扩展类型（httpfs, duckdb, postgres 等）
   - 扩展加载机制 (INSTALL / LOAD)
   - 自定义函数扩展
5. 数据库附加 (ATTACH) - 连接外部数据库 (DuckDB, PostgreSQL 等)
6. 配置选项 - buffer_pool_size, threads 等
7. CLI 工具 - Kuzu Shell 功能

请通过网络搜索和 Kuzu 官方文档获取信息。

重要：因为目标项目 gqlite 是 Rust 实现的，在文档最后加一节 "Rust 实现要点"，重点分析如何设计 Rust 原生 API：
- Database/Connection/Statement/Result 的 Rust API 设计
- FFI 接口设计（为其他语言提供 C ABI）
- 扩展系统的 Rust trait 设计
- 如何提供 Python 绑定 (PyO3)
