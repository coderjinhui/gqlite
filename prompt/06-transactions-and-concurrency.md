# Agent 06 - 事务与并发控制

你需要调研 Kuzu 嵌入式图数据库的**事务与并发控制**功能，然后把调研结果写成中文 markdown 文档保存到 `/Users/I520239/Desktop/codes/ai/graphdb/research/06-transactions-and-concurrency.md`。

调研重点：
1. ACID 事务支持
2. 事务隔离级别 - 可串行化 (Serializable)
3. MVCC (多版本并发控制) 实现
4. WAL (Write-Ahead Logging) 机制
5. 读写事务 vs 只读事务
6. 并发模型 - 单写多读 (SWMR)
7. 检查点 (Checkpoint) 机制
8. 事务回滚
9. 自动提交 vs 手动事务控制
10. 死锁检测与预防
11. 持久性保证和崩溃恢复

请通过网络搜索和 Kuzu 官方文档获取信息。

重要：因为目标项目 gqlite 是 Rust 实现的，在文档最后加一节 "Rust 实现要点"，分析如何用 Rust 实现事务系统。考虑 Rust 的 RwLock 用于 SWMR、Arc 用于共享状态、以及如何利用 Rust 的所有权系统保证事务安全性。参考 SQLite 的 WAL 实现思路。
