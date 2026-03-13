# Agent 05 - 索引机制

你需要调研 Kuzu 嵌入式图数据库的**索引机制**，然后把调研结果写成中文 markdown 文档保存到 `/Users/I520239/Desktop/codes/ai/graphdb/research/05-indexing.md`。

调研重点：
1. 主键索引 (Primary Key Index) - 基于哈希的索引、索引结构
2. 内部 ID (Internal ID / offset) 系统 - 节点和关系的内部标识
3. Zone Maps / Min-Max 索引 - 用于列式存储的跳过不必要数据块
4. 哈希索引 (Hash Index) - overflow 处理、动态扩展
5. 索引在查询中的使用场景
6. 是否支持用户自定义索引（二级索引）
7. 全文搜索索引（如果有）
8. 索引的持久化和恢复
9. 索引与并发控制的交互

请通过网络搜索和 Kuzu 官方文档、源码信息获取信息。

重要：因为目标项目 gqlite 是 Rust 实现的，在文档最后加一节 "Rust 实现要点"，分析如何用 Rust 实现这些索引结构。考虑使用 Rust 的 HashMap 优化、B-tree 实现、以及内存安全的并发索引访问模式。
