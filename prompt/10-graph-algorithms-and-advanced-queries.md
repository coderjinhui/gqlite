# Agent 10 - 图算法与高级查询

你需要调研 Kuzu 嵌入式图数据库的**图算法与高级查询功能**，然后把调研结果写成中文 markdown 文档保存到 `/Users/I520239/Desktop/codes/ai/graphdb/research/10-graph-algorithms-and-advanced-queries.md`。

调研重点：
1. 可变长路径查询 (Variable-Length Path Queries)
   - 语法: (a)-[r:REL*min..max]->(b)
   - 路径过滤 (Kleene Star)
   - 递归关系模式
2. 最短路径算法
   - 单源最短路径
   - ALL SHORTEST PATHS
   - BFS 最短路径
3. 路径语义
   - WALK / TRAIL / ACYCLIC 语义
   - 路径过滤表达式
4. 递归查询 (Recursive Queries)
5. 图投影 (Graph Projection)
   - 创建子图用于特定查询
6. 模式匹配的高级功能
   - 可选匹配 (OPTIONAL MATCH)
   - 多模式匹配
   - 负模式匹配（不存在某模式）
7. 与 GQL/ISO 标准的兼容性

请通过网络搜索和 Kuzu 官方文档获取信息。每个功能都给出具体语法示例。

重要：因为目标项目 gqlite 是 Rust 实现的，在文档最后加一节 "Rust 实现要点"，分析如何用 Rust 实现图遍历算法。考虑使用 petgraph 库的设计理念、BFS/DFS 迭代器模式、以及如何在 CSR 结构上高效实现路径查询。
