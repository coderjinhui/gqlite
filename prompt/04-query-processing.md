# Agent 04 - 查询处理管线

你需要调研 Kuzu 嵌入式图数据库的**查询处理管线 (Query Processing Pipeline)**，然后把调研结果写成中文 markdown 文档保存到 `/Users/I520239/Desktop/codes/ai/graphdb/research/04-query-processing.md`。

调研重点：
1. 查询处理整体流程 - Parser → Binder → Planner → Optimizer → Executor
2. 解析器 (Parser) - 如何将 Cypher 文本解析为 AST
3. 绑定器 (Binder) - 名称解析、类型检查、语义验证
4. 逻辑计划 (Logical Plan) - 逻辑算子类型（Scan, Filter, Project, Join, Aggregate, Order 等）
5. 查询优化器 (Optimizer) - 优化规则和策略
   - 谓词下推 (Predicate Pushdown)
   - 投影下推 (Projection Pushdown)
   - 连接顺序优化 (Join Order Optimization)
   - 基于代价的优化 (Cost-Based Optimization)
   - 侧向扁平化 (Sideways Information Passing)
6. 物理计划 (Physical Plan) - 物理算子
7. 向量化执行 (Vectorized Execution) - 批量处理模型
8. Morsel-Driven 并行执行模型
9. Factorized Execution - 关系分解执行
10. 执行引擎的 Pipeline 模型

请通过网络搜索 Kuzu 官方文档、博客文章、学术论文（特别是关于 factorized execution 和 worst-case optimal join 的论文）获取信息。

重要：因为目标项目 gqlite 是 Rust 实现的，在文档最后加一节 "Rust 实现要点"，分析如何用 Rust 实现查询处理管线。考虑 Rust 的 enum 用于表示算子树、trait object 用于执行器接口、rayon 用于并行执行等。
