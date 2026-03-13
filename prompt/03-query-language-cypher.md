# Agent 03 - 查询语言 Cypher 支持

你需要调研 Kuzu 嵌入式图数据库的**查询语言 (Cypher) 支持**，然后把调研结果写成中文 markdown 文档保存到 `/Users/I520239/Desktop/codes/ai/graphdb/research/03-query-language-cypher.md`。

调研重点：
1. Kuzu 支持的 Cypher 子集概览
2. 模式匹配 (MATCH) - 节点模式、关系模式、可变长路径、最短路径
3. 过滤 (WHERE) - 比较运算符、逻辑运算符、列表操作、模式匹配谓词
4. 返回 (RETURN) - 投影、别名、DISTINCT、ORDER BY、SKIP、LIMIT
5. 聚合函数 - COUNT, SUM, AVG, MIN, MAX, COLLECT 等
6. 创建/更新/删除 (CREATE, SET, DELETE, MERGE)
7. WITH 子句 - 查询链式组合
8. UNWIND - 列表展开
9. UNION / UNION ALL
10. 子查询 (EXISTS, COUNT subquery, OPTIONAL MATCH)
11. 宏/函数定义
12. CASE WHEN 表达式
13. 内置函数 - 字符串函数、数值函数、日期函数、列表函数
14. LOAD FROM (从外部文件查询)
15. COPY FROM / COPY TO (批量导入导出)

请通过网络搜索和 Kuzu 官方文档获取信息。每个功能都给出具体语法示例。

重要：因为目标项目 gqlite 是 Rust 实现的，在文档最后加一节 "Rust 实现要点"，分析如何用 Rust 实现 Cypher 解析器（词法分析、语法分析、AST 设计）。考虑使用 nom、pest、lalrpop 等 Rust parser 库，或手写递归下降解析器。
