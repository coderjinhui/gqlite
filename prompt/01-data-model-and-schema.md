# Agent 01 - 数据模型与Schema管理

你需要调研 Kuzu 嵌入式图数据库的**数据模型与Schema管理**功能，然后把调研结果写成中文 markdown 文档保存到 `/Users/I520239/Desktop/codes/ai/graphdb/research/01-data-model-and-schema.md`。

调研重点：
1. Kuzu 的属性图模型 (Property Graph Model) - 节点表(Node Table)、关系表(Rel Table)、RDF图
2. 支持的数据类型 (INT, FLOAT, STRING, BOOL, DATE, LIST, MAP, STRUCT, UNION 等)
3. Schema 操作 (CREATE NODE TABLE, CREATE REL TABLE, ALTER, DROP)
4. 关系表的方向性（有向/无向）
5. 关系的多重性约束 (ONE-TO-ONE, ONE-TO-MANY, MANY-TO-MANY)
6. 主键定义和约束
7. 节点/关系属性的默认值
8. RDFGraph 支持

请通过网络搜索和 Kuzu 官方文档获取信息。文档要详细，包含具体语法示例。

重要：因为目标项目 gqlite 是 Rust 实现的，在文档最后加一节 "Rust 实现要点"，分析在 Rust 中实现这些功能需要的关键数据结构和设计考量。例如用 enum 表示数据类型、trait 设计模式等。
