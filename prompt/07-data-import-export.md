# Agent 07 - 数据导入导出

你需要调研 Kuzu 嵌入式图数据库的**数据导入导出与外部数据源**功能，然后把调研结果写成中文 markdown 文档保存到 `/Users/I520239/Desktop/codes/ai/graphdb/research/07-data-import-export.md`。

调研重点：
1. COPY FROM - 批量数据导入
   - CSV 格式支持和选项 (delimiter, header, escape 等)
   - Parquet 格式支持
   - JSON 格式支持
   - NumPy 格式支持
   - 从 Pandas DataFrame 导入
2. COPY TO - 数据导出
   - 导出为 CSV
   - 导出为 Parquet
3. LOAD FROM - 直接从外部文件查询（不导入）
   - 扫描 CSV/Parquet/JSON
   - 附加其他数据库 (Attach)
4. 批量导入的性能优化策略
5. 数据类型映射 - 外部格式到 Kuzu 类型的映射
6. 错误处理 - 导入时的错误行为

请通过网络搜索和 Kuzu 官方文档获取信息。每个功能都给出具体语法示例。

重要：因为目标项目 gqlite 是 Rust 实现的，在文档最后加一节 "Rust 实现要点"，分析如何用 Rust 实现数据导入导出。考虑使用 csv、arrow/parquet crate，以及 Rust 的异步 I/O 进行高性能数据加载。
