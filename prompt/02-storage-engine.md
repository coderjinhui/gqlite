# Agent 02 - 存储引擎

你需要调研 Kuzu 嵌入式图数据库的**存储引擎**功能，然后把调研结果写成中文 markdown 文档保存到 `/Users/I520239/Desktop/codes/ai/graphdb/research/02-storage-engine.md`。

调研重点：
1. 整体存储架构 - 列式存储 (Columnar Storage)
2. 节点存储 - Node Table 的存储格式、Node Group 概念
3. 关系存储 - CSR (Compressed Sparse Row) 格式、关系列表
4. Buffer Pool Manager - 页面管理、缓存策略
5. 磁盘页面格式 - 页面大小、页面类型
6. 数据压缩 - 列式压缩算法（ALP, BitPacking, Dictionary, RLE 等）
7. 空值处理 (Null handling) - NULL bitmap
8. Overflow 页面 - 大字符串/列表的溢出存储
9. 存储文件组织 - 目录结构、文件类型
10. 内存映射 vs 读写策略

请通过网络搜索 Kuzu 官方文档、博客文章、学术论文获取信息。文档要详细，包含架构图描述。

重要：因为目标项目 gqlite 是 Rust 实现的，在文档最后加一节 "Rust 实现要点"，分析如何用 Rust 实现这些存储组件。考虑使用 mmap、自定义分配器、zero-copy 设计等 Rust 特有的优势。
