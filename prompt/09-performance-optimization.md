# Agent 09 - 性能优化特性

你需要调研 Kuzu 嵌入式图数据库的**性能优化特性**，然后把调研结果写成中文 markdown 文档保存到 `/Users/I520239/Desktop/codes/ai/graphdb/research/09-performance-optimization.md`。

调研重点：
1. 向量化执行引擎 (Vectorized Execution)
   - 向量大小 (Vector Size)
   - 数据批量处理
   - SIMD 优化（如果有）
2. Morsel-Driven 并行模型
   - 线程调度
   - 工作负载均衡
   - 并行度配置
3. Worst-Case Optimal Join (WCOJ)
   - 多路连接优化
   - 与传统二路连接的对比
4. Factorized Execution
   - 避免中间结果膨胀
   - 延迟物化 (Lazy Materialization)
5. 列式存储的性能优势
   - 缓存友好性
   - 数据压缩带来的 I/O 减少
6. Zone Maps 加速过滤
7. 内存管理优化
   - Buffer Pool 配置
   - 内存限制控制
8. 基准测试结果和性能对比
   - 与 Neo4j 的对比
   - LDBC SNB 基准测试

请通过网络搜索 Kuzu 官方博客文章、学术论文、基准测试报告获取信息。

重要：因为目标项目 gqlite 是 Rust 实现的，在文档最后加一节 "Rust 实现要点"，分析如何利用 Rust 特性实现高性能：
- packed_simd / std::simd 用于 SIMD
- rayon 用于并行执行
- Rust 的零成本抽象
- 内存对齐和缓存行优化
- unsafe 代码的使用场景和安全封装
