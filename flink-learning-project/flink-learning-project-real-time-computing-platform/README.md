## 基于 Apache Flink 的实时计算平台

本模块展示了如何使用 Flink SQL 和 Table API 构建实时计算平台，是数据分析师和平台开发者的参考实现。

### 核心功能

#### 1. FlinkSqlPlatformJob - Flink SQL 方式
- **DDL 建表**：通过 SQL DDL 定义 Kafka Source/Sink 表
- **Watermark 定义**：在 DDL 中声明 Watermark 策略
- **TUMBLE 窗口**：SQL 中的滚动窗口聚合
- **COUNT DISTINCT**：SQL 中的 UV 去重
- **INSERT INTO**：SQL 数据写入

#### 2. TableApiExampleJob - Table API 编程方式
- **流表转换**：DataStream ↔ Table 互相转换
- **编程查询**：filter、select、groupBy、window
- **Changelog 流**：动态表转回 DataStream

### Flink SQL vs Table API 对比

| 特性 | Flink SQL | Table API |
|------|-----------|-----------|
| 使用方式 | 纯 SQL 字符串 | Java/Scala 编程 |
| 适用人群 | 数据分析师 | 平台开发者 |
| 灵活性 | 中等 | 高 |
| 可调试性 | 低 | 高 |
| 动态提交 | 容易 | 较难 |

### 涉及的 Flink 知识点

| 知识点 | 说明 | 所在类 |
|--------|------|--------|
| StreamTableEnvironment | 流表桥接环境 | 全部 |
| SQL DDL | 用 SQL 定义表结构和连接器 | FlinkSqlPlatformJob |
| Kafka Connector DDL | SQL 中配置 Kafka 连接器 | FlinkSqlPlatformJob |
| TUMBLE Window | SQL 滚动窗口函数 | FlinkSqlPlatformJob |
| COUNT DISTINCT | SQL UV 去重 | FlinkSqlPlatformJob |
| fromDataStream | DataStream 转 Table | TableApiExampleJob |
| toChangelogStream | Table 转 DataStream | TableApiExampleJob |
| Expression API | $("field") 字段引用 | TableApiExampleJob |
