## Apache Flink 实时作业脚手架

这是一个生产级别的 Flink 作业模板，展示了 Flink 作业开发中的常见最佳实践。

### 核心功能

- **Checkpoint 配置**：Exactly-Once 语义、定时 Checkpoint、取消时保留
- **重启策略**：固定延迟重启策略，确保作业容错
- **Watermark 策略**：处理乱序数据，支持 Idle Source 检测
- **Keyed State**：使用 ValueState 实现数据去重
- **窗口聚合**：自定义 AggregateFunction 进行增量聚合
- **Kafka Source**：从 Kafka 消费数据的标准模式

### 使用方式

1. 复制本模板到新项目中
2. 根据业务需求修改数据模型（替换 `ServerMetric`）
3. 实现自己的处理逻辑（替换 `DeduplicateFunction`）
4. 配置合适的输出 Sink（替换 `print`）
5. 调整 Checkpoint、并行度等参数

### 涉及的 Flink 知识点

| 知识点 | 说明 |
|--------|------|
| StreamExecutionEnvironment | Flink 流处理执行环境 |
| CheckpointConfig | Checkpoint 配置（间隔、模式、超时） |
| RestartStrategy | 重启策略（固定延迟、指数退避） |
| WatermarkStrategy | 水位线策略（乱序处理、Idle Source） |
| ValueState | 键控状态，存储单个值 |
| AggregateFunction | 增量聚合函数（高效窗口计算） |
| TumblingEventTimeWindow | 基于事件时间的滚动窗口 |
| KafkaSource | 新版 Kafka Source API |
