## 基于 Apache Flink 的实时日志分析系统

本模块实现了一个完整的实时日志处理与分析系统，从 Kafka 消费应用日志，进行实时分析和告警。

### 核心功能

#### 1. LogAnalysisJob - 日志分析作业
- **日志分流**：使用 Side Output 将 ERROR/FATAL 日志分离到独立流中处理
- **窗口聚合统计**：按服务名称和日志级别，每分钟统计日志数量
- **增量聚合**：AggregateFunction + ProcessWindowFunction 组合使用

#### 2. ErrorLogAlertJob - 错误日志告警作业
- **错误检测**：实时监控每个服务的 ERROR 日志数量
- **定时器告警**：使用 KeyedProcessFunction + Timer 实现时间窗口内的计数告警
- **即时告警**：ERROR 数量达到阈值时立即触发告警

### 涉及的 Flink 知识点

| 知识点 | 说明 | 所在类 |
|--------|------|--------|
| Side Output | 侧输出流，将数据分流到不同通道 | LogAnalysisJob |
| AggregateFunction | 增量聚合函数，内存高效 | LogAnalysisJob |
| ProcessWindowFunction | 获取窗口元信息的全量窗口函数 | LogAnalysisJob |
| KeyedProcessFunction | 有状态的键控处理函数 | ErrorLogAlertJob |
| Timer | 事件时间/处理时间定时器 | ErrorLogAlertJob |
| ValueState | 键控状态，存储单个值 | ErrorLogAlertJob |
| WatermarkStrategy | 水位线策略处理乱序数据 | 全部 |

### 数据流向

```
Kafka (log-topic)
    │
    ├── LogAnalysisJob
    │   ├── 主流 → 按 serviceName+level 窗口统计 → 输出 LogStatistics
    │   └── 侧输出 → ERROR/FATAL 日志 → 单独处理
    │
    └── ErrorLogAlertJob
        └── ERROR 日志 → 按 serviceName 分组 → 定时器计数 → 超阈值告警 → AlertEvent
```
