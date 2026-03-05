## 基于 Apache Flink 的监控告警系统

本模块实现了两种监控告警模式，展示了 Flink 在实时监控场景中的典型应用。

### 核心功能

#### 1. DynamicAlertRuleJob - 动态规则告警
- **广播状态**：使用 Broadcast State 将告警规则广播到所有并行实例
- **动态更新**：支持运行时动态添加、修改、禁用告警规则，无需重启作业
- **规则匹配**：支持多种比较操作符（GT/LT/GTE/LTE）

#### 2. MetricAggregateAlertJob - 滑动窗口聚合告警
- **滑动窗口**：5 分钟窗口、1 分钟滑动步长，平滑检测异常
- **增量聚合**：ReduceFunction 增量计算窗口内最大值
- **阈值告警**：CPU > 90%、内存 > 85% 时触发告警

### 涉及的 Flink 知识点

| 知识点 | 说明 | 所在类 |
|--------|------|--------|
| Broadcast State | 广播状态，用于动态配置分发 | DynamicAlertRuleJob |
| BroadcastProcessFunction | 处理广播流和数据流的连接 | DynamicAlertRuleJob |
| MapStateDescriptor | 广播状态描述符 | DynamicAlertRuleJob |
| SlidingEventTimeWindow | 滑动窗口（有重叠） | MetricAggregateAlertJob |
| ReduceFunction | 增量聚合函数（输入输出类型相同） | MetricAggregateAlertJob |
| ProcessWindowFunction | 全量窗口函数获取窗口元信息 | MetricAggregateAlertJob |

### 架构设计

```
┌─────────────────┐     ┌──────────────────────┐
│ 指标数据 (Kafka) │────>│ DynamicAlertRuleJob   │───> 告警事件
└─────────────────┘     │  (Broadcast State)    │
                        └──────────────────────┘
┌─────────────────┐          ↑
│ 规则配置 (Kafka) │──────────┘ (广播)
└─────────────────┘

┌─────────────────┐     ┌──────────────────────┐
│ 指标数据 (Kafka) │────>│ MetricAggregateAlert  │───> 告警事件
└─────────────────┘     │  (Sliding Window)     │
                        └──────────────────────┘
```
