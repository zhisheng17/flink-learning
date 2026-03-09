## 基于 Apache Flink 的实时风控系统

本模块实现了两种风控检测方式，展示了 Flink 在金融风控领域的典型应用。

### 核心功能

#### 1. FraudDetectionCepJob - CEP 欺诈检测
- **模式匹配**：检测"多次小额试探 + 大额盗刷"的欺诈模式
- **CEP 规则**：3 次以上小额交易（< 10 元）后紧跟大额交易（> 5000 元），5 分钟内
- **超时处理**：模式部分匹配但超时的可疑事件通过侧输出流处理

#### 2. RiskScoreJob - 实时风险评分
- **多因素评分**：综合交易金额异常、交易频率、城市变更三个因素
- **用户画像**：使用 ValueState + MapState 维护用户历史行为画像
- **实时判定**：评分 ≥ 50 为中风险，≥ 80 为高风险

### 涉及的 Flink 知识点

| 知识点 | 说明 | 所在类 |
|--------|------|--------|
| Flink CEP | 复杂事件处理库 | FraudDetectionCepJob |
| Pattern API | 定义事件匹配模式 | FraudDetectionCepJob |
| SimpleCondition | CEP 简单条件 | FraudDetectionCepJob |
| PatternSelectFunction | 处理匹配到的事件序列 | FraudDetectionCepJob |
| PatternTimeoutFunction | 处理超时的部分匹配 | FraudDetectionCepJob |
| OutputTag (Side Output) | 侧输出流处理超时事件 | FraudDetectionCepJob |
| ValueState | 键控状态存储单个值 | RiskScoreJob |
| MapState | 键控状态存储键值对 | RiskScoreJob |
| KeyedProcessFunction | 有状态键控处理函数 | RiskScoreJob |

### 架构设计

```
                      ┌────────────────────────┐
                 ┌───>│  FraudDetectionCepJob   │──> 欺诈告警 (RiskEvent)
                 │    │  (CEP Pattern 模式匹配)  │──> 超时可疑事件 (Side Output)
交易数据 ────────┤    └────────────────────────┘
(Kafka)          │    ┌────────────────────────┐
                 └───>│  RiskScoreJob           │──> 风险评分 > 50 的事件
                      │  (多因素实时评分)        │
                      └────────────────────────┘
```
