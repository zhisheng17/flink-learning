## 基于 Apache Flink 的实时数仓建设

本模块实现了实时数据仓库的分层架构（ODS → DWD → DWS），展示了 Flink 在实时数仓场景的典型应用。

### 数仓分层架构

```
┌──────────────────────────────────────────────────────┐
│                     ADS（应用层）                      │
│              数据服务/API/实时大屏                      │
├──────────────────────────────────────────────────────┤
│                     DWS（汇总层）                      │
│    DwsOrderStatsJob: 按类别、5分钟窗口汇总统计         │
├──────────────────────────────────────────────────────┤
│                     DWD（明细层）                      │
│    OdsToKafkaJob: 数据清洗、标准化、脏数据分流          │
├──────────────────────────────────────────────────────┤
│                     ODS（原始层）                      │
│              Kafka 原始订单数据                        │
└──────────────────────────────────────────────────────┘
```

### 核心功能

#### 1. OdsToKafkaJob - ODS → DWD 数据清洗
- **数据校验**：订单ID、金额等关键字段校验
- **字段标准化**：空值填充、默认值设置
- **脏数据处理**：Side Output 分流到独立通道
- **写入 DWD**：使用 KafkaSink 写入清洗后数据

#### 2. DwsOrderStatsJob - DWD → DWS 汇总统计
- **多维度聚合**：订单数、商品数、销售额、独立用户数
- **UV 去重**：在 AggregateFunction 中使用 Set 去重
- **窗口聚合**：每 5 分钟一个窗口汇总

### 涉及的 Flink 知识点

| 知识点 | 说明 | 所在类 |
|--------|------|--------|
| Side Output | 脏数据分流 | OdsToKafkaJob |
| KafkaSink | 新版 Kafka Sink API | OdsToKafkaJob |
| ProcessFunction | 数据清洗与校验 | OdsToKafkaJob |
| AggregateFunction | 复杂累加器多维聚合 | DwsOrderStatsJob |
| Set 去重 | UV 去重技术 | DwsOrderStatsJob |
| TumblingWindow | 滚动窗口汇总 | DwsOrderStatsJob |
