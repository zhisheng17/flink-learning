## 基于 Apache Flink 的实时大屏系统

本模块实现了实时大屏的核心功能：PV/UV 统计和热门页面 TopN 排行榜。

### 核心功能

#### 1. RealTimeDashboardJob - PV/UV 统计
- **多维统计**：同时计算 PV、UV、平均停留时长
- **UV 去重**：在 AggregateFunction 累加器中使用 Set 去重
- **增量聚合**：AggregateFunction + ProcessWindowFunction

#### 2. TopNHotPagesJob - 热门页面 TopN
- **两阶段聚合**：先分组聚合，再全局排序
- **ListState 收集**：使用 ListState 收集同一窗口的所有分组结果
- **定时器排序**：Timer 触发后对收集的数据排序输出 TopN

### 涉及的 Flink 知识点

| 知识点 | 说明 | 所在类 |
|--------|------|--------|
| AggregateFunction | 复杂累加器设计（Tuple4） | RealTimeDashboardJob |
| ProcessWindowFunction | 窗口元信息获取 | RealTimeDashboardJob |
| SlidingEventTimeWindow | 滑动窗口 | TopNHotPagesJob |
| WindowFunction | 窗口函数关联窗口信息 | TopNHotPagesJob |
| ListState | 列表状态收集窗口数据 | TopNHotPagesJob |
| Timer | 定时器触发排序 | TopNHotPagesJob |
| KeyedProcessFunction | 两阶段聚合的第二阶段 | TopNHotPagesJob |

### 数据流向

```
                ┌──────────────────────┐
                │ RealTimeDashboardJob │
页面访问 ──────>│  按类别窗口聚合      │──> PV/UV/停留时长 统计
(Kafka)    │    └──────────────────────┘
           │    ┌──────────────────────┐
           └───>│ TopNHotPagesJob      │
                │  两阶段聚合排序      │──> 热门页面 Top10 排行榜
                └──────────────────────┘
```
