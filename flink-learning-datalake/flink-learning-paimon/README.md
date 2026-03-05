### Flink-learning-paimon

[Apache Paimon](https://paimon.apache.org/)（原 Flink Table Store）是一个流批一体的数据湖存储系统，支持高速数据摄入、变更日志跟踪和高效的实时分析。

### 相关资料

+ [Apache Paimon 官方文档](https://paimon.apache.org/docs/master/)

+ [Apache Paimon GitHub](https://github.com/apache/paimon)

+ [Paimon Flink 引擎文档](https://paimon.apache.org/docs/master/engines/flink/)

+ [Flink Table Store 是什么？](https://www.yuque.com/lijinsongzhixin/qxwonh/iktr4c)

+ [Flink Table Store ——从计算到存储提升流批统一端到端用户体验](https://mp.weixin.qq.com/s/siMnKbWzVFU4fic5-XFoRw)

+ [Demo 使用 Flink CDC 写入 Table Store，使用 Spark 查询 Table Store](https://www.yuque.com/lijinsongzhixin/qxwonh/yhktz8)

### 项目说明

本模块使用 Apache Paimon `1.3.1`，基于 Flink 1.20 引擎。

> **注意**: Flink Table Store 已于 2023 年正式更名为 [Apache Paimon](https://paimon.apache.org/)，成为 Apache 顶级项目。Maven 坐标已从 `org.apache.flink:flink-table-store-*` 迁移至 `org.apache.paimon:paimon-*`。

### 架构

![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/2022-12-16-075225.jpg)

