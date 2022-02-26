### 《大数据实时计算引擎 Flink 实战与性能优化》


2019 年著，基于 Flink 1.9 讲解的书籍目录大纲，含 Flink 入门、概念、原理、实战、性能调优、源码解析等内容。涉及 Flink Connector、Metrics、Library、DataStream API、Table API & SQL 等内容的学习案例，还有 Flink 落地应用的大型项目案例分享。


## 专栏介绍

首发地址：[http://www.54tianzhisheng.cn/2019/11/15/flink-in-action/](http://www.54tianzhisheng.cn/2019/11/15/flink-in-action/)

专栏地址：[https://gitbook.cn/gitchat/column/5dad4a20669f843a1a37cb4f](https://gitbook.cn/gitchat/column/5dad4a20669f843a1a37cb4f)

加入知识星球可以获取到专栏所有内容：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-07-23-124320.jpg)

### 专栏亮点

+ 全网首个使用最新版本 **Flink 1.9** 进行内容讲解（该版本更新很大，架构功能都有更新），领跑于目前市面上常见的 Flink 1.7 版本的教学课程。

+ 包含大量的**实战案例和代码**去讲解原理，有助于读者一边学习一边敲代码，达到更快，更深刻的学习境界。目前市面上的书籍没有任何实战的内容，还只是讲解纯概念和翻译官网。

+ 在专栏高级篇中，根据 Flink 常见的项目问题提供了**排查和解决的思维方法**，并通过这些问题探究了为什么会出现这类问题。

+ 在实战和案例篇，围绕大厂公司的**经典需求**进行分析，包括架构设计、每个环节的操作、代码实现都有一一讲解。

### 为什么要学习 Flink？

随着大数据的不断发展，对数据的及时性要求越来越高，实时场景需求也变得越来越多，主要分下面几大类：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-05-055800.jpg)

为了满足这些实时场景的需求，衍生出不少计算引擎框架。现有市面上的大数据计算引擎的对比如下图所示：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-05-055826.jpg)

可以发现无论从 Flink 的架构设计上，还是从其功能完整性和易用性来讲都是领先的，再加上 Flink 是**阿里巴巴主推**的计算引擎框架，所以从去年开始就越来越火了！

目前，阿里巴巴、腾讯、美团、华为、滴滴出行、携程、饿了么、爱奇艺、有赞、唯品会等大厂都已经将 Flink 实践于公司大型项目中，带起了一波 Flink 风潮，**势必也会让 Flink 人才市场产生供不应求的招聘现象**。

### 专栏内容

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-05-060049.jpg)

#### 预备篇

介绍实时计算常见的使用场景，讲解 Flink 的特性，并且对比了 Spark Streaming、Structured Streaming 和 Storm 等大数据处理引擎，然后准备环境并通过两个 Flink 应用程序带大家上手 Flink。

### 基础篇

深入讲解 Flink 中 Time、Window、Watermark、Connector 原理，并有大量文章篇幅（含详细代码）讲解如何去使用这些 Connector（比如 Kafka、ElasticSearch、HBase、Redis、MySQL 等），并且会讲解使用过程中可能会遇到的坑，还教大家如何去自定义 Connector。

### 进阶篇

讲解 Flink 中 State、Checkpoint、Savepoint、内存管理机制、CEP、Table／SQL API、Machine Learning 、Gelly。在这篇中不仅只讲概念，还会讲解如何去使用 State、如何配置 Checkpoint、Checkpoint 的流程和如何利用 CEP 处理复杂事件。

### 高级篇

重点介绍 Flink 作业上线后的监控运维：如何保证高可用、如何定位和排查反压问题、如何合理的设置作业的并行度、如何保证 Exactly Once、如何处理数据倾斜问题、如何调优整个作业的执行效率、如何监控 Flink 及其作业？

### 实战篇

教大家如何分析实时计算场景的需求，并使用 Flink 里面的技术去实现这些需求，比如实时统计 PV／UV、实时统计商品销售额 TopK、应用 Error 日志实时告警、机器宕机告警。这些需求如何使用 Flink 实现的都会提供完整的代码供大家参考，通过这些需求你可以学到 ProcessFunction、Async I／O、广播变量等知识的使用方式。

### 系统案例篇

讲解大型流量下的真实案例：如何去实时处理海量日志（错误日志实时告警／日志实时 ETL／日志实时展示／日志实时搜索）、基于 Flink 的百亿数据实时去重实践（从去重的通用解决方案 --> 使用 BloomFilter 来实现去重 --> 使用 Flink 的 KeyedState 实现去重）。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-05-060153.jpg)

### 多图讲解 Flink 知识点

![ Flink 支持多种时间语义](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-05-060219.jpg)

![Flink 提供灵活的窗口](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-05-060232.jpg)

![Flink On YARN](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-05-060310.jpg)

![Flink Checkpoint](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-05-060900.jpg)

![Flink 监控](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-05-060926.jpg)

### 你将获得什么

+ 掌握 Flink 与其他计算框架的区别
+ 掌握 Flink Time／Window／Watermark／Connectors 概念和实现原理
+ 掌握 Flink State／Checkpoint／Savepoint 状态与容错
+ 熟练使用 DataStream／DataSet／Table／SQL API 开发 Flink 作业
+ 掌握 Flink 作业部署／运维／监控／性能调优
+ 学会如何分析并完成实时计算需求
+ 获得大型高并发流量系统案例实战项目经验

### 适宜人群

+ Flink 爱好者
+ 实时计算开发工程师
+ 大数据开发工程师
+ 计算机专业研究生
+ 有实时计算场景场景的 Java 开发工程师

## 目录大纲

```
1预备篇

第一章——实时计算引擎
    1.1你的公司是否需要引入实时计算引擎
        1.1.1 实时计算需求
        1.1.2 数据实时采集
        1.1.3 数据实时计算
        1.1.4 数据实时下发
        1.1.5 实时计算场景
        1.1.6 离线计算 vs 实时计算
        1.1.7 实时计算面临的挑战
        1.1.8 小结与反思
    1.2彻底了解大数据实时计算框架 Flink
        1.2.1 Flink 简介
        1.2.2 Flink 整体架构
        1.2.3 Flink 的多种方式部署
        1.2.4 Flink 分布式运行流程
        1.2.5 Flink API
        1.2.6 Flink 程序与数据流结构
        1.2.7 丰富的 Connector
        1.2.8 事件时间&处理时间语义
        1.2.9 灵活的窗口机制
        1.2.10 并行执行任务机制
        1.2.11 状态存储和容错
        1.2.12 自己的内存管理机制
        1.2.13 多种扩展库
        1.2.14 小结与反思
    1.3大数据计算框架对比
        1.3.1 Flink
        1.3.2 Blink
        1.3.3 Spark
        1.3.4 Spark Streaming
        1.3.5 Structured Streaming
        1.3.6 Flink VS Spark
        1.3.7 Storm
        1.3.8 Flink VS Storm
        1.3.9 全部对比结果
        1.3.10 小结与反思
    1.4总结

2第二章——Flink 入门
    2.1Flink 环境准备
        2.1.1 JDK 安装与配置
        2.1.2 Maven 安装与配置
        2.1.3 IDE 安装与配置
        2.1.4 MySQL 安装与配置
        2.1.5 Kafka 安装与配置
        2.1.6 ElasticSearch 安装与配置
        2.1.7 小结与反思
    2.2Flink 环境搭建
        2.2.1 Flink 下载与安装
        2.2.2 Flink 启动与运行
        2.2.3 Flink 目录配置文件解读
        2.2.4 Flink 源码下载
        2.2.5 Flink 源码编译
        2.2.6 将 Flink 源码导入到 IDE
        2.2.7 小结与反思
    2.3案例1：WordCount 应用程序
        2.3.1 使用 Maven 创建项目
        2.3.2 使用 IDEA 创建项目
        2.3.3 流计算 WordCount 应用程序代码实现
        2.3.4 运行流计算 WordCount 应用程序
        2.3.5 流计算 WordCount 应用程序代码分析
        2.3.6 小结与反思
    2.4案例2：实时处理 Socket 数据
        2.4.1 使用 IDEA 创建项目
        2.4.2 实时处理 Socket 数据应用程序代码实现
        2.4.3 运行实时处理 Socket 数据应用程序
        2.4.4 实时处理 Socket 数据应用程序代码分析
        2.4.5 Flink 中使用 Lambda 表达式
        2.4.5 小结与反思
    2.5总结
    
2基础篇

3第三章——Flink 中的流计算处理
    3.1Flink 多种时间语义对比
        3.1.1 Processing Time
        3.1.2 Event Time
        3.1.3 Ingestion Time
        3.1.4 三种 Time 的对比结果
        3.1.5 使用场景分析
        3.1.6 Time 策略设置
        3.1.7 小结与反思
    3.2Flink Window 基础概念与实现原理
        3.2.1 Window 简介
        3.2.2 Window 有什么作用？
        3.2.3 Flink 自带的 Window
        3.2.4 Time Window 的用法及源码分析
        3.2.5 Count Window 的用法及源码分析
        3.2.6 Session Window 的用法及源码分析
        3.2.7 如何自定义 Window？
        3.2.8 Window 源码分析
        3.2.9 Window 组件之 WindowAssigner 的用法及源码分析
        3.2.10 Window 组件之 Trigger 的用法及源码分析
        3.2.11 Window 组件之 Evictor 的用法及源码分析
        3.2.12 小结与反思
    3.3必须熟悉的数据转换 Operator(算子)
        3.3.1 DataStream Operator
        3.3.2 DataSet Operator
        3.3.3 流计算与批计算统一的思路
        3.3.4 小结与反思
    3.4使用 DataStream API 来处理数据
        3.4.1 DataStream 的用法及分析
        3.4.2 SingleOutputStreamOperator 的用法及分析
        3.4.3 KeyedStream 的用法及分析
        3.4.4 SplitStream 的用法及分析
        3.4.5 WindowedStream 的用法及分析
        3.4.6 AllWindowedStream 的用法及分析
        3.4.7 ConnectedStreams 的用法及分析
        3.4.8 BroadcastStream 的用法及分析
        3.4.9 BroadcastConnectedStream 的用法及分析
        3.4.10 QueryableStateStream 的用法及分析
        3.4.11 小结与反思
    3.5Watermark 的用法和结合 Window 处理延迟数据
        3.5.1 Watermark 简介
        3.5.2 Flink 中的 Watermark 的设置
        3.5.3 Punctuated Watermark
        3.5.4 Periodic Watermark
        3.5.5 每个 Kafka 分区的时间戳
        3.5.6 将 Watermark 与 Window 结合起来处理延迟数据
        3.5.7 处理延迟数据的三种方法
        3.5.8 小结与反思
    3.6Flink 常用的 Source Connector 和 Sink Connector 介绍
        3.6.1 Data Source 简介
        3.6.2 常用的 Data Source
        3.6.3 Data Sink 简介
        3.6.4 常用的 Data Sink
        3.6.5 小结与反思
    3.7Flink Connector —— Kafka 的使用和源码分析
        3.7.1 准备环境和依赖
        3.7.2 将测试数据发送到 Kafka Topic
        3.7.3 Flink 如何消费 Kafka 数据？
        3.7.4 Flink 如何将计算后的数据发送到 Kafka？
        3.7.5 FlinkKafkaConsumer 源码分析
        3.7.6 FlinkKafkaProducer 源码分析
        3.7.7 使用 Flink-connector-kafka 可能会遇到的问题
        3.7.8 小结与反思
    3.8自定义 Flink Connector
        3.8.1 自定义 Source Connector
        3.8.2 RichSourceFunction 的用法及源码分析
        3.8.3 自定义 Sink Connector
        3.8.4 RichSinkFunction 的用法及源码分析
        3.8.5 小结与反思
    3.9Flink Connector —— ElasticSearch 的用法和分析
        3.9.1 准备环境和依赖
        3.9.2 使用 Flink 将数据写入到 ElasticSearch 应用程序
        3.9.3 验证数据是否写入 ElasticSearch？
        3.9.4 如何保证在海量数据实时写入下 ElasticSearch 的稳定性？
        3.9.5 使用 Flink-connector-elasticsearch 可能会遇到的问题
        3.9.6 小结与反思
    3.10Flink Connector —— HBase 的用法
        3.10.1 准备环境和依赖
        3.10.2 Flink 使用 TableInputFormat 读取 HBase 批量数据
        3.10.3 Flink 使用 TableOutputFormat 向 HBase 写入数据
        3.10.4 Flink 使用 HBaseOutputFormat 向 HBase 实时写入数据
        3.10.5 项目运行及验证
        3.10.6 小结与反思
    3.11Flink Connector —— Redis 的用法
        3.11.1 安装 Redis
        3.11.2 将商品数据发送到 Kafka
        3.11.3 Flink 消费 Kafka 中的商品数据
        3.11.4 Redis Connector 简介
        3.11.5 Flink 写入数据到 Redis
        3.11.6 项目运行及验证
        3.11.7 小结与反思
    3.12使用 Side Output 分流
        3.12.1 使用 Filter 分流
        3.12.2 使用 Split 分流
        3.12.3 使用 Side Output 分流
        3.12.4 小结与反思
    3.13总结
    
3进阶篇

4第四章——Flink 中的状态及容错机制
    4.1深度讲解 Flink 中的状态
        4.1.1 为什么需要 State？
        4.1.2 State 的种类
        4.1.3 Keyed State
        4.1.4 Operator State
        4.1.5 Raw and Managed State
        4.1.6 如何使用托管的 Keyed State
        4.1.7 State TTL(存活时间)
        4.1.8 如何使用托管的 Operator State
        4.1.9 Stateful Source Functions
        4.1.10 Broadcast State
        4.1.11 Queryable State
        4.1.12 小结与反思
    4.2Flink 状态后端存储
        4.2.1 State Backends
        4.2.2 MemoryStateBackend 的用法及分析
        4.2.3 FsStateBackend 的用法及分析
        4.2.4 RocksDBStateBackend 的用法及分析
        4.2.5 如何选择状态后端存储？
        4.2.6 小结与反思
    4.3Flink Checkpoint 和 Savepoint 的区别及其配置使用
        4.3.1 Checkpoint 简介及使用
        4.3.2 Savepoint 简介及使用
        4.3.3 Savepoint 与 Checkpoint 的区别
        4.3.4 Checkpoint 流程
        4.3.5 如何从 Checkpoint 中恢复状态
        4.3.6 如何从 Savepoint 中恢复状态
        4.3.6 小结与反思
    4.4总结
    
5第五章——Table API & SQL
    5.1Flink Table & SQL 概念与通用 API
        5.1.1 新增 Blink SQL 查询处理器
        5.1.2 为什么选择 Table API & SQL？
        5.1.3 Flink Table 项目模块
        5.1.4 两种 planner 之间的区别
        5.1.5 添加项目依赖
        5.1.6 创建一个 TableEnvironment
        5.1.7 Table API & SQL 应用程序的结构
        5.1.8 Catalog 中注册 Table
        5.1.9 注册外部的 Catalog
        5.1.10 查询 Table
        5.1.11 提交 Table
        5.1.12 翻译并执行查询
        5.1.13 小结与反思
    5.2Flink Table API & SQL 功能
        5.2.1 Flink Table 和 SQL 与 DataStream 和 DataSet 集成
        5.2.2 查询优化
        5.2.3 数据类型
        5.2.4 时间属性
        5.2.5 SQL Connector
        5.2.6 SQL Client
        5.2.7 Hive
        5.2.8 小结与反思
    5.3总结
    
    
6第六章——扩展库
    6.1Flink CEP 简介及其使用场景
        6.1.1 CEP 简介
        6.1.2 规则引擎对比
        6.1.3 Flink CEP 简介
        6.1.4 Flink CEP 动态更新规则
        6.1.5 Flink CEP 使用场景分析
        6.1.6 小结与反思
    6.2使用 Flink CEP 处理复杂事件
        6.2.1 准备依赖
        6.2.2 Flink CEP 入门应用程序
        6.2.3 Pattern API
        6.2.4 检测 Pattern
        6.2.5 CEP 时间属性
        6.2.6 小结与反思
    6.3Flink 扩展库——State Processor API
        6.3.1 State Processor API 简介
        6.3.2 在 Flink 1.9 之前是如何处理状态的？
        6.3.3 使用 State Processor API 读写作业状态
        6.3.4 使用 DataSet 读取作业状态
        6.3.5 为什么要使用 DataSet API？
        6.3.6 小结与反思
    6.4Flink 扩展库——Machine Learning
        6.4.1 Flink-ML 简介
        6.4.2 使用 Flink-ML
        6.4.3 使用 Flink-ML Pipeline
        6.4.4 小结与反思
    6.5Flink 扩展库——Gelly
        6.5.1 Gelly 简介
        6.5.2 使用 Gelly
        6.5.3 Gelly API
        6.5.4 小结与反思
    6.6 总结

4高级篇

7第七章——Flink 作业环境部署
    7.1Flink 配置详解及如何配置高可用？
        7.1.1 Flink 配置详解
        7.1.2 Log 的配置
        7.1.3 如何配置 JobManager 高可用？
        7.1.4 小结与反思
    7.2Flink 作业如何在 Standalone、YARN、Mesos、K8S 上部署运行？
        7.2.1 Standalone
        7.2.2 YARN
        7.2.3 Mesos
        7.3.4 Kubernetes
        7.2.5 小结与反思
    7.3总结
    
8第八章——Flink 监控
    8.1实时监控 Flink 及其作业
        8.1.1 监控 JobManager
        8.1.2 监控 TaskManager
        8.1.3 监控 Flink 作业
        8.1.4 最关心的性能指标
        8.1.5 小结与反思
    8.2搭建一套 Flink 监控系统
        8.2.1 利用 API 获取监控数据
        8.2.2 Metrics 类型简介
        8.2.3 利用 JMXReporter 获取监控数据
        8.2.4 利用 PrometheusReporter 获取监控数据
        8.2.5 利用 PrometheusPushGatewayReporter 获取监控数据
        8.2.6 利用 InfluxDBReporter 获取监控数据
        8.2.7 安装 InfluxDB 和 Grafana
        8.2.8 配置 Grafana 展示监控数据
        8.2.9 小结与反思
    8.3总结

9第九章——Flink 性能调优
    9.1如何处理 Flink Job Backpressure （反压）问题？
        9.1.1 Flink 流处理为什么需要网络流控
        9.1.2 Flink 1.5 之前的网络流控机制
        9.1.3 基于 Credit 的反压机制
        9.1.4 定位产生反压的位置
        9.1.5 分析和处理反压问题
        9.1.6 小结与反思
    9.2如何查看 Flink 作业执行计划？
        9.2.1 如何获取执行计划 JSON？
        9.2.2 生成执行计划图
        9.2.3 深入探究 Flink 作业执行计划
        9.2.4 Flink 中算子 chain 起来的条件
        9.2.5 如何禁止 Operator chain？
        9.2.6 小结与反思
    9.3Flink Parallelism 和 Slot 深度理解
        9.3.1 Parallelism 简介
        9.3.2 如何设置 Parallelism？
        9.3.3 Slot 简介
        9.3.4 Slot 和 Parallelism 的关系
        9.3.5 可能会遇到 Slot 和 Parallelism 的问题
        9.3.6 小结与反思
    9.4如何合理的设置 Flink 作业并行度？
        9.4.1 Source 端并行度的配置
        9.4.2 中间 Operator 并行度的配置
        9.4.3 Sink 端并行度的配置
        9.4.4 Operator Chain
        9.4.5 小结与反思
    9.5Flink 中如何保证 Exactly Once？
        9.5.1 Flink 内部如何保证 Exactly Once？
        9.5.2 端对端如何保证 Exactly Once？
        9.5.3 分析 FlinkKafkaConsumer 的设计思想
        9.5.4 小结与反思
    9.6如何处理 Flink 中数据倾斜问题？
        9.6.1 数据倾斜简介
        9.6.2 判断是否存在数据倾斜
        9.6.3 分析和解决数据倾斜问题
        9.6.4 小结与反思
    9.7总结

10第十章——Flink 最佳实践
    10.1如何设置 Flink Job RestartStrategy（重启策略）？
        10.1.1 常见错误导致 Flink 作业重启
        10.1.2 RestartStrategy 简介
        10.1.3 为什么需要 RestartStrategy？
        10.1.4 如何配置 RestartStrategy？
        10.1.5 RestartStrategy 源码分析
        10.1.6 Failover Strategies（故障恢复策略）
        10.1.7 小结与反思
    10.2如何使用 Flink ParameterTool 读取配置？
        10.2.1 Flink Job 配置
        10.2.2 ParameterTool 管理配置
        10.2.3 ParameterTool 源码分析
        10.2.4 小结与反思
    10.3总结
    

5实战篇

11第十一章——Flink 实战
    11.1如何统计网站各页面一天内的 PV 和 UV？
        11.1.1 统计网站各页面一天内的 PV
        11.1.2 统计网站各页面一天内 UV 的三种方案
        11.1.3 小结与反思
    11.2如何使用 Flink ProcessFunction 处理宕机告警?
        11.2.1 ProcessFunction 简介
        11.2.2 CoProcessFunction 简介
        11.2.3 Timer 简介
        11.2.4 如果利用 ProcessFunction 处理宕机告警？
        11.2.5 小结与反思
    11.3如何利用 Async I／O 读取告警规则？
        11.3.1 为什么需要 Async I/O？
        11.3.2 Async I/O API
        11.3.3 利用 Async I/O 读取告警规则需求分析
        11.3.4 如何使用 Async I/O 读取告警规则数据
        11.3.5 小结与反思
    11.4如何利用广播变量动态更新告警规则？
        11.4.1 BroadcastVariable 简介
        11.4.2 如何使用 BroadcastVariable ？
        11.4.3 利用广播变量动态更新告警规则数据需求分析
        11.4.4 读取告警规则数据
        11.4.5 监控数据连接规则数据
        11.4.6 小结与反思
    11.5如何实时将应用 Error 日志告警？
        11.5.1 日志处理方案的演进
        11.5.2 日志采集工具对比
        11.5.3 日志结构设计
        11.5.4 异常日志实时告警项目架构
        11.5.5 日志数据发送到 Kafka
        11.5.6 Flink 实时处理日志数据
        11.5.7 处理应用异常日志
        11.5.8 小结与反思
    11.6总结
    
6案例篇

12第十二章——Flink 案例
    12.1基于 Flink 实时处理海量日志
        12.2.1 实时处理海量日志需求分析
        12.2.2 实时处理海量日志架构设计
        12.2.3 日志实时采集
        12.2.4 日志格式统一
        12.2.5 日志实时清洗
        12.2.6 日志实时告警
        12.2.7 日志实时存储
        12.2.8 日志实时展示
        12.2.9 小结与反思
    12.2基于 Flink 的百亿数据实时去重
        12.2.1 去重的通用解决方案
        12.2.2 使用 BloomFilter 实现去重
        12.2.3 使用 HBase 维护全局 Set 实现去重
        12.2.4 使用 Flink 的 KeyedState 实现去重
        12.2.5 使用 RocksDBStateBackend 的优化方法
        12.2.6 小结与反思
    12.3基于 Flink 的实时监控告警系统
        12.3.1 监控系统的诉求
        12.3.2 监控系统包含的内容
        12.3.3 Metrics／Trace／Log 数据实时采集
        12.3.4 消息队列如何撑住高峰流量
        12.3.5 指标数据实时计算
        12.3.6 提供及时且准确的根因分析告警
        12.3.7 AIOps 智能运维道路探索
        12.3.8 如何保障高峰流量实时写入存储系统的稳定性
        12.3.9 监控数据使用可视化图表展示
        12.3.10 小结与反思
    12.4总结
```