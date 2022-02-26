---
toc: true
title: 《Flink 实战与性能优化》—— Flink 扩展库——State Processor API
date: 2021-07-30
tags:
- Flink
- 大数据
- 流式计算
---




## 6.3 Flink 扩展库——State Processor API

State Processor API 功能是在 1.9 版本中新增加的一个功能，本节将带你了解一下其功能和如何使用？

<!--more-->

### 6.3.1 State Processor API 简介

能够从外部访问 Flink 作业的状态一直用户迫切需要的功能之一，在 Apache Flink 1.9.0 中新引入了 State Processor API，该 API 让用户可以通过  Flink DataSet 作业来灵活读取、写入和修改 Flink 的 Savepoint 和 Checkpoint。


### 6.3.2 在 Flink 1.9 之前是如何处理状态的？

一般来说，大多数的 Flink 作业都是有状态的，并且随着作业运行的时间越来越久，就会累积越多越多的状态，如果因为故障导致作业崩溃可能会导致作业的状态都丢失，那么对于比较重要的状态来说，损失就会很大。为了保证作业状态的一致性和持久性，Flink 从一开始使用的就是 Checkpoint 和 Savepoint 来保存状态，并且可以从 Savepoint 中恢复状态。在 Flink 的每个新 Release 版本中，Flink 社区添加了越来越多与状态相关的功能以提高 Checkpoint 的速度和恢复速度。

有的时候，用户可能会有这些需求场景，比如从第三方外部系统访问作业的状态、将作业的状态信息迁移到另一个应用程序等，目前现有支持查询作业状态的功能 Queryable State，但是在 Flink 中目前该功能只支持根据 Key 查找，并且不能保证返回值的一致性。另外该功能不支持添加和修改作业的状态，所以适用的场景还是比较有限。


### 6.3.3 使用 State Processor API 读写作业状态

在 1.9 版本中的 State Processor API，它完全和之前不一致，该功能使用 InputFormat 和 OutputFormat 扩展了 DataSet API 以读取和写入 Checkpoint 和 Savepoint 数据。由于 DataSet 和 Table API 的互通性，所以也可以使用 Table API 或者 SQL 查询和分析状态的数据。例如，再获取到正在运行的流作业状态的 Checkpoint 后，可以使用 DataSet 批处理程序对其进行分析，以验证该流作业的运行是否正确。另外 State Processor API 还可以修复不一致的状态信息，它提供了很多方法来开发有状态的应用程序，这些方法在以前的版本中因为设计的问题导致作业在启动后不能再修改，否则状态可能会丢失。现在，你可以任意修改状态的数据类型、调整算子的最大并行度、拆分或合并算子的状态、重新分配算子的 uid 等。

如果要使用 State Processor API 去读写作业的状态，你需要添加下面的依赖：

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-state-processor-api_${scala.binary.version}</artifactId>
    <version>${flink.version}</version>
</dependency>
```


### 6.3.4 使用 DataSet 读取作业状态

State Processor API 将作业的状态映射到一个或多个可以单独处理的数据集，为了能够使用该 API，需要先了解这个映射的工作方式，首先来看下有状态的 Flink 作业是什么样子的。Flink 作业是由很多算子组成，通常是一个或多个数据源（Source）、一些实际处理数据的算子（比如 Map／Filter／FlatMap 等）和一个或者多个 Sink。每个算子会在一个或者多个任务中并行运行（取决于并行度），并且可以使用不同类型的状态，算子可能会有零个、一个或多个 Operator State，这些状态会组成一个以算子任务为范围的列表。如果是算子应用在 KeyedStream，它还有零个、一个或者多个 Keyed State，它们的作用域范围是从每个已处理数据中提取 Key，可以将 Keyed State 看作是一个分布式的 Map。

State Processor API 现在提供了读取、新增和修改 Savepoint 数据的方法，比如从已加载的 Savepoint 中读取数据集，然后将数据集转换为状态并将其保存到 Savepoint。下面分别讲解下这三种方法该如何使用。

#### 读取现有的 Savepoint

读取状态首先需要指定一个 Savepoint（或者 Checkpoint） 的路径和状态后端存储的类型。

```java
ExecutionEnvironment bEnv   = ExecutionEnvironment.getExecutionEnvironment();
ExistingSavepoint savepoint = Savepoint.load(bEnv, "hdfs://path/", new RocksDBStateBackend());
```

读取 Operator State 时，只需指定算子的 uid、状态名称和类型信息。

```java
DataSet<Integer> listState  = savepoint.readListState("zhisheng-uid", "list-state", Types.INT);

DataSet<Integer> unionState = savepoint.readUnionState("zhisheng-uid", "union-state", Types.INT);
 
DataSet<Tuple2<Integer, Integer>> broadcastState = savepoint.readBroadcastState("zhisheng-uid", "broadcast-state", Types.INT, Types.INT);
```

如果在状态描述符（StateDescriptor）中使用了自定义类型序列化器 TypeSerializer，也可以指定它：

```java
DataSet<Integer> listState = savepoint.readListState(
    "zhisheng-uid", "list-state", 
    Types.INT, new MyCustomIntSerializer());
```




#### 写入新的 Savepoint



#### 修改现有的 Savepoint




### 6.3.5 为什么要使用 DataSet API？


### 6.3.6 小结与反思


加入知识星球可以看到上面文章：https://t.zsxq.com/nMR7ufq

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-09-25-zsxq.jpg)


