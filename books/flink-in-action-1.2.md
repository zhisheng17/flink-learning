---
toc: true
title: 《Flink 实战与性能优化》—— 彻底了解大数据实时计算框架 Flink
date: 2021-07-05
tags:
- Flink
- 大数据
- 流式计算
---

## 1.2 彻底了解大数据实时计算框架 Flink

在 1.1 节中讲解了日常开发常见的实时需求，然后分析了这些需求的实现方式，接着对比了实时计算和离线计算。随着这些年大数据的飞速发展，也出现了不少计算的框架（Hadoop、Storm、Spark、Flink）。在网上有人将大数据计算引擎的发展分为四个阶段。


<!--more-->


+ 第一代：Hadoop 承载的 MapReduce
+ 第二代：支持 DAG（有向无环图）框架的计算引擎 Tez 和 Oozie，主要还是批处理任务
+ 第三代：支持 Job 内部的 DAG（有向无环图），以 Spark 为代表
+ 第四代：大数据统一计算引擎，包括流处理、批处理、AI、Machine Learning、图计算等，以 Flink 为代表

或许会有人不同意以上的分类，笔者觉得其实这并不重要的，重要的是体会各个框架的差异，以及更适合的场景。并进行理解，没有哪一个框架可以完美的支持所有的场景，也就不可能有任何一个框架能完全取代另一个。

本文将对 Flink 的整体架构和 Flink 的多种特性做个详细的介绍！在讲 Flink 之前的话，我们先来看看 **数据集类型** 和 **数据运算模型** 的种类。

#### 数据集类型

数据集类型有分无穷和有界数据集：

+ 无穷数据集：无穷的持续集成的数据集合
+ 有界数据集：有限不会改变的数据集合

那么那些常见的无穷数据集有哪些呢？

+ 用户与客户端的实时交互数据
+ 应用实时产生的日志
+ 金融市场的实时交易记录
+ …

#### 数据运算模型

数据运算模型有分流式处理和批处理：

+ 流式：只要数据一直在产生，计算就持续地进行
+ 批处理：在预先定义的时间内运行计算，当计算完成时释放计算机资源

那么我们再来看看 Flink 它是什么呢？


### 1.2.1 Flink 简介

Flink 是一个针对流数据和批数据的分布式处理引擎，代码主要是由 Java 实现，部分代码是 Scala。它可以处理有界的批量数据集、也可以处理无界的实时数据集，总结如下图所示。对 Flink 而言，其所要处理的主要场景就是流数据，批数据只是流数据的一个极限特例而已，所以 Flink 也是一款真正的流批统一的计算引擎。

![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/images/pRMhfm.jpg)

如下图所示，Flink 提供了 State、Checkpoint、Time、Window 等，它们为 Flink 提供了基石，本篇文章下面会稍作讲解，具体深度分析后面会有专门的文章来讲解。

![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/images/vY6T3M.jpg)


### 1.2.2 Flink 整体架构

Flink 整体架构从下至上分为：

1. 部署：Flink 支持本地运行（IDE 中直接运行程序）、能在独立集群（Standalone 模式）或者在被 YARN、Mesos、K8s 管理的集群上运行，也能部署在云上。

2. 运行：Flink 的核心是分布式流式数据引擎，意味着数据以一次一个事件的形式被处理。

3. API：DataStream、DataSet、Table API & SQL。

4. 扩展库：Flink 还包括用于 CEP（复杂事件处理）、机器学习、图形处理等场景。

整体架构如下图所示：

![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/images/Drsi9h.jpg)


### 1.2.3 Flink 的多种方式部署

作为一个计算引擎，如果要做的足够完善，除了它自身的各种特点要包含，还得支持各种生态圈，比如部署的情况，Flink 是支持以 Standalone、YARN、Kubernetes、Mesos 等形式部署的，如下图所示。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-12-15-034112.png)

每种部署方式介绍如下：

+ Local：直接在 IDE 中运行 Flink Job 时则会在本地启动一个 mini Flink 集群。

+ Standalone：在 Flink 目录下执行 `bin/start-cluster.sh` 脚本则会启动一个 Standalone 模式的集群。

+ YARN：YARN 是 Hadoop 集群的资源管理系统，它可以在群集上运行各种分布式应用程序，Flink 可与其他应用并行于 YARN 中，Flink on YARN 的架构如下图所示。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-12-15-034029.png)

+ Kubernetes：Kubernetes 是 Google 开源的容器集群管理系统，在 Docker 技术的基础上，为容器化的应用提供部署运行、资源调度、服务发现和动态伸缩等一系列完整功能，提高了大规模容器集群管理的便捷性，Flink 也支持部署在 Kubernetes 上，在 [GitHub](https://github.com/Aleksandr-Filichkin/flink-k8s/blob/master/flow.jpg) 看到有下面这种运行架构的。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-05-19-071249.jpg)

通常上面四种居多，另外还支持 AWS、MapR、Aliyun OSS 等。


### 1.2.4 Flink 分布式运行流程

Flink 作业提交架构流程如下图所示：

![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/images/p92UrK.jpg)

具体流程介绍如下：

1. Program Code：我们编写的 Flink 应用程序代码

2. Job Client：Job Client 不是 Flink 程序执行的内部部分，但它是任务执行的起点。 Job Client 负责接受用户的程序代码，然后创建数据流，将数据流提交给 JobManager 以便进一步执行。 执行完成后，Job Client 将结果返回给用户

3. JobManager：主进程（也称为作业管理器）协调和管理程序的执行。 它的主要职责包括安排任务，管理 Checkpoint ，故障恢复等。机器集群中至少要有一个 master，master 负责调度 task，协调 Checkpoints 和容灾，高可用设置的话可以有多个 master，但要保证一个是 leader, 其他是 standby; JobManager 包含 Actor system、Scheduler、Check pointing 三个重要的组件

4. TaskManager：从 JobManager 处接收需要部署的 Task。TaskManager 是在 JVM 中的一个或多个线程中执行任务的工作节点。 任务执行的并行性由每个 TaskManager 上可用的任务槽（Slot 个数）决定。 每个任务代表分配给任务槽的一组资源。 例如，如果 TaskManager 有四个插槽，那么它将为每个插槽分配 25％ 的内存。 可以在任务槽中运行一个或多个线程。 同一插槽中的线程共享相同的 JVM。
   同一 JVM 中的任务共享 TCP 连接和心跳消息。TaskManager 的一个 Slot 代表一个可用线程，该线程具有固定的内存，注意 Slot 只对内存隔离，没有对 CPU 隔离。默认情况下，Flink 允许子任务共享 Slot，即使它们是不同 task 的 subtask，只要它们来自相同的 job。这种共享可以有更好的资源利用率。


### 1.2.5 Flink API

Flink 提供了不同的抽象级别的 API 以开发流式或批处理应用，如下图所示。

![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/images/ozmU46.jpg)

这四种 API 功能分别是：

+ 最底层提供了有状态流。它将通过 Process Function 嵌入到 DataStream API 中。它允许用户可以自由地处理来自一个或多个流数据的事件，并使用一致性、容错的状态。除此之外，用户可以注册事件时间和处理事件回调，从而使程序可以实现复杂的计算。

+ DataStream / DataSet API 是 Flink 提供的核心 API ，DataSet 处理有界的数据集，DataStream 处理有界或者无界的数据流。用户可以通过各种方法（map / flatmap / window / keyby / sum / max / min / avg / join 等）将数据进行转换或者计算。

+ Table API 是以表为中心的声明式 DSL，其中表可能会动态变化（在表达流数据时）。Table API 提供了例如 select、project、join、group-by、aggregate 等操作，使用起来却更加简洁（代码量更少）。
  你可以在表与 DataStream/DataSet 之间无缝切换，也允许程序将 Table API 与 DataStream 以及 DataSet 混合使用。

+ Flink 提供的最高层级的抽象是 SQL 。这一层抽象在语法与表达能力上与 Table API 类似，但是是以 SQL查询表达式的形式表现程序。SQL 抽象与 Table API 交互密切，同时 SQL 查询可以直接在 Table API 定义的表上执行。

Flink 除了 DataStream 和 DataSet API，它还支持 Table API & SQL，Flink 也将通过 SQL 来构建统一的大数据流批处理引擎，因为在公司中通常会有那种每天定时生成报表的需求（批处理的场景，每晚定时跑一遍昨天的数据生成一个结果报表），但是也是会有流处理的场景（比如采用 Flink 来做实时性要求很高的需求），于是慢慢的整个公司的技术选型就变得越来越多了，这样开发人员也就要面临着学习两套不一样的技术框架，运维人员也需要对两种不一样的框架进行环境搭建和作业部署，平时还要维护作业的稳定性。当我们的系统变得越来越复杂了，作业越来越多了，这对于开发人员和运维来说简直就是噩梦，没准哪天凌晨晚上就被生产环境的告警电话给叫醒。所以 Flink 系统能通过 SQL API 来解决批流统一的痛点，这样不管是开发还是运维，他们只需要关注一个计算框架就行，从而减少企业的用人成本和后期开发运维成本。


### 1.2.6 Flink 程序与数据流结构

一个完整的 Flink 应用程序结构如下图所示：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-12-14-141653.png)

它们的功能分别是：

+ Source：数据输入，Flink 在流处理和批处理上的 source 大概有 4 类：基于本地集合的 source、基于文件的 source、基于网络套接字的 source、自定义的 source。自定义的 source 常见的有 Apache kafka、Amazon Kinesis Streams、RabbitMQ、Twitter Streaming API、Apache NiFi 等，当然你也可以定义自己的 source。
+ Transformation：数据转换的各种操作，有 Map / FlatMap / Filter / KeyBy / Reduce / Fold / Aggregations / Window / WindowAll / Union / Window join / Split / Select / Project 等，操作很多，可以将数据转换计算成你想要的数据。
+ Sink：数据输出，Flink 将转换计算后的数据发送的地点 ，你可能需要存储下来，Flink 常见的 Sink 大概有如下几类：写入文件、打印出来、写入 socket 、自定义的 sink 。自定义的 sink 常见的有 Apache kafka、RabbitMQ、MySQL、ElasticSearch、Apache Cassandra、Hadoop FileSystem 等，同理你也可以定义自己的 sink。

代码结构如下图所示：

![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/images/u3RagR.jpg)


### 1.2.7 丰富的 Connector

通过源码可以发现不同版本的 Kafka、不同版本的 ElasticSearch、Cassandra、HBase、Hive、HDFS、RabbitMQ 都是支持的，除了流应用的 Connector 是支持的，另外还支持 SQL，如下图所示。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-10-101956.png)

再就是要考虑计算的数据来源和数据最终存储，因为 Flink 在大数据领域的的定位就是实时计算，它不做存储（虽然 Flink 中也有 State 去存储状态数据，这里说的存储类似于 MySQL、ElasticSearch 等存储），所以在计算的时候其实你需要考虑的是数据源来自哪里，计算后的结果又存储到哪里去。庆幸的是 Flink 目前已经支持大部分常用的组件了，比如在 Flink 中已经支持了如下这些 Connector：

+ 不同版本的 Kafka
+ 不同版本的 ElasticSearch
+ Redis
+ MySQL
+ Cassandra
+ RabbitMQ
+ HBase
+ HDFS
+ ...

这些 Connector 除了支持流作业外，目前还有还有支持 SQL 作业的，除了这些自带的 Connector 外，还可以通过 Flink 提供的接口做自定义 Source 和 Sink（在 3.8 节中）。


### 1.2.8 事件时间&处理时间语义

Flink 支持多种 Time，比如 Event time、Ingestion Time、Processing Time，如下图所示，后面 3.1 节中会很详细的讲解 Flink 中 Time 的概念。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-12-14-140502.png)


### 1.2.9 灵活的窗口机制

Flink 支持多种 Window，比如 Time Window、Count Window、Session Window，还支持自定义 Window，如下图所示。后面 3.2 节中会很详细的讲解 Flink 中 Window 的概念。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-12-15-034900.png)


### 1.2.10 并行执行任务机制

Flink 的程序内在是并行和分布式的，数据流可以被分区成 stream partitions，operators 被划分为 operator subtasks; 这些 subtasks 在不同的机器或容器中分不同的线程独立运行；operator subtasks 的数量在具体的 operator 就是并行计算数，程序不同的 operator 阶段可能有不同的并行数；如下图所示，source operator 的并行数为 2，但最后的 sink operator 为 1：

![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/images/ggMHCK.jpg)


### 1.2.11 状态存储和容错

Flink 是一款有状态的流处理框架，它提供了丰富的状态访问接口，按照数据的划分方式，可以分为 Keyed State 和 Operator State，在 Keyed State 中又提供了多种数据结构：

+ ValueState
+ MapState
+ ListState
+ ReducingState
+ AggregatingState

另外状态存储也支持多种方式：

+ MemoryStateBackend：存储在内存中
+ FsStateBackend：存储在文件中
+ RocksDBStateBackend：存储在 RocksDB 中

Flink 中支持使用 Checkpoint 来提高程序的可靠性，开启了 Checkpoint 之后，Flink 会按照一定的时间间隔对程序的运行状态进行备份，当发生故障时，Flink 会将所有任务的状态恢复至最后一次发生 Checkpoint 中的状态，并从那里开始重新开始执行。另外 Flink 还支持根据 Savepoint 从已停止作业的运行状态进行恢复，这种方式需要通过命令进行触发。


### 1.2.12 自己的内存管理机制

Flink 并不是直接把对象存放在堆内存上，而是将对象序列化为固定数量的预先分配的内存段。它采用类似 DBMS 的排序和连接算法，可以直接操作二进制数据，以此将序列化和反序列化开销降到最低。如果需要处理的数据容量超过内存，那么 Flink 的运算符会将部分数据存储到磁盘。Flink 的主动内存管理和操作二进制数据有几个好处：

+ 保证内存可控，可以防止 OutOfMemoryError
+ 减少垃圾收集压力
+ 节省数据的存储空间
+ 高效的二进制操作

Flink 是如何分配内存、将对象进行序列化和反序列化以及对二进制数据进行操作的，可以参考文章 [Flink 是如何管理好内存的？](http://www.54tianzhisheng.cn/2019/03/24/Flink-code-memory-management/) ，该文中讲解了 Flink 的内存管理机制。


### 1.2.13 多种扩展库

Flink 扩展库中含有机器学习、Gelly 图形处理、CEP 复杂事件处理、State Processing API 等，这些扩展库在一些特殊场景下会比较适用，关于这块内容可以在第六章查看。


### 1.2.14 小结与反思

本节在开始介绍 Flink 之前先讲解了下数据集类型和数据运算模型，接着开始介绍 Flink 的各种特性，对于这些特性，你是否有和其他的计算框架做过对比？