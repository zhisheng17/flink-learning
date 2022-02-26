---
toc: true
title: 《Flink 实战与性能优化》—— 大数据计算框架对比
date: 2021-07-06
tags:
- Flink
- 大数据
- 流式计算
---

## 1.3 大数据计算框架对比

在 1.2 节中已经跟大家详细介绍了 Flink，那么在本节就主要 Blink、Spark Streaming、Structured Streaming 和 Storm 的区别。


<!--more-->


### 1.3.1 Flink

Flink 是一个针对流数据和批数据分布式处理的引擎，在某些对实时性要求非常高的场景，基本上都是采用 Flink 来作为计算引擎，它不仅可以处理有界的批数据，还可以处理无界的流数据，在 Flink 的设计愿想就是将批处理当成是流处理的一种特例。

如下图所示，在 Flink 的母公司 [Data Artisans 被阿里收购](https://www.eu-startups.com/2019/01/alibaba-takes-over-berlin-based-streaming-analytics-startup-data-artisans/)之后，阿里也在开始逐步将内部的 Blink 代码开源出来并合并在 Flink 主分支上。

![阿里巴巴收购 Data Artisans](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-07-01-143012.jpg)

而 Blink 一个很强大的特点就是它的 Table API & SQL 很强大，社区也在 Flink 1.9 版本将 Blink 开源版本大部分代码合进了 Flink 主分支。


### 1.3.2 Blink

Blink 是早期阿里在 Flink 的基础上开始修改和完善后在内部创建的分支，然后 Blink 目前在阿里服务于阿里集团内部搜索、推荐、广告、菜鸟物流等大量核心实时业务，如下图所示。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-07-01-160059.jpg)

Blink 在阿里内部错综复杂的业务场景中锻炼成长着，经历了内部这么多用户的反馈（各种性能、资源使用率、易用性等诸多方面的问题），Blink 都做了针对性的改进。在 Flink Forward China 峰会上，阿里巴巴集团副总裁周靖人宣布 Blink 在 2019 年 1 月正式开源，同时阿里也希望 Blink 开源后能进一步加深与 Flink 社区的联动，

Blink 开源地址：[https://github.com/apache/flink/tree/blink](https://github.com/apache/flink/tree/blink)

开源版本 Blink 的主要功能和优化点：

1、Runtime 层引入 Pluggable Shuffle Architecture，开发者可以根据不同的计算模型或者新硬件的需要实现不同的 shuffle 策略进行适配；为了性能优化，Blink 可以让算子更加灵活的 chain 在一起，避免了不必要的数据传输开销；在 BroadCast Shuffle 模式中，Blink 优化掉了大量的不必要的序列化和反序列化开销；Blink 提供了全新的 JM FailOver 机制，JM 发生错误之后，新的 JM 会重新接管整个 JOB 而不是重启 JOB，从而大大减少了 JM FailOver 对 JOB 的影响；Blink 支持运行在 Kubernetes 上。

2、SQL/Table API 架构上的重构和性能的优化是 Blink 开源版本的一个重大贡献。

3、Hive 的兼容性，可以直接用 Flink SQL 去查询 Hive 的数据，Blink 重构了 Flink catalog 的实现，并且增加了两种 catalog，一个是基于内存存储的 FlinkInMemoryCatalog，另外一个是能够桥接 Hive metaStore 的 HiveCatalog。

4、Zeppelin for Flink

5、Flink Web，更美观的 UI 界面，查看日志和监控 Job 都变得更加方便

对于开源那会看到一个对话让笔者感到很震撼：

> Blink 开源后，两个开源项目之间的关系会是怎样的？未来 Flink 和 Blink 也会由不同的团队各自维护吗？

> Blink 永远不会成为另外一个项目，如果后续进入 Apache 一定是成为 Flink 的一部分

对话详情如下图所示：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-07-01-162836.jpg)

在 Blink 开源那会，笔者就将源码自己编译了一份，然后自己在本地一直运行着，感兴趣的可以看看文章 [阿里巴巴开源的 Blink 实时计算框架真香](http://www.54tianzhisheng.cn/2019/02/28/blink/) ，你会发现 Blink 的 UI 还是比较美观和实用的。

如果你还对 Blink 有什么疑问，可以看看下面两篇文章：

[阿里重磅开源 Blink：为什么我们等了这么久？](https://www.infoq.cn/article/wZ_b7Hw9polQWp3mTwVh)

[重磅！阿里巴巴 Blink 正式开源，重要优化点解读](https://www.infoq.cn/article/ZkOGAl6_vkZDTk8tfbbg)


### 1.3.3 Spark

Apache Spark 是一种包含流处理能力的下一代批处理框架。与 Hadoop 的 MapReduce 引擎基于各种相同原则开发而来的 Spark 主要侧重于通过完善的内存计算和处理优化机制加快批处理工作负载的运行速度。Spark 可作为独立集群部署（需要相应存储层的配合），或可与 Hadoop 集成并取代 MapReduce 引擎。

[Spark Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide.html) 是 Spark API 核心的扩展，可实现实时数据的快速扩展，高吞吐量，容错处理。数据可以从很多来源（如 Kafka、Flume、Kinesis 等）中提取，并且可以通过很多函数来处理这些数据，处理完后的数据可以直接存入数据库或者 Dashboard 等，如下两图所示。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-07-06-154210.jpg)

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-07-06-134257.jpg)

**Spark Streaming 的内部实现原理**是接收实时输入数据流并将数据分成批处理，然后由 Spark 引擎处理以批量生成最终结果流，也就是常说的 micro-batch 模式，如下图所示。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-07-06-134430.jpg)

**Spark DStreams**

DStreams 是 Spark Streaming 提供的基本的抽象，它代表一个连续的数据流。它要么是从源中获取的输入流，要么是输入流通过转换算子生成的处理后的数据流。在内部实现上，DStream 由连续的序列化 RDD 来表示，每个 RDD 含有一段时间间隔内的数据，如下图所示：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-07-06-140956.jpg)

任何对 DStreams 的操作都转换成了对 DStreams 隐含的 RDD 的操作。例如 flatMap 操作应用于 lines 这个 DStreams 的每个 RDD，生成 words 这个 DStreams 的 RDD 过程如下图所示：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-07-06-134718.jpg)

通过 Spark 引擎计算这些隐含 RDD 的转换算子。DStreams 操作隐藏了大部分的细节，并且为了更便捷，为开发者提供了更高层的 API。

**Spark 支持的滑动窗口**

它和 Flink 的滑动窗口类似，支持传入两个参数，一个代表窗口长度，一个代表滑动间隔，如下图所示。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-07-06-134915.jpg)

**Spark 支持更多的 API**

因为 Spark 是使用 Scala 开发的居多，所以从官方文档就可以看得到对 Scala 的 API 支持的很好，而 Flink 源码实现主要以 Java 为主，因此也对 Java API 更友好，从两者目前支持的 API 友好程度，应该是 Spark 更好，它目前也支持 Python API，但是 Flink 新版本也在不断的支持 Python API。

**Spark 支持更多的 Machine Learning Lib**

你可以很轻松的使用 Spark MLlib 提供的机器学习算法，然后将这些这些机器学习算法模型应用在流数据中，目前 Flink Machine Learning 这块的内容还较少，不过阿里宣称会开源些 Flink Machine Learning 算法，保持和 Spark 目前已有的算法一致，我自己在 GitHub 上看到一个阿里开源的仓库，感兴趣的可以看看 [flink-ai-extended](https://github.com/alibaba/flink-ai-extended)。

**Spark Checkpoint**

Spark 和 Flink 一样都支持 Checkpoint，但是 Flink 还支持 Savepoint，你可以在停止 Flink 作业的时候使用 Savepoint 将作业的状态保存下来，当作业重启的时候再从 Savepoint 中将停止作业那个时刻的状态恢复起来，保持作业的状态和之前一致。

**Spark SQL**

Spark 除了 DataFrames 和 Datasets 外，也还有 SQL API，这样你就可以通过 SQL 查询数据，另外 Spark SQL 还可以用于从 Hive 中读取数据。

从 Spark 官网也可以看到很多比较好的特性，这里就不一一介绍了，如果对 Spark 感兴趣的话也可以去[官网](https://spark.apache.org/docs/latest/index.html)了解一下具体的使用方法和实现原理。

**Spark Streaming 优缺点**

1、优点

+ Spark Streaming 内部的实现和调度方式高度依赖 Spark 的 DAG 调度器和 RDD，这就决定了 Spark Streaming 的设计初衷必须是粗粒度方式的，也就无法做到真正的实时处理
+ Spark Streaming 的粗粒度执行方式使其确保“处理且仅处理一次”的特性，同时也可以更方便地实现容错恢复机制。
+ 由于 Spark Streaming 的 DStream 本质是 RDD 在流式数据上的抽象，因此基于 RDD 的各种操作也有相应的基于 DStream 的版本，这样就大大降低了用户对于新框架的学习成本，在了解 Spark 的情况下用户将很容易使用 Spark Streaming。

2、缺点

+ Spark Streaming 的粗粒度处理方式也造成了不可避免的数据延迟。在细粒度处理方式下，理想情况下每一条记录都会被实时处理，而在 Spark Streaming 中，数据需要汇总到一定的量后再一次性处理，这就增加了数据处理的延迟，这种延迟是由框架的设计引入的，并不是由网络或其他情况造成的。
+ 使用的是 Processing Time 而不是 Event Time


### 1.3.4 Structured Streaming



### 1.3.5 Storm


#### Storm 核心组件



#### Storm 核心概念


#### Storm 数据处理流程图


### 1.3.6 计算框架对比


#### Flink VS Spark


#### Flink VS Storm


#### 全部对比结果


加入知识星球可以看到上面文章：https://t.zsxq.com/vVjeMBY

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-09-25-zsxq.jpg)




### 1.3.7 小结与反思


因在 1.2 节中已经对 Flink 的特性做了很详细的讲解，所以本节主要介绍其他几种计算框架（Blink、Spark、Spark Streaming、Structured Streaming、Storm），并对比分析了这几种框架的特点与不同。你对这几种计算框架中的哪个最熟悉呢？了解过它们之间的差异吗？你有压测过它们的处理数据的性能吗？

本章第一节从公司的日常实时计算需求出发，来分析该如何去实现这种实时需求，接着对比了实时计算与离线计算的区别，从而引出了实时计算的优势，接着就在第二节开始介绍本书的重点 —— 实时计算引擎 Flink，把 Flink 的架构、API、特点、优势等方面都做了讲解，在第三节中对比了市面上现有的计算框架，分别对这些框架做了异同点对比，最后还汇总了它们在各个方面的优势和劣势，以供大家公司内部的技术选型。