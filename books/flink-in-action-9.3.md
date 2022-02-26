---
toc: true
title: 《Flink 实战与性能优化》—— Flink Parallelism 和 Slot 深度理解
date: 2021-08-09
tags:
- Flink
- 大数据
- 流式计算
---





## 9.3 Flink Parallelism 和 Slot 深度理解

相信使用过 Flink 的你或多或少遇到过下面这个问题（笔者自己的项目曾经也出现过这样的问题），错误信息如下：

<!--more-->

```
Caused by: akka.pattern.AskTimeoutException: 
Ask timed out on [Actor[akka://flink/user/taskmanager_0#15608456]] after [10000 ms]. 
Sender[null] sent message of type "org.apache.flink.runtime.rpc.messages.LocalRpcInvocation".
```

错误信息的完整截图如下图所示。

![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/images/FkaM6A.jpg)

跟着这问题在 Flink 的 Issue 列表里看到了一个类似的问题：[FLINK-9056 issues](https://issues.apache.org/jira/browse/FLINK-9056)，看到该 Issue 下面的评论说出现该问题的原因是因为 TaskManager 的 Slot 数量不足导致的 Job 提交失败，在 Flink 1.63 中已经修复了，变成抛出异常了，修复的代码如下图所示。

![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/images/p4Tr9Z.jpg)

竟然知道了是因为 Slot 不足的原因了，那么我们就要先了解下 Slot 是什么呢？不过在了解 Slot 之前这里先介绍下 Parallelism。


### 9.3.1 Parallelism 简介

Parallelism 翻译成中文是并行的意思，在 Flink 作业里面代表算子的并行度，适当的提高并行度可以大大提高 Job 的执行效率，比如你的 Job 消费 Kafka 数据过慢，适当调大可能就消费正常了。那么在 Flink 中怎么设置并行度呢？


### 9.3.2 如何设置 Parallelism？

在 Flink 配置文件中默认并行度是 1，你可以通过下面的命令查看到配置文件中的默认并行度：

```
cat flink-conf.yaml | grep parallelism
```

结果如下图所示：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-06-055925.png)


所以如果在你的 Flink Job 里面不设置任何 Parallelism 的话，那么它也会有一个默认的 Parallelism（默认为 1），那也意味着可以修改这个配置文件的默认并行度来提高 Job 的执行效率。如果是使用命令行启动你的 Flink Job，那么你也可以这样设置并行度(使用 -p n 参数)：

```
./bin/flink run -p 10 /Users/zhisheng/word-count.jar
```

你也可以在作业中通过 `env.setParallelism(n)` 代码来设置整个作业程序的并行度。

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(10);
```

注意：这样设置的并行度是整个程序的并行度，那么后面如果每个算子不单独设置并行度覆盖的话，那么后面每个算子的并行度就都是以这里设置的并行度为准了。如何给每个算子单独设置并行度呢？

```java
data.keyBy(new xxxKey())
    .flatMap(new XxxFlatMapFunction()).setParallelism(5)
    .map(new XxxMapFunction).setParallelism(5)
    .addSink(new XxxSink()).setParallelism(1)
```

如上就是给每个算子单独设置并行度，这样的话，就算程序设置了 `env.setParallelism(10)` 也是会被覆盖的。这也说明优先级是：算子设置并行度 > env 设置并行度 > 配置文件默认并行度。

并行度讲到这里应该都懂了，下面就继续讲什么是 Slot？


### 9.3.3 Slot 简介

其实 Slot 的概念在 1.2 节中已经提及到，这里再细讲一点。Flink 的作业提交的架构流程如下图所示：

![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/images/r19yJh.jpg)

图中 TaskManager 是从 JobManager 处接收需要部署的 Task，任务能配置的最大并行度由 TaskManager 上可用的 Slot 决定。每个任务代表分配给任务槽的一组资源，Slot 在 Flink 里面可以认为是资源组，Flink 将每个任务分成子任务并且将这些子任务分配到 Slot 中，这样就可以并行的执行程序。

例如，如果 TaskManager 有四个 Slot，那么它将为每个 Slot 分配 25％ 的内存。 可以在一个 Slot 中运行一个或多个线程。 同一 Slot 中的线程共享相同的 JVM。 同一 JVM 中的任务共享 TCP 连接和心跳消息。TaskManager 的一个 Slot 代表一个可用线程，该线程具有固定的内存，注意 Slot 只对内存隔离，没有对 CPU 隔离。默认情况下，Flink 允许子任务共享 Slot，即使它们是不同 Task 的 subtask，只要它们来自相同的 Job，这种共享模式可以大大的提高资源利用率。

如下图所示，有两个 TaskManager，每个 TaskManager 有三个 Slot，这样我们的算子最大并行度那么就可以达到 6 个，在同一个 Slot 里面可以执行 1 至多个子任务。那么再看下图，source/map/keyby/window/apply 算子最大可以设置 6 个并行度，sink 只设置了 1 个并行度。

![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/images/ECv5y2.jpg)

每个 Flink TaskManager 在集群中提供 Slot，Slot 的数量通常与每个 TaskManager 的可用 CPU 内核数成比例（一般情况下 Slot 个数是每个 TaskManager 的 CPU 核数）。Flink 配置文件中设置的一个 TaskManager 默认的 Slot 是 1，配置如下图所示。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-06-062913.png)

`taskmanager.numberOfTaskSlots: 1` 该参数可以根据实际情况做一定的修改。


### 9.3.4 Slot 和 Parallelism 的关系



### 9.3.5 可能会遇到 Slot 和 Parallelism 的问题



### 9.3.6 小结与反思


加入知识星球可以看到上面文章：https://t.zsxq.com/uFEEYzJ

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-09-25-zsxq.jpg)






