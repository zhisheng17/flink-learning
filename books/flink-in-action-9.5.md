---
toc: true
title: 《Flink 实战与性能优化》—— Flink 中如何保证 Exactly Once？
date: 2021-08-11
tags:
- Flink
- 大数据
- 流式计算
---




## 9.5 Flink 中如何保证 Exactly Once？

在分布式场景下，我们的应用程序随时可能出现任何形式的故障，例如：机器硬件故障、程序 OOM 等。当应用程序出现故障时，Flink 为了保证数据消费的 Exactly Once，需要有相应的故障容错能力。Flink 是通过周期性 Checkpoint 的方式来实现故障容错，这里使用的是基于 Chandy-Lamport 改进的算法。本节会介绍 Flink 内部如何保证 Exactly Once 以及端对端如何保证 Exactly Once。

<!--more-->

### 9.5.1 Flink 内部如何保证 Exactly Once？

Flink 官网的定义是 Stateful Computations over Data Streams（数据流上的有状态计算），那到底什么是状态呢？举一个无状态计算的例子，比如：我们只是进行一个字符串拼接，输入a，输出a_666,输入b，输出 b_666。无状态表示计算输出的结果跟之前的状态没关系，符合幂等性。幂等性就是用户对于同一操作发起的一次请求或者多次请求的结果是一致的，不会因为多次点击而产生副作用。而计算 PV、UV 就属于有状态计算。实时计算 PV 时，每次都需要从某个存储介质的结果表中拿到之前的 PV 值，+1 后 set 到结果表中。有状态计算表示输出的结果跟之前的状态有关系，不符合幂等性，访问多次，PV 会增加。

#### Flink的 Checkpoint 功能简介

Flink Checkpoint 机制的存在就是为了解决 Flink 任务在运行过程中由于各种原因导致任务失败后，能够正常恢复任务。那 Checkpoint 具体做了哪些功能，为什么任务挂掉之后，通过 Checkpoint 机制能使得任务恢复呢？Checkpoint 是通过给程序做快照的方式使得将整个程序某些时刻的状态保存下来，当任务挂掉之后，默认从最近一次保存的完整快照处进行恢复任务。问题来了，快照是什么东西？SnapShot翻译为快照，是指将程序中某些信息存一份，后期可以用这些信息来恢复任务。对于一个 Flink 任务来讲，快照里面到底保存着什么信息呢？理论知识一般比较晦涩难懂，我们分析一个案例，用案例辅助大家理解快照里面到底存储什么信息。计算各个 app 的 PV，使用 Flink 该怎么统计呢？

可以把要统计的 app_id 做为 key，对应的 PV 值做为 value，将统计的结果放到一个 Map 集合中，这个 Map 集合可以是内存里的 HashMap 或其他 kv 数据库，例如放到Redis 的 key、value 结构中。从 Kafka 读取到一条条日志，由于要统计各 app 的 PV，所以我们需要从日志中解析出 app_id 字段，每来一条日志，只需要从 Map 集合将相应 app_id 的 PV 值拿出来，+1 后 put 到 Map 中，这样我们的 Map 中永远保存着所有 app 最新的 PV 数据。详细流程如下图所示：

<img src="http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-07-151541.jpg" alt="flink任务task图.png" style="zoom:50%;" />

图中包含三部分：第一个是 Kafka 的一个名为 test 的 Topic，我们的数据来源于这个 Topic，第二个是 Flink 的 Source Task，是 Flink 应用程序读取数据的 Task，第三个是计算 PV 的 Flink Task，用于统计各个 app 的 PV 值，并将 PV 结果输出到 Map 集合。

Flink 的 Source Task 记录了当前消费到 test Topic 所有 partition 的 offset，为了方便理解 Checkpoint 的作用，这里先用一个 partition 进行讲解，假设名为 test 的 Topic只有一个partition0。例：（0，60000）表示0号partition 目前消费到 offset 为 60000 的数据。Flink 的 PV task 记录了当前计算的各 app 的 PV 值，为了方便讲解，这里假设有两个app：app1、app2。例：（app1，50000）（app2，10000）表示 app1 当前 PV 值为50000、app2 当前 PV 值为 10000。计算过程中，每来一条数据，只需要确定相应 app_id，将相应的 PV 值 +1 后 put 到 map 中即可。

该案例中，Checkpoint 到底记录了什么信息呢？记录的其实就是第 n 次 Checkpoint 消费的 offset 信息和各app 的 PV 值信息，记录下发生 Checkpoint 当前的状态信息，并将该状态信息保存到相应的状态后端。（注：**状态后端是保存状态的地方**，决定状态如何保存，如何保证状态高可用，我们只需要知道，我们能从状态后端拿到 offset 信息和 PV 信息即可。状态后端必须是高可用的，否则我们的状态后端经常出现故障，会导致无法通过 Checkpoint 来恢复我们的应用程序）。下面列出了第 100 次 Checkpoint 的时候，状态后端保存的状态信息：

```
chk-100
	- offset：（0，60000）
	- PV：（app1，50000）（app2，10000）
```

该状态信息表示第 100 次 Checkpoint 的时候， partition 0 offset 消费到了 60000，PV 统计结果为（app1，50000）（app2，10000） 。如果任务挂了，如何恢复？

假如我们设置了一分钟进行一次 Checkpoint，第 100 次 Checkpoint 成功后，过了十秒钟，offset已经消费到 （0，60100），PV 统计结果变成了（app1，50080）（app2，10020），突然任务挂了，怎么办？其实很简单，Flink 只需要从最近一次成功的 Checkpoint，也就是从第 100 次 Checkpoint 保存的 offset（0，60000）处接着消费即可，当然 PV 值也要从第 100 次 Checkpoint 里保存的 PV 值（app1，50000）（app2，10000）进行累加，不能从（app1，50080）（app2，10020）处进行累加，因为 **partition 0 offset消费到 60000 时，对应的 PV 统计结果为（app1，50000）（app2，10000）**。当然如果你想从offset （0，60100）PV（app1，50080）（app2，10020）这个状态恢复，也是做不到的，因为那个时刻程序突然挂了，这个状态根本没有保存下来，只有在 Checkpoint 的时候，才会把这些完整的状态保存到状态后端，供我们恢复任务。我们能做的最高效方式就是从最近一次成功的 Checkpoint 处恢复，也就是一直所说的 chk-100。以上基本就是 Checkpoint 承担的工作，为了方便理解，描述的业务场景比较简单。

补充两个问题：计算 PV 的 task 在一直运行，它怎么知道什么时候去做 Checkpoint 呢？计算 PV 的 task 怎么保证它自己计算的 PV 值（app1，50000）（app2，10000）就是offset（0，60000）那一刻的统计结果呢？Flink 在数据中加了一个叫做 barrier（栅栏） 的东西，如下图所示，用圈标注的就是 barrier。

<img src="http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-07-151548.jpg" style="zoom:20%;" />

barrier 从 Source Task 处生成，一直流到 Sink Task，期间所有的 Task 只要碰到 barrier，就会触发自身进行快照。如上图所示，Checkpoint barrier n-1 处做的快照就是指 Job 从开始处理到 barrier n-1 所有的状态数据，barrier n 处做的快照就是指从 Job 开始到处理到 barrier n 所有的状态数据。对应到 PV 案例中就是，Source Task 接收到 JobManager 的编号为 chk-100 的 Checkpoint 触发请求后，发现自己恰好接收到 kafka offset（0，60000）处的数据，所以会往 offset（0，60000）数据之后 offset（0，60001）数据之前插入一个barrier，然后自己开始做快照，也就是将offset（0，60000）保存到状态后端 chk-100 中。然后，Source Task 会把 barrier 和我们要处理的数据一块往下游发送，当统计 PV 的 task 接收到 barrier 后，意味着 barrier 之前的数据已经被 PV task 处理完了，此时也会暂停处理 barrier 之后的数据，将自己内存中保存的 PV 信息（app1，50000）（app2，10000）保存到状态后端 chk-100 中。Flink 大概就是通过以上过程来保存快照的。

上述过程中，barrier 的作用就是为了把数据区分开，barrier 之前的数据是本次 Checkpoint 之前必须处理完的数据，barrier 之后的数据在本次 Checkpoint 之前不能被处理。Checkpoint 过程中有一个同步做快照的环节不能处理 barrier 之后的数据，为什么呢？如果做快照的同时，也在处理数据，那么处理的数据可能会修改快照内容，所以先暂停处理数据，把内存中快照保存好后，再处理数据。结合案例来讲就是，PV task 在对（app1，50000）（app2，10000）做快照的同时，如果 barrier 之后的数据还在处理，可能会导致状态信息还没保存到磁盘，状态已经变成了（app1，50001）（app2，10001），导致我们最后快照里保存的 PV 值变成了（app1，50001）（app2，10001），这样如果从 Checkpoint 恢复任务时，我们从 offset 60000 开始消费，PV 值从 （app1，50001）（app2，10001） 开始累加，就会造成计算的 PV 结果偏高，结果不准确，就不能保证 Exactly Once。所以，Checkpoint 同步做快照的过程中，不能处理 barrier 之后的数据。Checkpoint 将快照信息写入到磁盘后，为了保证快照信息的高可用，需要将快照上传到 HDFS，这个上传快照到 HDFS 的过程是异步进行的，这个过程也可以处理 barrier 之后的数据，处理 barrier 之后的数据不会影响到磁盘上的快照信息。

从 PV 案例再分析 Flink 是如何做 Checkpoint 并从 Checkpoint 处恢复任务的，首先 JobManager 端会向所有 SourceTask 发送 Checkpoint，Source Task 会在数据流中安插 Checkpoint barrier，如下图所示。

<img src="http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-07-151547.jpg" alt="单并行度 PV 案例 Checkpoint 过程图示1" style="zoom:13%;" />

Source Task 安插好 barrier 后，会将 barrier 跟数据一块发送给下游，然后自身开始做快照，并将快照信息 offset (0,60000) 发送到高可用的持久化存储介质，例如 HDFS 上，发送流程如下图所示。

<img src="http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-07-151542.jpg" alt="单并行度 PV 案例 Checkpoint 过程图示2" style="zoom:13%;" />

下游的 PV task 接收到 barrier 后，也会做快照，并将快照信息 PV：(app1,50000) (app2,10000) 发送到 HDFS 上，如下图所示。

<img src="http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-07-151543.jpg" style="zoom:13%;" />

假设第 100 次 Checkpoint 完成后，一段时间后任务挂了，Flink 任务会自动从状态后端恢复任务。Source Task 去读取自己需要的状态信息 offset (0,60000) ，并从 offset 为 60000 的位置接着开始消费数据，PV task 也会去读取需要的状态信息 PV：(app1,50000) (app2,10000)，并在该状态值的基础上，往上累积计算 PV 值，流程如下图所示。

<img src="http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-07-151544.jpg" style="zoom:13%;" />

#### 多并行度、多 Operator 情况下，Checkpoint 的过程

上一节中讲述了单并行度情况下 Checkpoint 的过程，但是生产环境中，一般都是多并行度，而且算子也会比较多，这种情况下 Checkpoint 的过程就会变得复杂。分布式状态容错面临的问题与挑战：

- 如何确保状态拥有**精确一次**的容错保证？
- 如何在分布式场景下替多个拥有本地状态的算子产生**一个全域一致的快照**？
- 如何在**不中断运算**的前提下产生快照？

多并行度、多 Operator 实例的情况下，如何做全域一致的快照？所有的 Operator 运行过程中接收到所有上游算子发送 barrier 后，对自身的状态进行一次快照，保存到相应状态后端，流程如下图所示。

<img src="http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-07-151553.jpg" style="zoom: 13%;" />

当任务从状态恢复时，每个 Operator 从状态后端读取自己相应的状态信息，数据源会从状态中保存的位置开始重新消费，后续的其他算子也会基于 Checkpoint 中保存的状态进行计算，如下图所示。

<img src="http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-07-151556.jpg" alt="多并行度下，任务从 Checkpoint 恢复图示" style="zoom:13%;" />

整个 Checkpoint 的过程跟之前单并行度类似，图中有 4 个带状态的 Operator 实例，相应的状态后端就可以想象成 4 个格子。整个 Checkpoint 的过程可以当做 Operator 实例填自己格子的过程，Operator 实例将自身的状态写到状态后端中相应的格子，当所有的格子填满可以简单的认为一次完整的 Checkpoint 做完了。

上面只是快照的过程，Checkpoint 执行过程如下：

1、JobManager 端的 CheckPointCoordinator 向所有 Source Task 发送 CheckPointTrigger，Source Task会在数据流中安插 Checkpoint barrier

2、当 task 收到所有的 barrier 后，向自己的下游继续传递 barrier，然后自身执行快照，并将自己的状态**异步写入到持久化存储**中

- 增量 CheckPoint 只是把最新的一部分数据更新写入到外部存储
- 为了下游尽快开始做 CheckPoint，所以会先发送 barrier 到下游，自身再同步进行快照

3、当 task 对状态的快照信息完成备份后，会将备份数据的地址（state handle）通知给 JobManager 的 CheckPointCoordinator

如果 Checkpoint 的持续时长超过了 Checkpoint 设定的超时时间，CheckPointCoordinator 还没有收集完所有的 State Handle，CheckPointCoordinator就会认为本次 Checkpoint 失败，会把这次 Checkpoint 产生的所有 状态数据全部删除

4、CheckPointCoordinator 把整个 StateHandle 封装成 Completed Checkpoint Meta，写入到 HDFS，整个 Checkpoint 结束

#### barrier 对齐

什么是 barrier 对齐？如图所示，当前的 Operator 实例接收上游两个流的数据，一个是字母流，一个是数字流。

<img src="http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-07-151558.jpg" style="zoom:10%;" />

当 Checkpoint 时，上游字母流和数字流都会往 Operator 实例发送 Checkpoint barrier，但是由于每个算子的执行速率不同，所以不可能保证上游两个流的 barrier 同时到达 Operator 实例，那图中的 Operator 实例到底什么时候进行快照呢？接收到任意一个 barrier 就可以开始进行快照了吗，还是接收到所有的 barrier 才能开始进行快照呢？答案是：当一个 Operator 实例有多个输入流时，Operator 实例需要在做快照之前进行 barrier 对齐，等待所有输入流的 barrier 都到达。barrier 对齐的详细过程如下所示：

1、对于一个有多个输入流的 Operator 实例，当 Operator 实例从其中一个输入流接收到 Checkpoint barrier n 时，就不能处理来自该流的任何数据记录了，直到它从其他所有输入流接收到 barrier n为止，否则 **Operator 实例 Checkpoint n 的快照会混入快照 n 的记录和快照 n + 1 的记录**。如上图中第 1 个小图所示，数字流的 barrier 先到达了。

2、接收到 barrier n 的流暂时被搁置，从这些流接收的记录不会被处理，而是放入输入缓冲区。图 2 中，我们可以看到虽然数字流对应的 barrier 已经到达了，但是barrier之后的 1、2、3 这些数据只能放到缓冲区中，等待字母流的barrier到达。

3、一旦最后所有输入流都接收到 barrier n，Operator 实例就会把 barrier 之前所有已经处理完成的数据和 barrier n 一块发送给下游。然后 Operator 实例就可以对状态信息进行快照。如图 3 所示，Operator 实例接收到上游所有流的 barrier n，此时 Operator 实例就可以将 barrier 和 barrier 之前的数据发送到下游，然后自身状态进行快照。

4、快照做完后，Operator 实例将继续处理缓冲区的记录，然后就可以处理输入流的数据。如图 4 所示，先处理完缓冲区数据，就可以正常处理输入流的数据了。

上面的过程就是 Flink 在 Operator 实例有多个输入流的情况下，整个 barrier 对齐的过程。那什么是 barrier 不对齐呢？barrier 不对齐是指当还有其他流的 barrier 还没到达时，为了提高 Operator 实例的处理性能，Operator 实例会直接处理 barrier 之后的数据，等到所有流的 barrier 都到达后，就可以对该 Operator 做 Checkpoint 快照了。对应到图中就是，barrier 不对齐时会直接把 barrier 之后的数据 1、2、3 直接处理掉，而**不是**放到缓冲区中等待其他的输入流的 barrier 到达，当所有输入流的 barrier 都到达后，才开始对 Operator 实例的状态信息进行快照，这样会导致做快照之前，Operator 实例已经处理了一些 barrier n 之后的数据。Checkpoint 的目的是为了保存快照信息，如果 barrier 不对齐，那么 Operator 实例在做第 n 次 Checkpoint 之前，已经处理了一些 barrier n 之后的数据，当程序从第 n 次 Checkpoint 恢复任务时，程序会从第 n 次 Checkpoint 保存的 offset 位置开始消费数据，就会导致一些数据被处理了两次，就出现了重复消费。如果进行 barrier 对齐，就不会出现这种重复消费的问题，所以 **barrier 对齐就可以实现 Exactly Once，barrier 不对齐就变成了At Least Once。**

再结合计算 PV 的案例来证明一下，为什么 barrier 对齐就可以实现 Exactly Once，barrier 不对齐就变成了 At Least Once。之前的案例为了简单，描述的 kafka topic 只有 1 个 partition，这里为了讲述 barrier 对齐，假设 topic 有 2 个 partittion，且计算的是我们平台的总 PV，也就是说不需要区分 app，每条一条数据，我们都需要将其 PV 值 +1 即可。如下图所示，Flink 应用程序有两个 Source Task，一个计算 PV 的 Task，这里计算 PV 的 Task 就出现了存在多个输入流的情况。

<img src="http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-07-151546.jpg" style="zoom:13%;" />

假设 barrier 不对齐，那么 Checkpoint 过程是怎么样呢？如下图所示：

<img src="http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-07-151554.jpg" style="zoom:12%;" />

如上图左部分所示，Source Subtask 0 和 Subtask 1 已经完成了快照操作，他们的状态信息为 offset(0,10000)(1,10005) 表示 partition0 消费到 offset 为 10000 的位置，partition 1 消费到 offset 为 10005 的位置。当 Source Subtask 1 的 barrier 到达 PV task 时，计算的 PV 结果为 20002，但 PV task 还没有接收到 Source Subtask 0 发送的 barrier，所以 PV task 还不能对自身状态信息进行快照。由于设置的 barrier 不对齐，所以此时 PV task 会继续处理 Source Subtask 0 和 Source Subtask 1 传来的数据。很快，如上图右部分所示，PV task 接收到 Source Subtask 0 发来的 barrier，但是 PV task 已经处理了 Source Subtask 1 barrier 之后的三条数据，所以 PV 值目前已经为 20008了，这里的 PV=20008 实际上已经处理到 partition 1 offset 为 10008 的位置，此时 PV task 会对自身的状态信息（PV = 20008）做快照，整体的快照信息为 offset(0,10000)(1,10005)  PV=20008。

接着程序在继续运行，过了 10 秒，由于某个服务器故障，导致我们的 Operator 实例有一个挂了，所以 Flink 会从最近一次 Checkpoint 保存的状态恢复。那具体是怎么恢复的呢？Flink 同样会起三个 Operator 实例，我还称他们是 Source Subtask 0 、Source Subtask 1 和 PV task。三个 Operator 会从状态后端读取保存的状态信息。Source Subtask 0 会从 partition 0 offset 为 10000 的位置开始消费，Source Subtask 1 会从 partition 1 offset 为 10005 的位置开始消费，PV task 会基于 PV=20008 进行累加统计。然后就会发现的 PV 值 20008 实际上已经包含了 partition 1 的 offset 10005~10008 的数据，所以 partition 1 从 offset 10005 恢复任务时，partition1 的 offset 10005~10008 的数据被消费了两次，出现了重复消费的问题，所以 barrier 不对齐只能保证 At Least Once。

如果设置为 barrier 对齐，这里能保证 Exactly Once 吗？如下图所示，当 PV task 接收到 Source Subtask 1 的 barrier 后，并不会处理 Source Subtask 1 barrier 之后的数据，而是把这些数据放到 PV task 的输入缓冲区中，直到等到 Source Subtask 0 的 barrier 到达后，PV task 才会对自身状态信息进行快照，此时 PV task 会把 PV=20005 保存到快照信息中，整体的快照状态信息为 offset(0,10000)(1,10005)  PV=20005，当任务从 Checkpoint 恢复时，Source Subtask 0 会从 partition 0 offset 为 10000 的位置开始消费，Source Subtask 1 会从 partition 1 offset 为 10005 的位置开始消费，PV task 会基于 PV=20005 进行累加统计，所以 barrier 对齐能保证 Flink 内部的 Exactly Once。在 Flink 应用程序中，当 Checkpoint 语义设置 Exactly Once 或 At Least Once 时，唯一的区别就是 barrier 对不对齐。当设置为 Exactly Once 时，就会 barrier 对齐，当设置为 At Least Once 时，就会 barrier 不对齐。

<img src="http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-07-151555.jpg" style="zoom:12%;" />

通过本案例，我们应该发现了 barrier 在 Flink 的 Checkpoint 中起着非常大的作用。barrier 告诉 Flink 应用程序，Checkpoint 之前哪些数据不应该被处理，barrier 对齐的过程其实就是为了防止 Flink 应用程序处理重复的数据。总结一下，满足哪些条件时，会出现 barrier 对齐？在代码中设置了 Flink 的 Checkpoint 语义是 Exactly Once，其次 Operator 实例必须有多个输入流才会出现 barrier 对齐。对齐，汉语词汇，释义为使两个以上事物配合或接触得整齐。由汉语解释可得对齐肯定需要两个以上事物，所以必须有多个输入流才可能存在对齐。barrier 对齐就是上游多个流配合使得数据对齐的过程。言外之意：如果 Operator 实例只有一个输入流，就根本不存在 barrier 对齐，自己跟自己默认永远都是对齐的，所以当我们的应用程序从 Source 到 Sink 所有算子的并行度都是 1 的话，就算设置的 At Least Once，无形中也实现了 barrier 对齐，此时 Checkpoint 设置成 Exactly Once 和 At Least Once 一点区别都没有，都可以保证 Exactly Once。看到这里你应该已经知道了哪种情况会出现重复消费了，也应该要掌握为什么 barrier 对齐就能保证 Exactly Once，为什么 barrier 不对齐就是 At Least Once。

barrier 对齐其实是要付出代价的，从 barrier 对齐的过程可以看出，PV task 明明可以更高效的处理数据，但因为 barrier 对齐，导致 Source Subtask 1 barrier 之后的数据被放到缓冲区中，暂时性地没有被处理，假如生产环境中，Source Subtask 0 的 barrier 迟迟没有到达，比 Source Subtask 1 延迟了 30 秒，那么这 30 秒期间，Source Subtask 1 barrier 之后的数据不能被处理，所以 PV task 相当于被闲置了。所以，当我们的一些业务场景对 Exactly Once 要求不高时，我们可以设置 Flink 的 Checkpoint 语义是 At Least Once 来小幅度的提高应用程序的执行效率。Flink Web UI 的 Checkpoint 选项卡中可以看到 barrier 对齐的耗时，如果发现耗时比较长，且对 Exactly Once 语义要求不高时，可以考虑使用该优化方案。

前面提到如何在不中断运算的前提下产生快照？在 Flink 的 Checkpoint 过程中，无论下游算子有没有做完快照，只要上游算子将 barrier 发送到下游且上游算子自身已经做完快照时，那么上游算子就可以处理 barrier 之后的数据了，从而使得整个系统 Checkpoint 的过程影响面尽量缩到最小，来提升系统整体的吞吐量。

在整个 Checkpoint 的过程中，还存在一个问题，假设我们设置的 10 分钟一次 Checkpoint。在第 n 次 Checkpoint 成功后，过了 9 分钟，任务突然挂了，我们需要从最近一次成功的 Checkpoint 处恢复任务，也就是从 9 分钟之前的状态恢复任务，就需要把这 9分钟的数据全部再消费一次，成本比较大。有的同学可能会想，那可以不可以设置为 100 ms就做一次 Checkpoint 呢？这样的话，当任务出现故障时，就不需要从 9 分钟前的状态进行恢复了，直接从 100 ms之前的状态恢复即可，恢复就会很快，不需要处理大量重复数据了。但是，这样做会导致应用程序频繁的访问状态后端，一般我们为了高可用，会把状态里的数据比如 offset：（0，60000）PV：（app1，50000）（app2，10000） 信息保存到 HDFS 中，如果频繁访问 HDFS，肯定会造成吞吐量下降，所以一般我们的 Checkpoint 时间间隔可以设置为分钟级别，例如 1 分钟、3 分钟，对于状态很大的任务每次 Checkpoint 访问 HDFS 比较耗时，我们甚至可以设置为 5 分钟一次 Checkpoint，毕竟我们的应用程序挂的概率并不高，偶尔一次从 5 分钟前的状态恢复，我们是可以接受的。可以根据业务场景合理地调节 Checkpoint 的间隔时长，对于状态很小的 Job Checkpoint 会很快，我们可以调小时间间隔，对于状态比较大的 Job Checkpoint 会比较慢，我们可以调大 Checkpoint 时间间隔。

有的同学可能还有疑问，明明说好的 Exactly Once，但在 Checkpoint 成功后 10s 发生了故障，从最近一次成功的 Checkpoint 处恢复时，由于发生故障前的 10s Flink 也在处理数据，所以 Flink 应用程序肯定是把一些数据重复处理了呀。在面对任意故障时，不可能保证每个算子中用户定义的逻辑在每个事件中只执行一次，因为用户代码被部分执行的可能性是永远存在的。那么，当引擎声明 Exactly Once 处理语义时，它们能保证什么呢？如果不能保证用户逻辑只执行一次，那么哪些逻辑只执行一次？当引擎声明 Exactly Once 处理语义时，它们实际上是在说，它们可以保证引擎管理的状态更新只提交一次到持久的后端存储。换言之，无论以什么维度计算 PV、无论 Flink 应用程序发生多少次故障导致重启从 Checkpoint 恢复，Flink 都可以保证 PV 结果是准确的，不会因为各种任务重启而导致 PV 值计算偏高。

为了下游尽快做 Checkpoint，所以会先发送 barrier 到下游，自身再同步进行快照。这一步，如果向下发送barrier后，自己同步快照慢怎么办？下游已经同步好了，自己还没？可能会出现下游比上游快照还早的情况，但是这不影响快照结果，只是下游做快照更及时了，我只要保证下游把barrier之前的数据都处理了，并且不处理 barrier 之后的数据，然后做快照，那么下游也同样支持 Exactly Once。这个问题不要从全局思考，单独思考上游和下游的实例，你会发现上下游的状态都是准确的，既没有丢，也没有重复计算。这里需要注意，如果有一个Operator 的 Checkpoint 失败了或者因为 Checkpoint 超时也会导致失败，那么 JobManager 会认为整个 Checkpoint 失败。失败的 Checkpoint 是不能用来恢复任务的，必须所有的算子的 Checkpoint 都成功，那么这次 Checkpoint 才能认为是成功的，才能用来恢复任务。对应到 PV 案例就是，PV task 做快照速度较快，PV=20005 较早地写入到了 HDFS，但是 offset(0,10000)(1,10005) 过了几秒才写入到 HDFS，这种情况就算出现了，也不会影响计算结果，因为我们的快照信息是完全正确的。

再分享一个案例，Flink 的 Checkpoint 语义设置了 Exactly Once，程序中设置了 1 分钟 1 次 Checkpoint，5 秒向 MySQL 写一次数据，并commit。最后发现 MySQL 中数据重复了。为什么会重复呢？Flink要求端对端的 Exactly Once 都必须实现 TwoPhaseCommitSinkFunction。如果你的 Checkpoint 成功了，过了30秒突然程序挂了，由于 5 秒 commit 一次，所以在应用程序挂之前的 30 秒实际上已经写入了 6 批数据进入 MySQL。从 Checkpoint 处恢复时，之前提交的 6 批数据就会重复写入，所以出现了重复消费。Flink 的 Exactly Once 有两种情况，一个是我们本节所讲的 Flink 内部的 Exactly Once，一个是端对端的 Exactly Once。关于端对端如何保证 Exactly Once，我们在下一节中深入分析。


### 9.5.2 端对端如何保证 Exactly Once？

Flink 与外部存储介质之间进行数据交互统称为端对端或 end to end 数据传输。上一节讲述了 Flink 内部如何保证 Exactly Once，这一节来分析端对端的 Exactly Once。正如上述 Flink 写 MySQL 的案例所示，在第 n 次 Checkpoint 结束后，第 n+1 次 Checkpoint 之前，如果 Flink 应用程序已经向外部的存储介质中成功写入并提交了一些数据后，Flink 应用程序由于某些原因挂了，导致任务从第 n 次 Checkpoint 处恢复。这种情况下，就会导致第 n 次 Checkpoint 结束后且任务失败之前往外部存储介质中写入的那一部分数据重复写入两次，可能会导致相同的数据在存储介质中存储了两份，从而端对端的一致性语义保证从 Exactly Once 退化为 At Least Once。这里只考虑了数据重复的情况，为什么不考虑丢数据的情况呢？在写数据时可以对异常进行捕获增加重试策略，如果重试多次还没有成功可以让 Flink 任务失败，Flink 任务就会从最近一次成功的 Checkpoint 处恢复，就不会出现丢数据的情况，所以我们本节内容主要用来解决数据重复的问题。

针对上述端对端 Exactly Once 的问题，我们可以使用以下方案来解决：

1、假如我们使用的存储介质支持按照全局主键去重，那么比较容易实现 Exactly Once，无论相同的数据往外部存储中写入了几次，外部存储都会进行去重，只保留一条数据。例如，app1 的 PV 值为 10，现在把 （key=app1，value=10） 往 Redis 中写入 10 次，只是说把 value 值覆盖了 10次，并不会导致结果错误，这种方案属于幂等性写入。

2、我们上述案例中为什么会导致重复写入数据到外部存储呢？是因为在下一次 Checkpoint 之前如果任务失败时，一些数据已经成功写入到了外部存储中，没办法删除那些数据。既然问题是这样，那可以想办法把 "向外部存储中提交数据" 与 "Checkpoint" 强关联，两次 Checkpoint 之间不允许向外部存储介质中提交数据，Checkpoint 的时候再向外部存储提交。如果提交成功，则 Checkpoint 成功，提交失败，则 Checkpoint 也失败。这样在下一次 Checkpoint 之前，如果任务失败，也没有重复数据被提交到外部存储。这里只是描述一下大概思想，好多细节这里并没有详细描述，会在下文中详细描述。基于上述思想，Flink 实现了 TwoPhaseCommitSinkFunction，它提取了两阶段提交协议的通用逻辑，使得通过 Flink 来构建端到端的Exactly Once 程序成为可能。它提供了一个抽象层，用户只需要实现少数方法就能实现端到端的 Exactly Once 语义。不过这种方案必须要求我们的输出端 (Sink 端) 必须支持事务。

下面我们通过两部分来详细介绍上述两种方案。

#### 幂等性写入如何保证端对端的 Exactly Once

实时 ETL 当 HBase 做为 Sink 端时，就是典型的应用场景。把日志中的主键做为 HBase 的 RowKey，就可以保证数据不重复，实现比较简单，这里不多赘述。

继续探讨实时计算各 app PV 的案例，将统计结果以普通键值对的形式保存到 Redis 中供业务方查询。到底如何实现，才能保证 Redis 中的结果是精准的呢？在之前 Strom 或 Spark Streaming 的方案中，将统计的 PV 结果保存在 Redis 中，每来一条数据，从 Redis 中获取相应 app 对应的 PV 值然后内存中进行 +1 后，再将 PV 值 put 到 Redis 中。例如：Redis 中保存 app1 的 PV 为 10，现在来了一条 app1 的日志，首先从 Redis 中获取 app1 的 PV 值=10，内存中 10+1=11，将 (app1,11) put 到 Redis 中，这里的 11 就是我们统计的 app1 的 PV 结果。可以将这种方案优化为 incr 或 incrby，直接对 Redis 中的 10 进行累加，不需要手动在内存中进行累加操作。当然 Flink 也可以用上述的这种方案来统计各 app 的 PV，但是上述方案并不能保证 Exactly Once，为什么呢？当第 n 次 Checkpoint 时，app1 的 PV 结果为 10000，第 n 次 Checkpoint 结束后运行了 10 秒，Redis 中 app1 的 PV 结果已经累加到了 10200。此时如果任务挂了，从第 n 次 Checkpoint 恢复任务时，会继续按照 Redis 中保存的 PV=10200 进行累加，但是正确的结果应该是从 PV=10000 开始累加。如果按照上面的方案统计 PV，就可能会出现统计值偏高的情况。这里也证实了一点：并不是说 Flink 程序的 Checkpoint 语义设置为 Exactly Once，就能保证我们的统计结果或者各种输出结果都能满足 Exactly Once。为了编写真正满足 Exactly Once 的代码，我们需要对 Flink 的 Checkpoint 原理做一些了解，编写对 Exactly Once 友好的代码。

那如何编写代码才能使得最后在 Redis 中保存的 PV 结果满足 Exactly Once 呢？上一节中，讲述了 Flink 内部状态可以保证 Exactly Once，这里可以将统计的 PV 结果保存在 Flink 内部的状态里，每次基于状态进行累加操作，并将累加到的结果 put 到 Redis 中，这样当任务从 Checkpoint 处恢复时，并不是基于 Redis 中实时统计的 PV 值进行累加，而是基于 Checkpoint 中保存的 PV 值进行累加，Checkpoint 中会保存每次 Checkpoint 时对应的 PV 快照信息，例如：第 n 次 Checkpoint 会把当时 pv=10000 保存到快照信息里，同时状态后端还保存着一份实时的状态信息用于实时累加。示例代码如下所示：

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 1 分钟一次Checkpoint
env.enableCheckpointing(TimeUnit.MINUTES.toMillis(1));

CheckpointConfig checkpointConf = env.getCheckpointConfig();
// Checkpoint 语义 EXACTLY ONCE
checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

Properties props = new Properties();
props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ConsumerConfig.GROUP_ID_CONFIG, "app-pv-stat");

DataStreamSource<String> appInfoSource = env.addSource(new FlinkKafkaConsumer011<>(
        // kafka topic， String 序列化
        "app-topic",  new SimpleStringSchema(), props));

// 按照 appId 进行 keyBy
appInfoSource.keyBy((KeySelector<String, String>) appId -> appId)
        .map(new RichMapFunction<String, Tuple2<String, Long>>() {
            private ValueState<Long> pvState;
            private long pv = 0;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 初始化状态
                pvState = getRuntimeContext().getState(
                        new ValueStateDescriptor<>("pvStat",
                        TypeInformation.of(new TypeHint<Long>() {})));
            }

            @Override
            public Tuple2<String, Long> map(String appId) throws Exception {
                // 从状态中获取该 app 的 PV 值，+1后，update 到状态中
                if(null == pvState.value()){
                    pv = 1;
                } else {
                    pv = pvState.value();
                    pv += 1;
                }
                pvState.update(pv);
                return new Tuple2<>(appId, pv);
            }
        })
        .print();

env.execute("Flink PV stat");
```

代码中设置 1 分钟一次 Checkpoint，Checkpoint 语义 EXACTLY ONCE，从 Kafka 中读取数据，这里为了简化代码，所以 Kafka 中读取的直接就是 String 类型的 appId，按照 appId KeyBy 后，执行 RichMapFunction，RichMapFunction 的 open 方法中会初始化 ValueState<Long> 类型的 pvState，pvState 就是上文一直强调的状态信息，每次 Checkpoint 的时候，会把 pvState 的状态信息快照一份到 HDFS 来提供恢复。这里按照 appId 进行 keyBy，所以每一个 appId 都会对应一个 pvState，pvState 里存储着该 appId 对应的 pv 值。每来一条数据都会执行一次 map 方法，当这条数据对应的 appId 是新 app 时，pvState 里就没有存储这个 appId 当前的 pv 值，将 pv 值赋值为 1，当 pvState 里存储的 value 不为 null 时，拿出 pv 值 +1后 update 到 pvState 里。map 方法再将 appId 和 pv 值发送到下游算子，下游直接调用了 print 进行输出，这里完全可以替换成相应的 RedisSink 或 HBaseSink。本案例中计算 pv 的工作交给了 Flink 内部的 ValueState，不依赖外部存储介质进行累加，外部介质承担的角色仅仅是提供数据给业务方查询，所以无论下游使用什么形式的 Sink，只要 Sink 端能够按照主键去重，该统计方案就可以保证 Exactly Once。本案例使用的 ValueState，关于 State 的详细使用请参阅第3.1节。

#### TwoPhaseCommitSinkFunction 如何保证端对端的 Exactly Once

Flink 的源码中有这么一段注释：This is a recommended base class for all of the {@link SinkFunction} that intend to implement exactly-once semantic。意思是对于打算实现 Exactly Once 语义的所有 SinkFunction 都推荐继承该抽象类。在介绍 TwoPhaseCommitSinkFunction 之前，先了解一下 2PC 分布式一致性协议。

在分布式系统中，每一个机器节点虽然都能明确地知道自己在进行事务操作过程中的结果是成功或失败，但无法直接获取到其他分布式节点的操作结果。因此，当一个事务操作需要跨越多个分布式节点的时候，为了让每个节点都能够获取到其他节点的事务执行状况，需要引入一个"协调者（Coordinator）"节点来统一调度所有分布式节点的执行逻辑，这些被调度的分布式节点被称为"参与者（Participant）"。协调者负责调度参与者的行为，并最终决定这些参与者是否要把事务真正的提交。
普通的事务可以保证单个事务内所有操作要么全部成功，要么全部失败，而分布式系统中具体如何保证多台节点上执行的事务要么所有节点事务都成功，要么所有节点事务都失败呢？先了解一下 2PC 一致性协议。

2PC 是 Two-Phase Commit 的缩写，即两阶段提交。2PC 将分布式事务分为了两个阶段，分别是提交事务请求（投票）和执行事务提交。协调者会根据参与者在第一阶段的投票结果，来决定第二阶段是否真正的执行事务，具体流程如下。

##### 提交事务请求（投票）阶段

提交事务请求阶段如下所示：

+ 协调者向所有参与者发送 prepare 请求与事务内容，询问是否可以准备事务提交，并等待参与者的响应
+ 各参与者执行事务操作，并记录 Undo日志（用于回滚）和 Redo日志（用于重放），但不真正提交
+ 参与者向协调者返回事务操作的执行结果，执行成功返回 Yes，否则返回 No

##### 执行事务提交阶段

分为成功与失败两种情况：

- 若第一阶段所有参与者都返回 Yes，说明事务可以提交
    + 协调者向所有参与者发送 Commit 请求
    + 参与者收到 Commit 请求后，会正式执行事务提交操作，并在提交完成后释放事务资源
    + 完成事务提交后，向协调者发送 Ack 消息
    + 协调者收到所有参与者的 Ack 消息，完成事务

事务提交成功的流程如下图所示：

<img src="http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-07-151552.jpg" style="zoom:12%;" />

- 若第一阶段有参与者返回 No 或者超时未返回，说明事务中断，需要回滚
    + 协调者向所有参与者发送 Rollback 请求
    + 参与者收到 Rollback 请求后，根据 Undo日志回滚到事务执行前的状态，释放占用的事务资源
    + 参与者在完成事务回滚后，向协调者返回 Ack
    + 协调者收到所有参与者的 Ack 消息，事务回滚完成

事务提交中断，需要回滚的流程如下图所示：

<img src="http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-07-151557.jpg" style="zoom:12%;" />

简单来讲，2PC 将一个事务的处理过程分为了投票和执行两个阶段，其核心是每个事务都采用先尝试后提交的处理方式。2PC 的优缺点如下所示：

优点：原理简单，实现方便

缺点：

- 协调者单点问题：协调者在整个 2PC 协议中非常重要，一旦协调者故障，则 2PC 将无法运转
- 过于保守：在 2PC 的阶段一，如果参与者出现故障而导致协调者无法获取到参与者的响应信息，这时协调者只能依靠自身的超时机制来判断是否需要中断事务，这种策略比较保守。换言之，2PC 没有涉及较为完善的容错机制，任意一个节点失败都会导致整个事务的失败
- 同步阻塞：执行过程是完全同步的，各个参与者在等待其他参与者投票响应的的过程中，将无法进行其他任何操作
- 数据不一致：在二阶段提交协议的阶段二，当协调者向所有的参与者发送 Commit 请求后，出现了局部网络异常或局部参与者机器故障等因素导致一部分的参与者执行了 Commit 操作，而发生故障的参与者没有执行 Commit，于是整个分布式系统便出现了数据不一致现象

Flink 的 TwoPhaseCommitSinkFunction 是基于 2PC 实现的。Flink 的 JobManager 对应到 2PC 中的协调者，Operator 实例对应到 2PC 中的参与者。TwoPhaseCommitSinkFunction 实现了 CheckpointedFunction 和 CheckpointListener 接口。CheckpointedFunction 接口中有两个方法 snapshotState 和 initializeState，snapshotState 方法会在 Checkpoint 时且做快照之前被调用，initializeState 方法会在自定义 Function 初始化恢复状态时被调用。CheckpointListener 接口中有一个 notifyCheckpointComplete 方法，Operator 实例的 Checkpoint 成功后，会反馈给 JobManager，当 JobManager 接收到所有 Operator 实例 Checkpoint 成功的通知后，就认为本次 Checkpoint 成功了，会给所有 Operator 实例发送一个 Checkpoint 完成的通知，Operator 实例接收到通知后，就会调用 notifyCheckpointComplete 方法。

TwoPhaseCommitSinkFunction定义了如下 5 个抽象方法：

```java
// 处理每一条数据
protected abstract void invoke(TXN transaction, IN value, Context context) throws Exception;
// 开始一个事务，返回事务信息的句柄
protected abstract TXN beginTransaction() throws Exception;
// 预提交（即提交请求）阶段的逻辑
protected abstract void preCommit(TXN transaction) throws Exception;
// 正式提交阶段的逻辑
protected abstract void commit(TXN transaction);
// 取消事务,Rollback 相关的逻辑
protected abstract void abort(TXN transaction);
```



### 9.5.3 分析 FlinkKafkaConsumer 的设计思想


#### kafka offset 存储及如何实现 Consumer 实例消费 partition 的负载均衡


#### Source 端并行度改变了，如何来恢复 offset



#### 如何实现自动发现当前消费 topic 下新增的 partition


### 9.5.4 小结与反思


加入知识星球可以看到上面文章：https://t.zsxq.com/uFEEYzJ

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-09-25-zsxq.jpg)






