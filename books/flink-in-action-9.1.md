---
toc: true
title: 《Flink 实战与性能优化》—— 如何处理 Flink Job BackPressure （反压）问题?
date: 2021-08-07
tags:
- Flink
- 大数据
- 流式计算
---




# 第九章 —— Flink 性能调优

通过第八章的监控图表信息，我们可以发现问题，在发现问题后，需要去分析为什么会发生这些问题以及我们该如何去解决这些问题。本章将会介绍很多 Flink 生产环境遇到的问题，比如作业出现反压、作业并行度配置不合理、作业数据倾斜等，除了引出这种常见问题之外，笔者还将和你一起去分析这种问题造成的原因以及如何去优化作业。比如合理的配置并行度、让作业算子尽可能的 chain 在一起已达到最优等。希望通过本章的内容，你可以将这些解决方法运用在你的公司，帮助公司解决类似的问题。


## 9.1 如何处理 Flink Job BackPressure （反压）问题?

反压（BackPressure）机制被广泛应用到实时流处理系统中，流处理系统需要能优雅地处理反压问题。反压通常产生于这样的场景：短时间的负载高峰导致系统接收数据的速率远高于它处理数据的速率。许多日常问题都会导致反压，例如，垃圾回收停顿可能会导致流入的数据快速堆积，或遇到大促、秒杀活动导致流量陡增。反压如果不能得到正确的处理，可能会导致资源耗尽甚至系统崩溃。反压机制是指系统能够自己检测到被阻塞的 Operator，然后自适应地降低源头或上游数据的发送速率，从而维持整个系统的稳定。Flink 任务一般运行在多个节点上，数据从上游算子发送到下游算子需要网络传输，若系统在反压时想要降低数据源头或上游算子数据的发送速率，那么肯定也需要网络传输。所以下面先来了解一下 Flink 的网络流控（Flink 对网络数据流量的控制）机制。

<!--more-->

### 9.1.1 Flink 流处理为什么需要网络流控

下图是一个简单的 Flink 流任务执行图：任务首先从 Kafka 中读取数据、通过 map 算子对数据进行转换、keyBy 按照指定 key 对数据进行分区（key 相同的数据经过 keyBy 后分到同一个 subtask 实例中），keyBy 后对数据进行 map 转换，然后使用 Sink 将数据输出到外部存储。

<img src="http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-07-152946.jpg" alt="简单的Flink流任务执行图.png" align='center' style="zoom: 67%;" />

众所周知，在大数据处理中，无论是批处理还是流处理，单点处理的性能总是有限的，我们的单个 Job 一般会运行在多个节点上，通过多个节点共同配合来提升整个系统的处理性能。图中，任务被切分成 4 个可独立执行的 subtask 分别是 A0、A1、B0、B1，在数据处理过程中就会存在 shuffle。例如，subtask A0 处理完的数据经过 keyBy 后被发送到 subtask B0、B1 所在节点去处理。那么问题来了，subtask A0 应该以多快的速度向 subtask B0、B1 发送数据呢？把上述问题抽象化，如下图所示，将 subtask A0 当作 Producer，subtask B0 当做 Consumer，上游 Producer 向下游 Consumer 发送数据，在发送端和接收端有相应的 Send Buffer 和 Receive Buffer，但是上游 Producer 生产数据的速率比下游 Consumer 消费数据的速率大，Producer 生产数据的速率为 2MB/s， Consumer 消费数据速率为 1MB/s，Receive Buffer 容量只有 5MB，所以过了 5 秒后，接收端的 Receive Buffer 满了。

<img src="http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-12-004946.png" alt="网络流控存在的问题.png" style="zoom:13%;" />

下游消费速率慢，且接收区的 Receive Buffer 有限，如果上游一直有源源不断的数据，那么将会面临着以下两种情况：

+ 下游消费者的缓冲区放不下数据，导致下游消费者会丢弃新到达的数据
+ 为了不丢弃数据，所以下游消费者的 Receive Buffer 持续扩张，最后耗尽消费者的内存，导致 OOM 程序挂掉

常识告诉我们，这两种情况在生产环境下都是不能接受的，第一种会丢数据、第二种会把应用程序挂掉。所以，该问题的解决方案不应该是下游 Receive Buffer 一直累积数据，而是上游 Producer 发现下游 Consumer 消费比较慢的时候，应该在 Producer 端做出限流的策略，防止在下游 Consumer 端无限制地堆积数据。那上游 Producer 端该如何做限流呢？可以采用如下图所示的静态限流策略：

<img src="http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-12-005000.png" alt="网络流控-静态限速.png" style="zoom:13%;" />

静态限速的思想就是，提前已知下游 Consumer 端的消费速率，然后在上游 Producer 端使用类似令牌桶的思想，限制 Producer 端生产数据的速率，从而控制上游 Producer 端向下游 Consumer 端发送数据的速率。但是静态限速会存在问题：

+ 通常无法事先预估下游 Consumer 端能承受的最大速率
+ 就算通过某种方式预估出下游 Consumer 端能承受的最大速率，在运行过程中也可能会因为网络抖动、 CPU 共享竞争、内存紧张、IO阻塞等原因造成下游 Consumer 的吞吐量降低，但是上游 Producer 的吞吐量正常，然后又会出现之前所说的下游接收区的 Receive Buffer 有限，上游一直有源源不断的数据发送到下游的问题，还是会造成下游要么丢数据，要么为了不丢数据 buffer 不断扩充导致下游 OOM 的问题

综上所述，我们发现了，上游 Producer 端必须有一个限流的策略，且静态限流是不可靠的，于是就需要一个动态限流的策略。可以采用如下图所示的动态反馈策略：

<img src="http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-12-005009.png" alt="网络流控-动态限速.png" style="zoom:13%;" />

下游 Consumer 端会频繁地向上游 Producer 端进行动态反馈，告诉 Producer 下游 Consumer 的负载能力，从而使 Producer 端可以动态调整向下游 Consumer 发送数据的速率，以实现 Producer 端的动态限流。当 Consumer 端处理较慢时，Consumer 将负载反馈到 Producer 端，Producer 端会根据反馈适当降低 Producer 自身从上游或者 Source 端读数据的速率来降低向下游 Consumer 发送数据的速率。当 Consumer 处理负载能力提升后，又及时向 Producer 端反馈，Producer 会通过提升自身从上游或 Source 端读数据的速率来提升向下游发送数据的速率，通过动态反馈的策略来动态调整系统整体的吞吐量。

读到这里，应该知道 Flink 为什么需要网络流控机制了，并且知道 Flink 的网络流控机制必须是一个动态反馈的策略。但是还有以下几个问题：

+ Flink 中数据具体是怎么从上游  Producer 端发送到下游 Consumer 端的？
+ Flink 的动态限流具体是怎么实现的？下游的负载能力和压力是如何传递给上游的？

带着这两个问题，学习下面的 Flink 网络流控与反压机制。


### 9.1.2 Flink 1.5 之前网络流控机制介绍

在 Flink 1.5 之前，Flink 没有使用任何复杂的机制来解决反压问题，因为根本不需要那样的方案！Flink 利用自身作为纯数据流引擎的优势来优雅地响应反压问题。下面我们会深入分析 Flink 是如何在 Task 之间传输数据的，以及数据流如何实现自然降速的。

如下图所示，Job 分为 Task A、B、C，Task A 是 Source Task、Task B 处理转换数据、Task C 是 Sink Task。Task A 从外部 Source 端读取到数据后将数据序列化放到 Send Buffer 中，再由 Task A 的 Send Buffer 发送到 Task B 的 Receive Buffer，Task B 的算子从 Task B 的 Receive Buffer 中将数据反序列后进行处理，将处理后数据序列化放到 Task B 的 Send Buffer 中，再由 Task B 的 Send Buffer 发送到 Task C 的 Receive Buffer，Task C 再从 Task C 的 Receive Buffer 中将数据反序列后输出到外部 Sink 端，这就是所有数据的传输和处理流程。

<img src="http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-12-005021.png" alt="简单的3个Task数据传输示意图.png" style="zoom:12%;" />

Flink 中，动态反馈策略原理比较简单，假如 Task C 由于各种原因吞吐量急剧降低，那么肯定会造成 Task C 的 Receive Buffer 中堆积大量数据，此时 Task B 还在给 Task C 发送数据，但是毕竟内存是有限的，持续一段时间后 Task C 的 Receive Buffer 满了，此时 Task B 发现 Task C 的 Receive Buffer 满了后，就不会再往 Task C 发送数据了，Task B 处理完的数据就开始往 Task B 的 Send Buffer 积压，一段时间后 Task B 的 Send Buffer 也满了，Task B 的处理就会被阻塞，这时 Task A 还在往 Task B 的 Receive Buffer 发送数据。同样的道理，Task B 的 Receive Buffer 很快满了，导致 Task A 不再往 Task B 发送数据，Task A 的 Send Buffer 也会被用完，Task A 是 Source Task 没有上游，所以 Task A 直接降低从外部 Source 端读取数据的速率甚至完全停止读取数据。通过以上原理，Flink 将下游的压力传递给上游。如果下游 Task C 的负载能力恢复后，如何将负载提升的信息反馈给上游呢？实际上 Task B 会一直向 Task C 发送探测信号，检测 Task C 的 Receive Buffer 是否有足够的空间，当 Task C 的负载能力恢复后，Task C 会优先消费 Task C Receive Buffer 中的数据，Task C Receive Buffer 中有足够的空间时，Task B 会从 Send Buffer 继续发送数据到 Task C 的 Receive Buffer，Task B 的 Send Buffer 有足够空间后，Task B 又开始正常处理数据，很快 Task B 的 Receive Buffer 中也会有足够空间，同理，Task A 会从 Send Buffer 继续发送数据到 Task B 的 Receive Buffer，Task A 的 Receive Buffer 有足够空间后，Task A 就可以从外部的 Source 端开始正常读取数据了。通过以上原理，Flink 将下游负载过低的消息传递给上游。所以说 Flink 利用自身纯数据流引擎的优势优雅地响应反压问题，并没有任何复杂的机制来解决反压。上述流程，就是 Flink 动态限流（反压机制）的简单描述，可以看到 Flink 的反压是从下游往上游传播的，一直往上传播到 Source Task 后，Source Task 最终会降低或提升从外部 Source 端读取数据的速率。

如下图所示，对于一个 Flink 任务，动态反馈要考虑如下两种情况：

<img src="http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-07-153007.jpg" alt="简单的3个Task反压图示.png" style="zoom:12%;" />

1.跨 Task，动态反馈具体如何从下游 Task 的 Receive Buffer 反馈给上游 Task 的 Send Buffer

- 当下游 Task C 的 Receive Buffer 满了，如何告诉上游 Task B 应该降低数据发送速率
- 当下游 Task C 的 Receive Buffer 空了，如何告诉上游 Task B 应该提升数据发送速率


> 注：这里又分了两种情况，Task B 和 Task C 可能在同一个 TaskManager 上运行，也有可能不在同一个 TaskManager 上运行
> 1. Task B 和 Task C 在同一个 TaskManager 运行指的是：一个 TaskManager 包含了多个 Slot，Task B 和 Task C 都运行在这个 TaskManager 上。此时 Task B 给 Task C 发送数据实际上是同一个 JVM 内的数据发送，所以**不存在网络通信**
> 2. Task B 和 Task C 不在同一个 TaskManager 运行指的是：Task B 和 Task C 运行在不同的 TaskManager 中。此时 Task B 给 Task C 发送数据是跨节点的，所以**会存在网络通信**

2.Task 内，动态反馈如何从内部的 Send Buffer 反馈给内部的 Receive Buffer

- 当 Task B 的 Send Buffer 满了，如何告诉 Task B 内部的 Receive Buffer，自身的 Send Buffer 已经满了？要让 Task B 的 Receive Buffer 感受到压力，才能把下游的压力传递到 Task A
- 当 Task B 的 Send Buffer 空了，如何告诉 Task B 内部的 Receive Buffer 下游 Send Buffer 空了，并把下游负载很低的消息传递给 Task A

到目前为止，动态反馈的具体细节抽象成了三个问题：

- 跨 Task 且 Task 不在同一个 TaskManager 内，动态反馈具体如何从下游 Task 的 Receive Buffer 反馈给上游 Task 的 Send Buffer
- 跨 Task 且 Task 在同一个 TaskManager 内，动态反馈具体如何从下游 Task 的 Receive Buffer 反馈给上游 Task 的 Send Buffer
- Task 内，动态反馈具体如何从 Task 内部的 Send Buffer 反馈给内部的 Receive Buffer

#### TaskManager 之间网络传输相关组件

TaskManager 之间数据传输流向如下图所示，可以看到 Source Task 给 Task B 发送数据，Source Task 做为 Producer，Task B 做为 Consumer，Producer 端产生的数据最后通过网络发送给 Consumer 端。Producer 端 Operator 实例对一条条的数据进行处理，处理完的数据首先缓存到 ResultPartition 内的 ResultSubPartition 中。ResultSubPartition 中一个 Buffer 写满或者超时后，就会触发将 ResultSubPartition 中的数据拷贝到 Producer 端 Netty 的 Buffer 中，之后又把数据拷贝到 Socket 的 Send Buffer 中，这里有一个从用户态拷贝到内核态的过程，最后通过 Socket 发送网络请求，把 Send Buffer 中的数据发送到 Consumer 端的 Receive Buffer。数据到达 Consumer 端后，再依次从 Socket 的 Receive Buffer 拷贝到 Netty 的 Buffer，再拷贝到 Consumer Operator InputGate 内的 InputChannel 中，最后 Consumer Operator 就可以读到数据进行处理了。这就是两个 TaskManager 之间的数据传输过程，我们可以看到发送方和接收方各有三层的 Buffer。当 Task B 往下游发送数据时，整个流程与 Source Task 给 Task B 发送数据的流程类似。

<img src="http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-12-005036.png" alt="TaskManager 之间数据传输流向.png" style="zoom:12%;" />

根据上述流程，下表中对 Flink 通信相关的一些术语进行介绍：

| 概念/术语          | 解释                                                         |
| :----------------- | ------------------------------------------------------------ |
| ResultPartition    | 生产者生产的数据首先写入到 ResultPartition 中，一个 Operator 实例对应一个ResultPartition。 |
| ResultSubpartition | 一个 ResultPartition 是由多个 ResultSubpartition 组成。当 Producer Operator 实例生产的数据要发送给下游 Consumer Operator n 个实例时，那么该 Producer Operator 实例对应的 ResultPartition 中就包含 n 个 ResultSubpartition。 |
| InputGate          | 消费者消费的数据来自于 InputGate 中，一个 Operator 实例对应一个InputGate。网络中传输的数据会写入到 Task 的 InputGate。 |
| InputChannel       | 一个 InputGate 是由多个 InputChannel 组成。当 Consumer Operator 实例读取的数据来自于上游 Producer Operator n 个实例时，那么该 Consumer Operator 实例对应的 InputGate 中就包含 n 个 InputChannel。 |
| RecordReader       | 用于将记录从Buffer中读出。                                   |
| RecordWriter       | 用于将记录写入Buffer。                                       |
| LocalBufferPool    | 为 ResultPartition 或 InputGate 分配内存，每一个 ResultPartition 或 InputGate分别对应一个 LocalBufferPool。 |
| NetworkBufferPool  | 为 LocalBufferPool 分配内存，NetworkBufferPool 是 Task 之间共享的，每个 TaskManager 只会实例化一个。 |

InputGate 和 ResultPartition 的内存是如何申请的呢？如下图所示，了解一下 Flink 网络传输相关的内存管理。在 TaskManager 初始化时，Flink 会在 NetworkBufferPool 中生成一定数量的内存块 MemorySegment，内存块的总数量就代表了网络传输中所有可用的内存。 NetworkBufferPool 是 Task 之间共享的，每个 TaskManager 只会实例化一个。Task 线程启动时，会为 Task 的 InputChannel 和 ResultSubPartition 分别创建一个 LocalBufferPool。InputGate 或 ResultPartition 需要写入数据时，会向相对应的 LocalBufferPool 申请内存（图中①），当 LocalBufferPool 没有足够的内存且还没到达 LocalBufferPool 设置的上限时，就会向 NetworkBufferPool 申请内存（图中②），并将内存分配给相应的 InputChannel 或 ResultSubPartition （图③④）。虽然可以申请，但是必须明白内存申请肯定是有限制的，不可能无限制的申请，我们在启动任务时可以指定该任务最多可能申请多大的内存空间用于 NetworkBufferPool。当 InputChannel 的内存块被 Operator 读取消费掉或 ResultSubPartition 的内存块已经被写入到了 Netty 中，那么 InputChannel 和 ResultSubPartition 中的内存块就可以还给 LocalBufferPool 了（图中⑤），如果 LocalBufferPool 中有较多空闲的内存块，就会还给 NetworkBufferPool （图中⑥）。

<img src="http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-12-005044.png" alt="Flink 网络传输相关的内存管理.png" style="zoom:12%;" />

了解了 Flink 网络传输相关的内存管理，我们来分析 3 种动态反馈的具体细节。

#### 跨 Task 且 Task 不在同一个 TaskManager 内时，反压如何向上游传播


#### 跨 Task 且 Task 在同一个 TaskManager 内，反压如何向上游传播

#### Task 内部，反压如何向上游传播


### 9.1.3 基于 Credit 的反压机制

#### Flink 1.5 之前反压机制存在的问题


#### 基于 Credit 的反压机制原理


### 9.1.4 Flink 如何定位产生反压的位置？


#### Flink 反压监控原理介绍


#### 利用 Flink Web UI 定位产生反压的位置

#### 利用 Flink Metrics 定位产生反压的位置



### 9.1.5 定位到反压来源后，该如何处理？

#### 系统资源

#### 垃圾收集（GC）

#### CPU/线程瓶颈

#### 线程竞争

#### 负载不平衡

#### 外部依赖


### 9.1.6 小结与反思


加入知识星球可以看到上面文章：https://t.zsxq.com/f66iAMz

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-09-25-zsxq.jpg)


