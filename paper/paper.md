这是一份流处理系统相关 paper，看到  [@lw-lin](https://github.com/lw-lin) 维护的，比较好，可以加深对流处理引擎的认识。
原仓库地址：[https://github.com/lw-lin/streaming-readings](https://github.com/lw-lin/streaming-readings)

如果你认为还有比较好的论文应当加入到这个列表中，请提交一个 pull request，谢谢！


# Readings in Streaming Systems


这是一份 streaming systems 领域相关的论文列表 20+ 篇，涉及 streaming systems 的设计，实现，故障恢复，弹性扩展等各方面。也包含自 2014 年以来 streaming system 和 batch system 的统一模型的论文。


## 2016 年


- [Drizzle: Fast and Adaptable Stream Processing at Scale](http://shivaram.org/drafts/drizzle.pdf) (Draft): Record-at-a-time 的系统，如 Naiad, Flink，处理延迟较低、但恢复延迟较高；micro-batch 系统，如 Spark Streaming，恢复延迟低但处理延迟略高。Drizzle 则采用 group scheduling + pre-scheduling shuffles 的方式对 Spark Streaming 做了改进，保留低恢复延迟的同时，降低了处理延迟至 100ms 量级。


- [Realtime Data Processing at Facebook](https://research.fb.com/wp-content/uploads/2016/11/realtime_data_processing_at_facebook.pdf) (SIGMOD): Facebook 明确自己实时的使用场景是 seconds of latency, not milliseconds，并基于自己的需求构建了 3 个实时处理组件：Puma, Swift, 以及 Stylus。Puma, Swift 和 Stylus 都从 Scribe 读数据，并可向 Scribe 写回数据（Scribe 是 Facebook 内部的分布式消息系统，类似 Kafka）。


## 2015 年


- [The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing](http://people.csail.mit.edu/matei/courses/2015/6.S897/readings/google-dataflow.pdf) (VLDB): 来自 Google 的将 stream processing 模型和 batch processing 模型统一的尝试。在 Dataflow model 下，底层依赖 FlumeJava 支持 batch processing，依赖 MillWheel 支持 stream processing。Dataflow model 的开源实现是 Apache Beam 项目。


- [Apache Flink: Stream and Batch Processing in a Single Engine](https://www.researchgate.net/publication/308993790_Apache_Flink_Stream_and_Batch_Processing_in_a_Single_Engine): Apache Flink 是一个处理 streaming data 和 batch data 的开源系统。Flink 的设计哲学是，包括实时分析 (real-time analytics)、持续数据处理 (continuous data pipelines)、历史数据处理 (historic data processing / batch)、迭代式算法 (iterative algorithms - machine learning, graph analysis) 等的很多类数据处理应用，都能用 pipelined fault-tolerant 的 dataflows 执行模型来表达。


- [Lightweight asynchronous snapshots for distributed dataflows](https://arxiv.org/pdf/1506.08603.pdf): Apache Flink 所实现的一个轻量级的、异步做状态快照的方法。基于此，Flink 得以保证分布式状态的一致性，从而保证整个系统的 exactly-once 语义。具体的，Flink 会持续性的在 stream 里插入 barrier markers，制造一个分布式的顺序关系，使得不同的节点能够在同一批 barrier marker 上达成整个系统的一致性状态。


- [Twitter Heron: Stream Processing at Scale](https://pdfs.semanticscholar.org/e847/c3ec130da57328db79a7fea794b07dbccdd9.pdf) (SIGMOD): Heron 是 Twitter 开发的用于代替 Storm 的实时处理系统，解决了 Storm 在扩展性、调试能力、性能、管理方式上的一些问题。Heron 实现了 Storm 的接口，因此对 Storm 有很好的兼容性，也成为了 Twitter 内部实时处理系统的事实上的标准。


## 2014 年


- [Trill: A High-Performance Incremental Query Processor for Diverse Analytics](http://www.vldb.org/pvldb/vol8/p401-chandramouli.pdf) (VLDB): 此篇介绍了 Microsoft 的 Trill - 一个新的分析查询处理器。Trill 很好的结合以下 3 方面需求：(1) *Query Model*: Trill 是基于时间-关系 (tempo-relational) 模型，所以很好的支持从实时到离线计算的延迟需求；(2) *Fabric and Language Integration*: Trill 作为一个类库，可以很好的与高级语言、已有类库结合；以及 (3) *Performance*: 无论实时还是离线，Trill 的 throughput 都很高 —— 实时计算比流处理引擎高 2-4 个数量级，离线计算与商业的列式 DBMS 同等。从实现角度讲，包括 punctuation 的使用来分 batch 满足 latency 需求，batch 内使用列式存储、code-gen 等技术来提高 performance，都具有很好的借鉴意义 —— 尤其注意这是 2014 年发表的论文。


- [Summingbird: A Framework for Integrating Batch and Online MapReduce Computations](http://www.vldb.org/pvldb/vol7/p1441-boykin.pdf) (VLDB): Twitter 开发的目标是将 online Storm 计算和 batch MapReduce 计算逻辑统一描述的一套 domain-specific language。Summingbird 抽象了 sources, sinks, 以及 stores 等，基于此抽象，上层应用就不必为 streaming 和 batch 维护两套计算逻辑，而可以使用同一套计算逻辑，只在运行时分别编译后跑在 streaming 的 Storm 上和 batch 的 MapReduce 上。


- [Storm@Twitter](http://db.cs.berkeley.edu/cs286/papers/storm-sigmod2014.pdf) (SIGMOD): 这是一篇来迟的论文。Apache Storm 最初在 Backtype 及 Twitter，而后在业界范围都有广泛的应用，甚至曾经一度也是事实上的流处理系统标准。此篇介绍了 Storm 的设计，及在 Twitter 内部的应用情况。当然后面我们知道 Apache Storm 也暴露出一些问题，业界也出现了一些更优秀的流处理系统。Twitter 虽没有在 2012 年 Storm 时代开启时发声，但在 2014 年 Storm 落幕时以此文发声向其致敬，也算是弥补了些许遗憾吧。


## 2013 年


- [Discretized Streams: Fault-Tolerant Streaming Computation at Scale](https://people.csail.mit.edu/matei/papers/2013/sosp_spark_streaming.pdf) (SOSP): Spark Streaming 是基于 Spark 执行引擎、micro-batch 模式的准实时处理系统。对比 RDD 是 Spark 引擎的数据抽象，DStream (Discretized Stream) 则是 Spark Streaming 引擎的数据抽象。DStream 像 RDD 一样，具有分布式、可故障恢复的特点，并且能够充分利用 Spark 引擎的推测执行，应对 straggler 的出现。


- [MillWheel: Fault-Tolerant Stream Processing at Internet Scale](https://static.googleusercontent.com/media/research.google.com/zh-CN//pubs/archive/41378.pdf) (VLDB): MillWheel 是 Google 内部研发的实时流数据处理系统，具有分布式、低延迟、高可用、支持 exactly-once 语义的特点。不出意外，MillWheel 是 Google 强大 infra structure 和强大 engeering 能力的综合体现 —— 利用 Bigtable/Spanner 作为后备状态存储、保证 exactly-once 特性等等。另外，MillWheel 将 watermark 机制发扬光大，对 event time 有着非常好的支持。推荐对 streaming system 感兴趣的朋友一定多读几遍此篇论文 —— 虽然此篇已经发表了几年，但工业界开源的系统尚未完全达到 MillWheel 的水平。


- [Integrating Scale Out and Fault Tolerance in Stream Processing using Operator State Management](http://openaccess.city.ac.uk/8175/1/sigmod13-seep.pdf) (SIGMOD): 针对有状态的算子的状态，此篇的基本洞察是，scale out 和 fault tolerance 其实很相通，应该结合到一起考虑和实现，而不是将其割裂开来。文章提出了算子的 3 类状态：(a) processing state, (b) buffer state, 和 (c) routing state，并提出了算子状态的 4 个操作原语：(1) checkpoint state, (2) backup state, (3) restore state, (4) partition state。


## 2010 年


- [S4: Distributed Stream Computing Platform](https://pdfs.semanticscholar.org/53a8/7ccd0ecbad81949c688c2240f2c0c321cdb1.pdf) (ICDMW): 2010 年算是 general stream processing engine 元年 —— Yahoo! 研发并发布了 S4, Backtype 开始研发了 Storm 并将在 1 年后（由 Twitter）将其开源。S4 和 Storm 都是 general-purpose 的 stream processing engine，允许用户通过代码自定义计算逻辑，而不是仅仅是使用声明式的语言或算子。


## 2008 年


- [Out-of-Order Processing: A New Architecture for HighPerformance Stream System](https://www.researchgate.net/publication/220538528_Out-of-Order_Processing_a_New_Architecture_for_High-Performance_Stream_Systems) (VLDB): 这篇文章提出了一种新的处理模型，即 out-of-order processing (OOP)，取消了以往 streaming system 里对事件有序的假设。重要的是，这篇文章提出了并实现了 low watermark: lwm(n, S, A) is the smallest value for A that occurs after prefix Sn of stream S。我们看到，在 2 年后 Google 开始研发的 MillWheel 里，watermark 将被发扬光大。


- [Fast and Highly-Available Stream Processing over Wide Area Networks](http://cs.brown.edu/~ugur/fast.pdf) (ICDE): 针对广域网 (wide area networks) 的 stream processing 设计的快速、高可用方案。主要思想是依靠 replication。


## 2007 年


- [A Cooperative, Self-Configuring High-Availability Solution for Stream Processing](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.89.2978&rep=rep1&type=pdf) (ICDE): 与 2005 年 ICDE 的文章一样，此篇也讨论 stream processing 的高可用问题。与 2005 年文章做法不同的是，此篇的 checkpointing 方法更细粒度一些，所以一个节点上的不同状态能够备份到不同的节点上去，因而在恢复的时候能够并行恢复以提高速度。


## 2005 年


- [The 8 Requirements of Real-Time Stream Processing](https://ix.cs.uoregon.edu/~jsventek/papers/02bStonebraker2005.pdf) (SIGMOD): 图领奖得主 Michael Stonebraker 老爷子与他在 StreamBase 的小伙伴们勾画的 stream processing applications 应当满足的 8 条规则，如 Rule 1: Keep the Data Moving, Rule 2: Query using SQL on Streams (StreamSQL), Rule 3: Handle Stream Imperfections (Delayed, Missing and Out-of-Order Data) … 等等。虽然此篇有引导舆论的嫌疑 —— 不知是先有了这流 8 条、再有了 StreamBase，还是先有了 StreamBase、再有了这流 8 条 —— 但其内容还是有相当的借鉴意义。


- [The Design of the Borealis Stream Processing Engine](http://cs.brown.edu/research/aurora/cidr05.borealis.pdf) (CIDR): Borealis 是 Aurora 的分布式、更优化版本的续作。Borealis 提出并解决了 3 个新一代系统的基础问题：(1) dynamic revision of query results, (2) dynamic query modification, 以及 (3) flexible and highly-scalable optimization. 此篇讲解了 Borealis 的设计与实现 —— p.s. 下，Aurora 及续作 Borealis 的命名还真是非常讲究，是学院派的风格 :-D


- [High-availability algorithms for distributed stream processing](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.162.306&rep=rep1&type=pdf) (ICDE): 此篇主要聚焦在 streaming system 的高可用性，即故障恢复。文章提出了 3 种 recovery types: (a) precise, (b) gap, 和 (c) rollback，并通过 (1) passive standby, (2) upstream backup, (3) active standby 的方式进行 recover。可与 2007 年 ICDE 的文章对比阅读。


## 2004 年


- [STREAM: The Stanford Data Stream Management System](http://ilpubs.stanford.edu:8090/641/1/2004-20.pdf) (Technique Report): 这篇 technique report 定义了一种 Continuous Query Language (CQL)，讲解了 Query Plans 和 Execution，讨论了一些 Performance Issues。系统也注意到并讨论了 Adaptivity 和 Approximation 的问题。从这篇 technique report 可以看出，这时的流式计算，更多是传统 RDBMS 的思路，扩展到了处理实时流式数据；这大约也是 2010 以前的 stream processing 相关研究的缩影。


## 2002 年


- [Monitoring Streams – A New Class of Data Management Applications](http://www.vldb.org/conf/2002/S07P02.pdf) (VLDB): 大约在 2002 年前后，从实时数据监控（如监控 sensors 数据等）应用出发，大家已经开始区分传统的查询主动、数据被动 (Human-Active, DBMS-Passive) 模式和新兴的数据主动、查询被动 (DBMS-Active, Human-Passive) 模式的区别 —— 此篇即是其中的典型代表。此篇提出了新式的 DBMS 的 Aurora，描述了其基本系统模型、面向流式数据的操作算子集、 优化策略、及实时应用。


- [Exploiting Punctuation Semantics in Continuous Data Streams](http://www.whitworth.edu/academic/department/mathcomputerscience/faculty/tuckerpeter/pdf/117896_final.pdf) (TKDE): 此篇很早的注意到了一些传统的操作算子不能用于无尽的数据流入的场景，因为将导致无尽的状态（考虑 outer join），或者无尽的阻塞（考虑 count 或 max）等。此篇提出，如果在 stream 里加入一些特殊的 punctuation，来标识一段一段的数据，那么我们就可以把无限的 stream 划分为多个有限的数据集的集合，从而使得之前提到的算子变得可用。此篇的价值更多体现在给了 2008 年 watermark 相关的文章以基础，乃至集大成在了 2010 年 Google MillWheel 中。
