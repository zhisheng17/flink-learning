---
toc: true
title: 《Flink 实战与性能优化》—— 如何利用广播变量动态更新告警规则？
date: 2021-08-17
tags:
- Flink
- 大数据
- 流式计算
---


## 11.4 如何利用广播变量动态更新告警规则？

一个在生产环境运行的流作业有时候会想变更一些作业的配置或者数据流的配置，然后作业可以读取并使用新的配置，而不是通过修改配置然后重启作业来读取配置，毕竟重启一个有状态的流作业代价挺大，本节将带你熟悉 Broadcast，并通过一个案例来教会你如何去动态的更新作业的配置。


<!--more-->


### 11.4.1 BroadcastVariable 简介

BroadcastVariable 中文意思是广播变量，其实可以理解是一个公共的共享变量（可能是固定不变的数据集合，也可能是动态变化的数据集合），在作业中将该共享变量广播出去，然后下游的所有任务都可以获取到该共享变量，这样就可以不用将这个变量拷贝到下游的每个任务中。之所以设计这个广播变量的原因主要是因为在 Flink 中多并行度的情况下，每个算子或者不同算子运行所在的 Slot 不一致，这就导致它们不会共享同一个内存，也就不可以通过静态变量的方式去获取这些共享变量值。对于这个问题，有不少读者在问过我为啥我设置的静态变量值在本地运行是可以获取到的，在集群环境运行作业就出现空指针啊，该问题其实笔者自己也在生产环境遇到过，所以接下来好好教大家使用！


### 11.4.2 如何使用 BroadcastVariable ？

在 3.4 节中讲过如何 broadcast 算子和 BroadcastStream 如何使用，在 4.1 节中讲解了 Broadcast State 如何使用以及需要注意的地方，注意 BroadcastVariable 只能应用在批作业中，如果要应用在流作业中则需要要使用 BroadcastStream。

在批作业中通过使用 `withBroadcastSet(DataSet, String)` 来广播一个 DataSet 数据集合，并可以给这份数据起个名字，如果要获取数据的时候，可以通过 `getRuntimeContext().getBroadcastVariable(String)` 获取广播出去的变量数据。下面演示一下广播一个 DataSet 变量和获取变量的样例。

```java
final ParameterTool params = ParameterTool.fromArgs(args);
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//1. 待广播的数据
DataSet<Integer> toBroadcast = env.fromElements(1, 2, 3);

env.fromElements("a", "b")
        .map(new RichMapFunction<String, String>() {
            List<Integer> broadcastData;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 3. 获取广播的 DataSet 数据 作为一个 Collection
                broadcastData = getRuntimeContext().getBroadcastVariable("zhisheng");
            }

            @Override
            public String map(String value) throws Exception {
                return broadcastData.get(1) + value;
            }
        }).withBroadcastSet(toBroadcast, "zhisheng")// 2. 广播 DataSet
        .print();
```

注意广播的时候设置的名称和获取的名称要一致，然后运行的结果如下图所示。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-17-013429.png)

流作业中通常使用 BroadcastStream 的方式将变量集合在数据流中传递，可能数据集合会做修改更新，但是修改后其实并不想重启作业去读取这些新修改的配置，因为对于一个流作业来说重启带来的代价很高（需要考虑数据堆积和如何恢复至重启前的状态等问题），那么这种情况下就可以在广播数据流处定时查询数据，这样就能够获取更改后的数据，通常在这种广播数据处获取数据只需要设置一个并行度就好，时间根据需求来判断及时性，一般 1 分钟内的数据变更延迟都是在容忍范围之内。广播流中的元素保证流所有的元素最终都会发到下游的所有并行实例，但是元素到达下游的并行实例的顺序可能不相同。因此，对广播状态的修改不能依赖于输入数据的顺序。在进行 Checkpoint 时，所有的任务都会 Checkpoint 下它们的广播状态。

另外需要注意的是：广播出去的变量存在于每个节点的内存中，所以这个数据集不能太大，因为广播出去的数据，会一致在内存中存在，除非程序执行结束。个人建议：如果数据集在几十兆或者百兆的时候，可以选择进行广播，如果数据集的大小上 G 的话，就不建议进行广播了。

上面介绍了下广播变量的在批作业的使用方式，下面通过一个案例来教大家如何在流作业中使用广播变量。


### 11.4.3 利用广播变量动态更新告警规则数据需求分析

在 11.3.3 节中有设计一张简单的告警规则表，通常告警规则是会对外提供接口进行增删改查的，那么随着业务应用上线，开发人员会对其应用服务新增或者修改告警规则（更改之前规则中的阈值），那么更改之后就需要让告警的作业能够去感知到之前的规则发生了变动，所以就需要在作业中想个什么办法去获取到更改后的数据。有两种方式可以让作业知道规则的变更： push 和 pull 模式。

push 模式则需要在更新、删除、新增接口中不仅操作数据库，还需要额外的发送更新、删除、新增规则的事件到消息队列中，然后作业消费消息队列的数据再去做更新、删除、新增规则，这种及时性有保证，但是可能会有数据不统一的风险（如果消息队列的数据丢了，但是在接口中还是将规则的数据变更存储到数据库）；pull 模式下就需要作业定时去查找一遍所有的告警规则数据，然后存在作业内存中，这个时间可以设置的比较短，比如 1 分钟，这样就能既保证数据的一致性，时间延迟也是在容忍范围之内。

对于这种动态变化的规则数据，在 Flink 中通常是使用广播流来处理的。那么接下来就演示下如何利用广播变量动态更新告警规则数据，假设我们在数据库中新增告警规则或者修改告警规则指标的阈值，然后看作业中是否会出现相应的变化。


### 11.4.4 读取告警规则数据



### 11.4.5 监控数据连接规则数据




### 11.4.6 小结与反思


加入知识星球可以看到上面文章：https://t.zsxq.com/RBYj66M

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-09-25-zsxq.jpg)





