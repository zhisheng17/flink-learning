---
toc: true
title: 《Flink 实战与性能优化》—— 如何使用 Flink ProcessFunction 处理宕机告警?
date: 2021-08-15
tags:
- Flink
- 大数据
- 流式计算
---


## 11.2 如何使用 Flink ProcessFunction 处理宕机告警?

在 3.3 节中讲解了 Process 算子的概念，本节中将更详细的讲解 Flink ProcessFunction，然后教大家如何使用 ProcessFunction 来解决公司中常见的问题 —— 宕机，这个宕机不仅仅包括机器宕机，还包含应用宕机，通常出现宕机带来的影响是会很大的，所以能及时收到告警会减少损失。

<!--more-->


### 11.2.1 ProcessFunction 简介

在 1.2.5 节中讲了 Flink 的 API 分层，其中可以看见 Flink 的底层 API 就是 ProcessFunction，它是一个低阶的流处理操作，它可以访问流处理程序的基础构建模块：Event、State、Timer。ProcessFunction 可以被认为是一种提供了对 KeyedState 和定时器访问的 FlatMapFunction。每当数据源中接收到一个事件，就会调用来此函数来处理。对于容错的状态，ProcessFunction 可以通过 RuntimeContext 访问 KeyedState。

定时器可以对处理时间和事件时间的变化做一些处理。每次调用 processElement() 都可以获得一个 Context 对象，通过该对象可以访问元素的事件时间戳以及 TimerService。TimerService 可以为尚未发生的事件时间/处理时间实例注册回调。当定时器到达某个时刻时，会调用 onTimer() 方法。在调用期间，所有状态再次限定为定时器创建的 key，允许定时器操作 KeyedState。如果要访问 KeyedState 和定时器，那必须在 KeyedStream 上使用 KeyedProcessFunction，比如在 keyBy 算子之后使用：

```java
dataStream.keyBy(...).process(new KeyedProcessFunction<>(){
    
})
```

KeyedProcessFunction 是 ProcessFunction 函数的一个扩展，它可以在 onTimer 和 processElement 方法中获取到分区的 Key 值，这对于数据传递是很有帮助的，因为经常有这样的需求，经过 keyBy 算子之后可能还需要这个 key 字段，那么在这里直接构建成一个新的对象（新增一个 key 字段），然后下游的算子直接使用这个新对象中的 key 就好了，而不在需要重复的拼一个唯一的 key。

```java
public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
    System.out.println(ctx.getCurrentKey());
    out.collect(value);
}

@Override
public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
    System.out.println(ctx.getCurrentKey());
    super.onTimer(timestamp, ctx, out);
}
```


### 11.2.2 CoProcessFunction 简介

如果要在两个输入流上进行操作，可以使用 CoProcessFunction，这个函数可以传入两个不同的数据流输入，并为来自两个不同数据源的事件分别调用 processElement1() 和  processElement2() 方法。可以按照下面的步骤来实现一个典型的 Join 操作：

+ 为一个数据源的数据建立一个状态对象
+ 从数据源处有新数据流过来的时候更新这个状态对象
+ 在另一个数据源接收到元素时，关联状态对象并对其产生出连接的结果

比如，将监控的 metric 数据和告警规则数据进行一个连接，在流数据的状态中存储了告警规则数据，当有监控数据过来时，根据监控数据的 metric 名称和一些 tag 去找对应告警规则计算表达式，然后通过规则的表达式对数据进行加工处理，判断是否要告警，如果是要告警则会关联构造成一个新的对象，新对象中不仅有初始的监控 metric 数据，还有含有对应的告警规则数据以及通知策略数据，组装成这样一条数据后，下游就可以根据这个数据进行通知，通知还会在状态中存储这个告警状态，表示它在什么时间告过警了，下次有新数据过来的时候，判断新数据是否是恢复的，如果属于恢复则把该状态清除。


### 11.2.3 Timer 简介

Timer 提供了一种定时触发器的功能，通过 TimerService 接口注册 timer。TimerService 在内部维护两种类型的定时器（处理时间和事件时间定时器）并排队执行。处理时间定时器的触发依赖于 ProcessingTimeService，它负责管理所有基于处理时间的触发器，内部使用 ScheduledThreadPoolExecutor 调度定时任务；事件时间定时器的触发依赖于系统当前的 Watermark。需要注意的一点就是：**Timer 只能在 KeyedStream 中使用**。

TimerService 会删除每个 Key 和时间戳重复的定时器，即每个 Key 在同一个时间戳上最多有一个定时器。如果为同一时间戳注册了多个定时器，则只会调用一次 onTimer（） 方法。Flink 会同步调用 onTimer() 和  processElement() 方法，因此不必担心状态的并发修改问题。TimerService 不仅提供了注册和删除 Timer 的功能，还可以通过它来获取当前的系统时间和 Watermark 的值。TimerService 类中的方法如下图所示。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/2020-01-12-070737.png)

#### 容错

定时器具有容错能力，并且会与应用程序的状态一起进行 Checkpoint，如果发生故障重启会从 Checkpoint／Savepoint 中恢复定时器的状态。如果有处理时间定时器原本是要在恢复起来的那个时间之前触发的，那么在恢复的那一刻会立即触发该定时器。定时器始终是异步的进行 Checkpoint（除 RocksDB 状态后端存储、增量的 Checkpoint、基于堆的定时器外）。因为定时器实际上也是一种特殊状态的状态，在 Checkpoint 时会写入快照中，所以如果有大量的定时器，则无非会增加一次 Checkpoint 所需要的时间，必要的话得根据实际情况合并定时器。

#### 合并定时器

由于 Flink 仅为每个 Key 和时间戳维护一个定时器，因此可以通过降低定时器的频率来进行合并以减少定时器的数量。对于频率为 1 秒的定时器（基于事件时间或处理时间），可以将目标时间向下舍入为整秒数，则定时器最多提前 1 秒触发，但不会迟于我们的要求，精确到毫秒。因此，每个键每秒最多有一个定时器。

```java
long coalescedTime = ((ctx.timestamp() + timeout) / 1000) * 1000;
ctx.timerService().registerProcessingTimeTimer(coalescedTime);
```

由于事件时间计时器仅在 Watermark 到达时才触发，因此可以将当前 Watermark 与下一个 Watermark 的定时器一起调度和合并：

```java
long coalescedTime = ctx.timerService().currentWatermark() + 1;
ctx.timerService().registerEventTimeTimer(coalescedTime);
```

定时器也可以类似下面这样移除：

```java
//删除处理时间定时器
long timestampOfTimerToStop = ...
ctx.timerService().deleteProcessingTimeTimer(timestampOfTimerToStop);

//删除事件时间定时器
long timestampOfTimerToStop = ...
ctx.timerService().deleteEventTimeTimer(timestampOfTimerToStop);
```

如果没有该时间戳的定时器，则删除定时器无效。


### 11.2.4 如果利用 ProcessFunction 处理宕机告警？


#### 宕机告警需求分析


#### 宕机告警代码实现


### 11.2.5 小结与反思


加入知识星球可以看到上面文章：https://t.zsxq.com/RBYj66M

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-09-25-zsxq.jpg)





