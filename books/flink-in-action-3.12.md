---
toc: true
title: 《Flink 实战与性能优化》—— 使用 Side Output 分流
date: 2021-07-23
tags:
- Flink
- 大数据
- 流式计算
---

## 3.12 使用 Side Output 分流

通常，在 Kafka 的 topic 中会有很多数据，这些数据虽然结构是一致的，但是类型可能不一致，举个例子：Kafka 中的监控数据有很多种：机器、容器、应用、中间件等，如果要对这些数据分别处理，就需要对这些数据流进行一个拆分，那么在 Flink 中该怎么完成这需求呢，有如下这些方法。

<!--more-->

### 3.12.1 使用 Filter 分流

使用 filter 算子根据数据的字段进行过滤分成机器、容器、应用、中间件等。伪代码如下：

```java
DataStreamSource<MetricEvent> data = KafkaConfigUtil.buildSource(env);  //从 Kafka 获取到所有的数据流
SingleOutputStreamOperator<MetricEvent> machineData = data.filter(m -> "machine".equals(m.getTags().get("type")));  //过滤出机器的数据
SingleOutputStreamOperator<MetricEvent> dockerData = data.filter(m -> "docker".equals(m.getTags().get("type")));    //过滤出容器的数据
SingleOutputStreamOperator<MetricEvent> applicationData = data.filter(m -> "application".equals(m.getTags().get("type")));  //过滤出应用的数据
SingleOutputStreamOperator<MetricEvent> middlewareData = data.filter(m -> "middleware".equals(m.getTags().get("type")));    //过滤出中间件的数据
```


### 3.12.2 使用 Split 分流

先在 split 算子里面定义 OutputSelector 的匿名内部构造类，然后重写 select 方法，根据数据的类型将不同的数据放到不同的 tag 里面，这样返回后的数据格式是 SplitStream，然后要使用这些数据的时候，可以通过 select 去选择对应的数据类型，伪代码如下：

```java
DataStreamSource<MetricEvent> data = KafkaConfigUtil.buildSource(env);  //从 Kafka 获取到所有的数据流
SplitStream<MetricEvent> splitData = data.split(new OutputSelector<MetricEvent>() {
    @Override
    public Iterable<String> select(MetricEvent metricEvent) {
        List<String> tags = new ArrayList<>();
        String type = metricEvent.getTags().get("type");
        switch (type) {
            case "machine":
                tags.add("machine");
                break;
            case "docker":
                tags.add("docker");
                break;
            case "application":
                tags.add("application");
                break;
            case "middleware":
                tags.add("middleware");
                break;
            default:
                break;
        }
        return tags;
    }
});

DataStream<MetricEvent> machine = splitData.select("machine");
DataStream<MetricEvent> docker = splitData.select("docker");
DataStream<MetricEvent> application = splitData.select("application");
DataStream<MetricEvent> middleware = splitData.select("middleware");
```

上面这种只分流一次是没有问题的，注意如果要使用它来做连续的分流，那是有问题的，笔者曾经就遇到过这个问题，当时记录了博客 —— [Flink 从0到1学习—— Flink 不可以连续 Split(分流)？](http://www.54tianzhisheng.cn/2019/06/12/flink-split/) ，当时排查这个问题还查到两个相关的 Flink Issue。

+ [FLINK-5031 Issue](https://issues.apache.org/jira/browse/FLINK-5031)

+ [FLINK-11084 Issue](https://issues.apache.org/jira/browse/FLINK-11084)

这两个 Issue 反映的就是连续 split 不起作用，在第二个 Issue 下面的评论就有回复说 Side Output 的功能比 split 更强大， split 会在后面的版本移除（其实在 1.7.x 版本就已经设置为过期），那么下面就来学习一下 Side Output。


### 3.12.3 使用 Side Output 分流

要使用 Side Output 的话，你首先需要做的是定义一个 OutputTag 来标识 Side Output，代表这个 Tag 是要收集哪种类型的数据，如果是要收集多种不一样类型的数据，那么你就需要定义多种 OutputTag。要完成本节前面的需求，需要定义 4 个 OutputTag，如下：

```java
//创建 output tag
private static final OutputTag<MetricEvent> machineTag = new OutputTag<MetricEvent>("machine") {
};
private static final OutputTag<MetricEvent> dockerTag = new OutputTag<MetricEvent>("docker") {
};
private static final OutputTag<MetricEvent> applicationTag = new OutputTag<MetricEvent>("application") {
};
private static final OutputTag<MetricEvent> middlewareTag = new OutputTag<MetricEvent>("middleware") {
};
```

定义好 OutputTag 后，可以使用下面几种函数来处理数据：

+ ProcessFunction
+ KeyedProcessFunction
+ CoProcessFunction
+ ProcessWindowFunction
+ ProcessAllWindowFunction

在利用上面的函数处理数据的过程中，需要对数据进行判断，将不同种类型的数据存到不同的 OutputTag 中去，如下代码所示：

```java
DataStreamSource<MetricEvent> data = KafkaConfigUtil.buildSource(env);  //从 Kafka 获取到所有的数据流
SingleOutputStreamOperator<MetricEvent> sideOutputData = data.process(new ProcessFunction<MetricEvent, MetricEvent>() {
    @Override
    public void processElement(MetricEvent metricEvent, Context context, Collector<MetricEvent> collector) throws Exception {
        String type = metricEvent.getTags().get("type");
        switch (type) {
            case "machine":
                context.output(machineTag, metricEvent);
            case "docker":
                context.output(dockerTag, metricEvent);
            case "application":
                context.output(applicationTag, metricEvent);
            case "middleware":
                context.output(middlewareTag, metricEvent);
            default:
                collector.collect(metricEvent);
        }
    }
});
```

好了，既然上面已经将不同类型的数据放到不同的 OutputTag 里面了，那么该如何去获取呢？可以使用 getSideOutput 方法来获取不同 OutputTag 的数据，比如：

```java
DataStream<MetricEvent> machine = sideOutputData.getSideOutput(machineTag);
DataStream<MetricEvent> docker = sideOutputData.getSideOutput(dockerTag);
DataStream<MetricEvent> application = sideOutputData.getSideOutput(applicationTag);
DataStream<MetricEvent> middleware = sideOutputData.getSideOutput(middlewareTag);
```

这样你就可以获取到 Side Output 数据了，其实在 3.4 和 3.5 节就讲了 Side Output 在 Flink 中的应用（处理窗口的延迟数据），大家如果没有印象了可以再返回去复习一下。


### 3.12.4 小结与反思

本节讲了下 Flink 中将数据分流的三种方式，完整代码的 GitHub 地址：[sideoutput](https://github.com/zhisheng17/flink-learning/tree/master/flink-learning-examples/src/main/java/com/zhisheng/examples/streaming/sideoutput)



本章全部在介绍 Flink 的技术点，比如多种时间语义的对比分析和应用场景分析、多种灵活的窗口的使用方式及其原理实现、平时开发使用较多的一些算子、深入讲解了 DataStream 中的流类型及其对应的方法实现、如何将 Watermark 与 Window 结合来处理延迟数据、Flink Connector 的使用方式。

因为 Flink 的 Connector 比较多，所以本书只挑选了些平时工作用的比较多的 Connector，比如 Kafka、ElasticSearch、HBase、Redis 等，并教会了大家如何去自定义 Source 和 Sink，这样就算 Flink 中的 Connector 没有你需要的，那么也可以通过这种自定义的方法来实现读取和写入数据。

本章讲解的内容更侧重于在 Flink DataStream 和 Connector 的使用技巧和优化，在讲解这些时还提供了详细的代码实现，目的除了大家可以参考外，其实更期望大家能够自己跟着多动手去实现和优化，这样才可以提高自己的编程水平。另外本章还介绍了自己在使用这些 Connector 时遇到的一些问题，是怎么解决的，也希望大家在工作的时候遇到问题可以静下来自己独立的思考下出现问题的原因是啥，该如何解决，多独立思考后，相信遇到问题后你也能够从容的分析和解决问题。


加入知识星球可以看到更多文章

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-09-25-zsxq.jpg)


