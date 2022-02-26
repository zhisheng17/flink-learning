---
toc: true
title: 《Flink 实战与性能优化》—— 使用 Flink CEP 处理复杂事件
date: 2021-07-29
tags:
- Flink
- 大数据
- 流式计算
---


## 6.2 使用 Flink CEP 处理复杂事件

6.1 节中介绍 Flink CEP 和其使用场景，本节将详细介绍 Flink CEP 的 API，教会大家如何去使用 Flink CEP。

<!--more-->

### 6.2.1 准备依赖

要开发 Flink CEP 应用程序，首先你得在项目的 `pom.xml` 中添加依赖。

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-cep_${scala.binary.version}</artifactId>
    <version>${flink.version}</version>
</dependency>
```

这个依赖有两种，一个是 Java 版本的，一个是 Scala 版本，你可以根据项目的开发语言自行选择。


### 6.2.2 Flink CEP 入门应用程序

准备好依赖后，我们开始第一个 Flink CEP 应用程序，这里我们只做一个简单的数据流匹配，当匹配成功后将匹配的两条数据打印出来。首先定义实体类 Event 如下：

```java
public class Event {
    private Integer id;
    private String name;
}
```

然后构造读取 Socket 数据流将数据进行转换成 Event，代码如下：

```java
SingleOutputStreamOperator<Event> eventDataStream = env.socketTextStream("127.0.0.1", 9200)
    .flatMap(new FlatMapFunction<String, Event>() {
        @Override
        public void flatMap(String s, Collector<Event> collector) throws Exception {
            if (StringUtil.isNotEmpty(s)) {
                String[] split = s.split(",");
                if (split.length == 2) {
                    collector.collect(new Event(Integer.valueOf(split[0]), split[1]));
                }
            }
        }
    });
```

接着就是定义 CEP 中的匹配规则了，下面的规则表示第一个事件的 id 为 42，紧接着的第二个事件 id 要大于 10，满足这样的连续两个事件才会将这两条数据进行打印出来。

```java
Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(
        new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) {
                log.info("start {}", event.getId());
                return event.getId() == 42;
            }
        }
).next("middle").where(
        new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) {
                log.info("middle {}", event.getId());
                return event.getId() >= 10;
            }
        }
);

CEP.pattern(eventDataStream, pattern).select(new PatternSelectFunction<Event, String>() {
    @Override
    public String select(Map<String, List<Event>> p) throws Exception {
        StringBuilder builder = new StringBuilder();
        log.info("p = {}", p);
        builder.append(p.get("start").get(0).getId()).append(",").append(p.get("start").get(0).getName()).append("\n")
                .append(p.get("middle").get(0).getId()).append(",").append(p.get("middle").get(0).getName());
        return builder.toString();
    }
}).print();//打印结果
```

然后笔者在终端开启 Socket，输入的两条数据如下：

```
42,zhisheng
20,zhisheng
```

作业打印出来的日志如下图所示：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-29-072247.png)

整个作业 print 出来的结果如下图所示：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-29-072320.png)

好了，一个完整的 Flink CEP 应用程序如上，相信你也能大概理解上面的代码，接着来详细的讲解一下 Flink CEP 中的 Pattern API。


### 6.2.3 Pattern API

你可以通过 Pattern API 去定义从流数据中匹配事件的 Pattern，每个复杂 Pattern 都是由多个简单的 Pattern 组成的，拿前面入门的应用来讲，它就是由 `start` 和 `middle` 两个简单的 Pattern 组成的，在其每个 Pattern 中都只是简单的处理了流数据。在处理的过程中需要标示该 Pattern 的名称，以便后续可以使用该名称来获取匹配到的数据，如 `p.get("start").get(0)` 它就可以获取到 Pattern 中匹配的第一个事件。接下来我们先来看下简单的 Pattern 。

#### 单个 Pattern

**数量**


**条件**


#### 组合 Pattern

#### Group Pattern


#### 事件匹配跳过策略

### 6.2.4 检测 Pattern


#### 选择 Pattern


### 6.2.5 CEP 时间属性


#### 根据事件时间处理延迟数据



#### 时间上下文

加入知识星球可以看到上面文章：https://t.zsxq.com/nMR7ufq

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-09-25-zsxq.jpg)



### 6.2.6 小结与反思

本节开始通过一个 Flink CEP 案例教大家上手，后面通过讲解 Flink CEP 的 Pattern API，更多详细的还是得去看官网文档，其实也建议大家好好的跟着官网的文档过一遍所有的 API，并跟着敲一些样例来实现，这样在开发需求的时候才能够及时的想到什么场景下该使用哪种 API，接着教了大家如何将 Pattern 与数据流结合起来匹配并获取匹配的数据，最后讲了下 CEP 中的时间概念。

你公司有使用 Flink CEP 吗？通常使用哪些 API 居多？