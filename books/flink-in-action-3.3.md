---
toc: true
title: 《Flink 实战与性能优化》—— Flink 数据转换必须熟悉的算子（Operator)   
date: 2021-07-13
tags:
- Flink
- 大数据
- 流式计算
---

## 3.3 必须熟悉的数据转换 Operator(算子)

在 Flink 应用程序中，无论你的应用程序是批程序，还是流程序，都是上图这种模型，有数据源（source），有数据下游（sink），我们写的应用程序多是对数据源过来的数据做一系列操作，总结如下。

<!--more-->

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-12-14-141653.png)

1、**Source**: 数据源，Flink 在流处理和批处理上的 source 大概有 4 类：基于本地集合的 source、基于文件的 source、基于网络套接字的 source、自定义的 source。自定义的 source 常见的有 Apache kafka、Amazon Kinesis Streams、RabbitMQ、Twitter Streaming API、Apache NiFi 等，当然你也可以定义自己的 source。

2、**Transformation**: 数据转换的各种操作，有 Map / FlatMap / Filter / KeyBy / Reduce / Fold / Aggregations / Window / WindowAll / Union / Window join / Split / Select / Project 等，操作很多，可以将数据转换计算成你想要的数据。

3、**Sink**: 接收器，Sink 是指 Flink 将转换计算后的数据发送的地点 ，你可能需要存储下来。Flink 常见的 Sink 大概有如下几类：写入文件、打印出来、写入 Socket 、自定义的 Sink 。自定义的 sink 常见的有 Apache kafka、RabbitMQ、MySQL、ElasticSearch、Apache Cassandra、Hadoop FileSystem 等，同理你也可以定义自己的 Sink。

那么本文将给大家介绍的就是 Flink 中的批和流程序常用的算子（Operator）。


### 3.3.1 DataStream Operator

我们先来看看流程序中常用的算子。

#### Map

Map 算子的输入流是 DataStream，经过 Map 算子后返回的数据格式是 SingleOutputStreamOperator 类型，获取一个元素并生成一个元素，举个例子：

```java
SingleOutputStreamOperator<Employee> map = employeeStream.map(new MapFunction<Employee, Employee>() {
    @Override
    public Employee map(Employee employee) throws Exception {
        employee.salary = employee.salary + 5000;
        return employee;
    }
});
map.print();
```

新的一年给每个员工的工资加 5000。

#### FlatMap

FlatMap 算子的输入流是 DataStream，经过 FlatMap 算子后返回的数据格式是 SingleOutputStreamOperator 类型，获取一个元素并生成零个、一个或多个元素，举个例子：

```java
SingleOutputStreamOperator<Employee> flatMap = employeeStream.flatMap(new FlatMapFunction<Employee, Employee>() {
    @Override
    public void flatMap(Employee employee, Collector<Employee> out) throws Exception {
        if (employee.salary >= 40000) {
            out.collect(employee);
        }
    }
});
flatMap.print();
```

将工资大于 40000 的找出来。

#### Filter

#### KeyBy

#### Reduce

#### Aggregations

#### Window

#### WindowAll

#### Union

#### Window Join

#### Split

#### Select


### 3.3.2 DataSet Operator


#### First-n


### 3.3.3 流计算与批计算统一的思路



加入知识星球可以看到上面文章：https://t.zsxq.com/iYFMZFA

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-09-25-zsxq.jpg)




### 3.3.4 小结与反思

本节介绍了在开发 Flink 作业中数据转换常使用的算子（包含流作业和批作业），DataStream API 和 DataSet API 中部分算子名字是一致的，也有不同的地方，最后讲解了下 Flink 社区后面流批统一的思路。

你们公司使用 Flink 是流作业居多还是批作业居多？