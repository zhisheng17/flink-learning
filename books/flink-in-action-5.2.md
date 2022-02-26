---
toc: true
title: 《Flink 实战与性能优化》—— Flink Table API & SQL 功能
date: 2021-07-27
tags:
- Flink
- 大数据
- 流式计算
---


## 5.2 Flink Table API & SQL 功能

在 5.1 节中对 Flink Table API & SQL 的概述和常见 API 都做了介绍，这篇文章先来看下其与 DataStream 和 DataSet API 的集成。

<!--more-->


### 5.2.1 Flink Table 和 SQL 与 DataStream 和 DataSet 集成

两个 planner 都可以与 DataStream API 集成，只有以前的 planner 才可以集成 DataSet API，所以下面讨论 DataSet API 都是和以前的 planner 有关。

Table API & SQL 查询与 DataStream 和 DataSet 程序集成是非常简单的，比如可以通过 Table API 或者 SQL 查询外部表数据，进行一些预处理后，然后使用 DataStream 或 DataSet API 继续处理一些复杂的计算，另外也可以将 DataStream 或 DataSet 处理后的数据利用 Table API 或者 SQL 写入到外部表去。总而言之，它们之间互相转换或者集成比较容易。

#### Scala 的隐式转换

Scala Table API 提供了 DataSet、DataStream 和 Table 类的隐式转换，可以通过导入 `org.apache.flink.table.api.scala._` 或者 `org.apache.flink.api.scala._` 包来启用这些转换。

#### 将 DataStream 或 DataSet 注册为 Table

DataStream 或者 DataSet 可以注册为 Table，结果表的 schema 取决于已经注册的 DataStream 和 DataSet 的数据类型。你可以像下面这种方式转换：

```java
StreamTableEnvironment tableEnv = ...;

DataStream<Tuple2<Long, String>> stream = ...

//将 DataStream 注册为 myTable 表
tableEnv.registerDataStream("myTable", stream);

//将 DataStream 注册为 myTable2 表（表中的字段为 myLong、myString）
tableEnv.registerDataStream("myTable2", stream, "myLong, myString");
```

#### 将 DataStream 或 DataSet 转换为 Table

除了可以将 DataStream 或 DataSet 注册为 Table，还可以将它们转换为 Table，代码如下所示，转换之后再去使用 Table API 查询就比较方便了。

```java
StreamTableEnvironment tableEnv = ...;

DataStream<Tuple2<Long, String>> stream = ...

//将 DataStream 转换成 Table
Table table1 = tableEnv.fromDataStream(stream);

//将 DataStream 转换成 Table
Table table2 = tableEnv.fromDataStream(stream, "myLong, myString");
```

#### 将 Table 转换成 DataStream 或 DataSet

Table 可以转换为 DataStream 或 DataSet，这样就可以在 Table API 或 SQL 查询的结果上运行自定义的 DataStream 或 DataSet 程序。当将一个 Table 转换成 DataStream 或 DataSet 时，需要指定结果 DataStream 或 DataSet 的数据类型，最方便的数据类型是 Row，下面几个数据类型表示不同的功能：

+ Row：字段按位置映射，任意数量的字段，支持 null 值，没有类型安全访问。
+ POJO：字段按名称映射，POJO 属性必须按照 Table 中的属性来命名，任意数量的字段，支持 null 值，类型安全访问
+ Case Class：字段按位置映射，不支持 null 值，类型安全访问。
+ Tuple：按位置映射字段，限制为 22（Scala）或 25（Java）字段，不支持 null 值，类型安全访问。
+ 原子类型：Table 必须具有单个字段，不支持 null 值，类型安全访问。

##### 将 Table 转换成 DataStream

流查询的结果表会动态更新，即每个新的记录到达输入流时结果就会发生变化。所以在将 Table 转换成 DataStream 就需要对表的更新进行编码，有两种将 Table 转换为 DataStream 的模式：

+ 追加模式(Append Mode)：这种模式只能在动态表仅通过 INSERT 更改修改时才能使用，即仅追加，之前发出的结果不会更新。
+ 撤回模式(Retract Mode)：任何时刻都可以使用此模式，它使用一个 boolean 标志来编码 INSERT 和 DELETE 的更改。

两种模式的代码如下所示：

```java
StreamTableEnvironment tableEnv = ...;

//有两个字段(name、age) 的 Table
Table table = ...

//通过指定类，将表转换为一个 append DataStream
DataStream<Row> dsRow = tableEnv.toAppendStream(table, Row.class);

//将表转换为 Tuple2<String, Integer> 的 append DataStream
TupleTypeInfo<Tuple2<String, Integer>> tupleType = new TupleTypeInfo<>(Types.STRING(), Types.INT());
DataStream<Tuple2<String, Integer>> dsTuple = tableEnv.toAppendStream(table, tupleType);

//将表转换为一个 Retract DataStream Row
DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
```

##### 将 Table 转换成 DataSet

将 Table 转换成 DataSet 的样例如下：

```java
BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

//有两个字段(name、age) 的 Table
Table table = ...

//通过指定一个类将表转换为一个 Row DataSet
DataSet<Row> dsRow = tableEnv.toDataSet(table, Row.class);

//将表转换为 Tuple2<String, Integer> 的 DataSet
TupleTypeInfo<Tuple2<String, Integer>> tupleType = new TupleTypeInfo<>(Types.STRING(), Types.INT());
DataSet<Tuple2<String, Integer>> dsTuple = tableEnv.toDataSet(table, tupleType);
```


### 5.2.2 查询优化

Flink 使用 Calcite 来优化和翻译查询，以前的 planner 不会去优化 join 的顺序，而是按照查询中定义的顺序去执行。通过提供一个 CalciteConfig 对象来调整在不同阶段应用的优化规则集，这个可以通过调用 `CalciteConfig.createBuilder()` 获得的 builder 来创建，并且可以通过调用`tableEnv.getConfig.setCalciteConfig(calciteConfig)` 来提供给 TableEnvironment。而在 Blink planner 中扩展了 Calcite 来执行复杂的查询优化，这包括一系列基于规则和成本的优化，比如：

+ 基于 Calcite 的子查询去相关性
+ Project pruning
+ Partition pruning
+ Filter push-down
+ 删除子计划中的重复数据以避免重复计算
+ 重写特殊的子查询，包括两部分：
    - 将 IN 和 EXISTS 转换为 left semi-joins
    - 将 NOT IN 和 NOT EXISTS 转换为 left anti-join
+ 重排序可选的 join
    - 通过启用 table.optimizer.join-reorder-enabled

注意：IN/EXISTS/NOT IN/NOT EXISTS 目前只支持子查询重写中的连接条件。

#### 解释 Table

Table API 提供了一种机制来解释计算 Table 的逻辑和优化查询计划。你可以通过 `TableEnvironment.explain(table)` 或者 `TableEnvironment.explain()` 方法来完成。`explain(table)` 会返回给定计划的 Table，`explain()` 会返回多路 Sink 计划的结果（主要用于 Blink planner）。它返回一个描述三个计划的字符串：

+ 关系查询的抽象语法树，即未优化的逻辑查询计划
+ 优化的逻辑查询计划
+ 实际执行计划

以下代码演示了一个 Table 示例：

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

DataStream<Tuple2<Integer, String>> stream1 = env.fromElements(new Tuple2<>(1, "hello"));
DataStream<Tuple2<Integer, String>> stream2 = env.fromElements(new Tuple2<>(1, "hello"));

Table table1 = tEnv.fromDataStream(stream1, "count, word");
Table table2 = tEnv.fromDataStream(stream2, "count, word");
Table table = table1.where("LIKE(word, 'F%')").unionAll(table2);

System.out.println(tEnv.explain(table));
```

通过 `explain(table)` 方法返回的结果：

```
== Abstract Syntax Tree ==
LogicalUnion(all=[true])
  LogicalFilter(condition=[LIKE($1, _UTF-16LE'F%')])
    FlinkLogicalDataStreamScan(id=[1], fields=[count, word])
  FlinkLogicalDataStreamScan(id=[2], fields=[count, word])

== Optimized Logical Plan ==
DataStreamUnion(all=[true], union all=[count, word])
  DataStreamCalc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])
    DataStreamScan(id=[1], fields=[count, word])
  DataStreamScan(id=[2], fields=[count, word])

== Physical Execution Plan ==
Stage 1 : Data Source
	content : collect elements with CollectionInputFormat

Stage 2 : Data Source
	content : collect elements with CollectionInputFormat

	Stage 3 : Operator
		content : from: (count, word)
		ship_strategy : REBALANCE

		Stage 4 : Operator
			content : where: (LIKE(word, _UTF-16LE'F%')), select: (count, word)
			ship_strategy : FORWARD

			Stage 5 : Operator
				content : from: (count, word)
				ship_strategy : REBALANCE
```


### 5.2.3 数据类型


### 5.2.4 时间属性

#### Processing Time


#### Event time



### 5.2.5 SQL Connector

**使用代码**

**使用 YAML 文件**


**使用 DDL**


### 5.2.6 SQL Client



### 5.2.7 Hive

加入知识星球可以看到上面文章：https://t.zsxq.com/MNBEYvf

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-09-25-zsxq.jpg)





### 5.2.8 小结与反思

本章节继续介绍了 Flink Table API & SQL 中的部分 API，然后讲解了 Flink 之前的 planner 和 Blink planner 在某些特性上面的区别，还讲解了 SQL Connector，最后介绍了 SQL Client 和 Hive。


本章讲解了 Flink Table API & SQL 相关的概述，另外还介绍了它们的 API 使用方式，除此之外还对 Connectors、SQL Client、Hive 做了一定的讲解。