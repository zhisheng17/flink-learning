---
toc: true
title: 《Flink 实战与性能优化》—— Flink 中 Processing Time、Event Time、Ingestion Time 对比及其使用场景分析
date: 2021-07-11
tags:
- Flink
- 大数据
- 流式计算
---





# 第三章 —— Flink 中的流计算处理

通过第二章的入门案例讲解，相信你已经知道了 Flink 程序的开发过程，本章将带你熟悉 Flink 中的各种特性，比如多种时间语义、丰富的窗口机制、流计算中常见的运算操作符、Watermark 机制、丰富的 Connectors（Kafka、ElasticSearch、Redis、HBase 等） 的使用方式。除了介绍这些知识点的原理之外，笔者还将通过案例来教会大家如何去实战使用，最后还会讲解这些原理的源码实现，希望你可以更深刻的理解这些特性。

## 3.1 Flink 多种时间语义对比

<!--more-->

Flink 在流应用程序中支持不同的 **Time** 概念，就比如有 Processing Time、Event Time 和 Ingestion Time。下面我们一起来看看这三个 Time。


### 3.1.1 Processing Time

Processing Time 是指事件被处理时机器的系统时间。

如果我们 Flink Job 设置的时间策略是 Processing Time 的话，那么后面所有基于时间的操作（如时间窗口）都将会使用当时机器的系统时间。每小时 Processing Time 窗口将包括在系统时钟指示整个小时之间到达特定操作的所有事件。

例如，如果应用程序在上午 9:15 开始运行，则第一个每小时 Processing Time 窗口将包括在上午 9:15 到上午 10:00 之间处理的事件，下一个窗口将包括在上午 10:00 到 11:00 之间处理的事件。

Processing Time 是最简单的 "Time" 概念，不需要流和机器之间的协调，它提供了最好的性能和最低的延迟。但是，在分布式和异步的环境下，Processing Time 不能提供确定性，因为它容易受到事件到达系统的速度（例如从消息队列）、事件在系统内操作流动的速度以及中断的影响。


### 3.1.2 Event Time

Event Time 是指事件发生的时间，一般就是数据本身携带的时间。这个时间通常是在事件到达 Flink 之前就确定的，并且可以从每个事件中获取到事件时间戳。在 Event Time 中，时间取决于数据，而跟其他没什么关系。Event Time 程序必须指定如何生成 Event Time 水印，这是表示 Event Time 进度的机制。

完美的说，无论事件什么时候到达或者其怎么排序，最后处理 Event Time 将产生完全一致和确定的结果。但是，除非事件按照已知顺序（事件产生的时间顺序）到达，否则处理 Event Time 时将会因为要等待一些无序事件而产生一些延迟。由于只能等待一段有限的时间，因此就难以保证处理 Event Time 将产生完全一致和确定的结果。

假设所有数据都已到达，Event Time 操作将按照预期运行，即使在处理无序事件、延迟事件、重新处理历史数据时也会产生正确且一致的结果。 例如，每小时事件时间窗口将包含带有落入该小时的事件时间戳的所有记录，不管它们到达的顺序如何（是否按照事件产生的时间）。


### 3.1.3 Ingestion Time

Ingestion Time 是事件进入 Flink 的时间。 在数据源操作处（进入 Flink source 时），每个事件将进入 Flink 时当时的时间作为时间戳，并且基于时间的操作（如时间窗口）会利用这个时间戳。

Ingestion Time 在概念上位于 Event Time 和 Processing Time 之间。 与 Processing Time 相比，成本可能会高一点，但结果更可预测。因为 Ingestion Time 使用稳定的时间戳（只在进入 Flink 的时候分配一次），所以对事件的不同窗口操作将使用相同的时间戳（第一次分配的时间戳），而在 Processing Time 中，每个窗口操作符可以将事件分配给不同的窗口（基于机器系统时间和到达延迟）。

与 Event Time 相比，Ingestion Time 程序无法处理任何无序事件或延迟数据，但程序中不必指定如何生成水印。

在 Flink 中，Ingestion Time 与 Event Time 非常相似，唯一区别就是 Ingestion Time 具有自动分配时间戳和自动生成水印功能。


### 3.1.4 三种 Time 的对比结果


### 3.1.5 使用场景分析


### 3.1.6 Time 策略设置


### 3.1.7 小结与反思



加入知识星球可以看到上面文章：https://t.zsxq.com/znqnMNB

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-09-25-zsxq.jpg)


