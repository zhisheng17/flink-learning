# Flink 学习

## 项目结构

```
├── README.md
├── flink-learning-cep
├── flink-learning-common
├── flink-learning-connectors
│   ├── flink-learning-connectors-cassandra
│   ├── flink-learning-connectors-es6
│   ├── flink-learning-connectors-flume
│   ├── flink-learning-connectors-hbase
│   ├── flink-learning-connectors-hdfs
│   ├── flink-learning-connectors-influxdb
│   ├── flink-learning-connectors-kafka
│   ├── flink-learning-connectors-mysql
│   ├── flink-learning-connectors-rabbitmq
│   ├── flink-learning-connectors-redis
│   ├── flink-learning-connectors-rocketmq
├── flink-learning-data-sinks
├── flink-learning-data-sources
├── flink-learning-template
```

## 博客

1、[《从0到1学习Flink》—— Apache Flink 介绍](http://www.54tianzhisheng.cn/2018/10/13/flink-introduction/)

2、[《从0到1学习Flink》—— Mac 上搭建 Flink 1.6.0 环境并构建运行简单程序入门](http://www.54tianzhisheng.cn/2018/09/18/flink-install)

3、[《从0到1学习Flink》—— Flink 配置文件详解](http://www.54tianzhisheng.cn/2018/10/27/flink-config/)

4、[《从0到1学习Flink》—— Data Source 介绍](http://www.54tianzhisheng.cn/2018/10/28/flink-sources/)

5、[《从0到1学习Flink》—— 如何自定义 Data Source ？](http://www.54tianzhisheng.cn/2018/10/30/flink-create-source/)

6、[《从0到1学习Flink》—— Data Sink 介绍](http://www.54tianzhisheng.cn/2018/10/29/flink-sink/)

7、[《从0到1学习Flink》—— 如何自定义 Data Sink ？](http://www.54tianzhisheng.cn/2018/10/31/flink-create-sink/)

8、[《从0到1学习Flink》—— Flink Data transformation(转换)](http://www.54tianzhisheng.cn/2018/11/04/Flink-Data-transformation/)

9、[《从0到1学习Flink》—— 介绍 Flink 中的 Stream Windows](http://www.54tianzhisheng.cn/2018/12/08/Flink-Stream-Windows/)

10、[《从0到1学习Flink》—— Flink 中的几种 Time 详解](http://www.54tianzhisheng.cn/2018/12/11/Flink-time/)

11、[《从0到1学习Flink》—— Flink 读取 Kafka 数据写入到 ElasticSearch](http://www.54tianzhisheng.cn/2018/12/30/Flink-ElasticSearch-Sink/)

12、[《从0到1学习Flink》—— Flink 项目如何运行？](http://www.54tianzhisheng.cn/2019/01/05/Flink-run/)

13、[《从0到1学习Flink》—— Flink 读取 Kafka 数据写入到 Kafka](http://www.54tianzhisheng.cn/2019/01/06/Flink-Kafka-sink/)

14、[《从0到1学习Flink》—— Flink JobManager 高可用性配置](http://www.54tianzhisheng.cn/2019/01/13/Flink-JobManager-High-availability/)

15、[《从0到1学习Flink》—— Flink parallelism 和 Slot 介绍](http://www.54tianzhisheng.cn/2019/01/14/Flink-parallelism-slot/)

16、[《从0到1学习Flink》—— Flink 读取 Kafka 数据批量写入到 MySQL](http://www.54tianzhisheng.cn/2019/01/15/Flink-MySQL-sink/)

17、[《从0到1学习Flink》—— Flink 读取 Kafka 数据写入到 RabbitMQ](http://www.54tianzhisheng.cn/2019/01/20/Flink-RabbitMQ-sink/)

18、[《从0到1学习Flink》—— Flink 读取 Kafka 数据写入到 HBase]()

19、[《从0到1学习Flink》—— Flink 读取 Kafka 数据写入到 HDFS]()

20、[《从0到1学习Flink》—— Flink 读取 Kafka 数据写入到 Redis]()

## 学习资料

另外我自己整理了些 Flink 的学习资料，目前已经全部放到微信公众号了。
你可以加我的微信：**zhisheng_tian**，然后回复关键字：**Flink** 即可无条件获取到，转载请联系本人获取授权，违者必究。

![](https://ws2.sinaimg.cn/large/006tNbRwly1fyh07imy15j30bq0bwq43.jpg)

更多私密资料请加入知识星球！

![](https://ws1.sinaimg.cn/large/006tKfTcly1g0c5a3l1cyj30u00gewg6.jpg)

有人要问知识星球里面更新什么内容？值得加入吗？

目前知识星球内已更新的系列文章：

1、[《从1到100深入学习Flink》—— 源码编译](http://www.54tianzhisheng.cn/)

2、[《从1到100深入学习Flink》—— 项目结构一览](http://www.54tianzhisheng.cn/)

3、[《从1到100深入学习Flink》—— local 模式启动流程](http://www.54tianzhisheng.cn/)

4、[《从1到100深入学习Flink》—— standalonesession 模式启动流程](http://www.54tianzhisheng.cn/)

5、[《从1到100深入学习Flink》—— Standalone Session Cluster 启动流程深度分析之 Job Manager 启动](http://www.54tianzhisheng.cn/)

6、[《从1到100深入学习Flink》—— Standalone Session Cluster 启动流程深度分析之 Task Manager 启动](http://www.54tianzhisheng.cn/)

除了《从1到100深入学习Flink》源码学习这个系列文章，《从0到1学习Flink》的案例文章也会优先在知识星球更新，让大家先通过一些 demo 学习 Flink，再去深入源码学习！

如果学习 Flink 的过程中，遇到什么问题，可以在里面提问，我会优先解答，这里做个抱歉，自己平时工作也挺忙，微信的问题不能做全部做一些解答，
但肯定会优先回复给知识星球的付费用户的，庆幸的是现在星球里的活跃氛围还是可以的，有不少问题通过提问和解答的方式沉淀了下来。

另外里面还会及时分享 Flink 的一些最新的资料（包括数据、视频、PPT、优秀博客，持续更新，保证全网最全，因为我知道 Flink 目前的资料还不多）

再就是星球用户给我提的一点要求：不定期分享一些自己遇到的 Flink 项目的实战，生产项目遇到的问题，是如何解决的等经验之谈！

当然，除了更新 Flink 相关的东西外，我还会更新一些大数据相关的东西，因为我个人之前不是大数据开发，所以现在也要狂补些知识！总之，希望进来的童鞋们一起共同进步！