---
toc: true
title: 《Flink 实战与性能优化》—— 如何统计网站各页面一天内的 PV 和 UV？
date: 2021-08-14
tags:
- Flink
- 大数据
- 流式计算
---

# 第十一章 —— Flink 实战

本章主要是 Flink 实战，介绍了一些常见的需求，比如实时统计网站页面的 PV/UV、宕机告警、动态更新配置、应用 Error 日志实时告警等，然后分别去分析这些需求的实现方式，明白该使用 Flink 中的哪些知识点才能够很好的完成这种需求，并提供完整的案例代码供大家参考。在实现完成这些需求之后，笔者还将会更深一步的去讲解下这些知识点背后的实现方式，希望可以加深你对这些知识点的印象，以便后面你可以灵活的处理类似的需求。


## 11.1 如何统计网站各页面一天内的 PV 和 UV？

大数据开发最常统计的需求可能就是 PV、UV。PV 全拼 PageView，即页面访问量，用户每次对网站的访问均被记录，按照访问量进行累计，假如用户对同一页面访问了 5 次，那该页面的 PV 就应该加 5。UV 全拼为 UniqueVisitor，即独立访问用户数，访问该页面的一台电脑客户端为一个访客，假如用户对同一页面访问了 5 次，那么该页面的 UV 只应该加 1，因为 UV 计算的是去重后的用户数而不是访问次数。当然如果是按天统计，那么当天 0 点到 24 点相同的客户端只被计算一次，如果过了今天 24 点，第二天该用户又访问了该页面，那么第二天该页面的 UV 应该加 1。 概念明白了那如何使用 Flink 来统计网站各页面的 PV 和 UV 呢？通过本节来详细描述。


<!--more-->


### 11.1.1 统计网站各页面一天内的 PV

在 9.5.2 节端对端如何保证 Exactly Once 中的幂等性写入如何保证端对端 Exactly Once 部分已经用案例讲述了如何通过 Flink 的状态来计算 APP 的 PV，并能够保证 Exactly Once。如果在工作中需要计算网站各页面一天内的 PV，只需要将案例中的 APP 替换成各页面的 id 或者各页面的 url 进行统计即可，按照各页面 id 和日期组合做为 key 进行 keyBy，相同页面、相同日期的数据发送到相同的实例中进行 PV 值的累加，每个 key 对应一个 ValueState，将 PV 值维护在 ValueState 即可。如果一些页面属于爆款页面，例如首页或者活动页面访问特别频繁就可能出现某些 subtask 上的数据量特别大，导致各个 subtask 之前出现数据倾斜的问题，关于数据倾斜的解决方案请参考 9.6 节。

### 11.1.2 统计网站各页面一天内 UV 的三种方案

PV 统计相对来说比较简单，每来一条用户的访问日志只需要从日志中提取出相应的页面 id 和日期，将其对应的 PV 值加一即可。相对而言统计 UV 就有难度了，同一个用户一天内多次访问同一个页面，只能计数一次。所以每来一条日志，日志中对应页面的 UV 值是否需要加一呢？存在两种情况：如果该用户今天第一次访问该页面，那么 UV 应该加一。如果该用户今天不是第一次访问该页面，表示 UV 中已经记录了该用户，UV 要基于用户去重，所以此时 UV 值不应该加一。难点就在于如何判断该用户今天是不是第一次访问该页面呢？

把问题简单化，先不考虑日期，现在统计网站各页面的累积 UV，可以为每个页面维护一个 Set 集合，假如网站有 10 个页面，那么就维护 10 个 Set 集合，集合中存放着所有访问过该页面用户的 user_id。每来一条用户的访问日志，我们都需要从日志中解析出相应的页面 id 和用户 user_id，去该页面 id 对应的 Set 中查找该 user_id 之前有没有访问过该页面，如果 Set 中包含该 user_id 表示该用户之前访问过该页面，所以该页面的 UV 值不应该加一，如果 Set 中不包含该 user_id 表示该用户之前没有访问过该页面，所以该页面的 UV 值应该加一，并且将该 user_id 插入到该页面对应的 Set 中，表示该用户访问过该页面了。要按天去统计各页面 UV，只需要将日期和页面 id 看做一个整体 key，每个 key 对应一个 Set，其他流程与上述类似。具体的程序流程图如下图所示。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-12-11-151250.png)

#### 使用 Redis 的 set 来维护用户集合

每个 key 都需要维护一个 Set，这个 Set 存放在哪里呢？这里每条日志都需要访问一次 Set，对 Set 访问比较频繁，对存储介质的延迟要求比较高，所以可以使用 Redis 的 set 数据结构，Redis 的 set 数据结构也会对数据进行去重。可以将页面 id 和日期拼接做为 Redis 的 key，通过 Redis 的 sadd 命令将 user_id 放到 key 对应的 set 中即可。Redis 的 set 中存放着今天访问过该页面所有用户的 user_id。

在真实的工作中，Flink 任务可能不需要维护一个 UV 值，Flink 任务承担的角色是实时计算，而查询 UV 可能是一个 Java Web 项目。Web 项目只需要去 Redis 查询相应 key 对应的 set 中元素的个数即可，Redis 的 set 数据结构有 scard 命令可以查询 set 中元素个数，这里的元素个数就是我们所要统计的网站各页面每天的 UV 值。所以使用 Redis set 数据结构的方案 Flink 任务的代码很简单，只需要从日志中解析出相应的日期、页面id 和 user_id，将日期和页面 id 组合做为 Redis 的 key，最后将 user_id 通过 sadd 命令添加到 set 中，Flink 任务的工作就结束了，之后 Web 项目就能从 Redis 中查询到实时增加的 UV 了。下面来看详细的代码实现。

用户访问网站页面的日志实体类：

```java
public class UserVisitWebEvent {
    // 日志的唯一 id
    private String id;
    // 日期，如：20191025
    private String date;
    // 页面 id
    private Integer pageId;
    // 用户的唯一标识，用户 id
    private String userId;
    // 页面的 url
    private String url;
}
```

生成测试数据的核心代码如下:

```java
String yyyyMMdd = new DateTime(System.currentTimeMillis()).toString("yyyyMMdd");
int pageId = random.nextInt(10);    // 随机生成页面 id
int userId = random.nextInt(100);   // 随机生成用户 id

UserVisitWebEvent userVisitWebEvent = UserVisitWebEvent.builder()
        .id(UUID.randomUUID().toString())   // 日志的唯一 id
        .date(yyyyMMdd)                     // 日期
        .pageId(pageId)                     // 页面 id
        .userId(Integer.toString(userId))   // 用户 id
        .url("url/" + pageId)               // 页面的 url
        .build();
// 对象序列化为 JSON 发送到 Kafka
ProducerRecord record = new ProducerRecord<String, String>(topic,
        null, null, GsonUtil.toJson(userVisitWebEvent));
producer.send(record);
```

统计 UV 的核心代码如下，对 Redis Connector 不熟悉的请参阅 3.11 节如何使用 Flink Connectors —— Redis：

```java
public class RedisSetUvExample {
    public static void main(String[] args) throws Exception {
        //  省略了 env初始化及 Checkpoint 相关配置
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, UvExampleUtil.broker_list);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "app-uv-stat");

        FlinkKafkaConsumerBase<String> kafkaConsumer = new FlinkKafkaConsumer011<>(
                UvExampleUtil.topic, new SimpleStringSchema(), props)
                .setStartFromLatest();

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig
                .Builder().setHost("192.168.30.244").build();

        env.addSource(kafkaConsumer)
                .map(string -> {
                    // 反序列化 JSON
                    UserVisitWebEvent userVisitWebEvent = GsonUtil.fromJson(
                            string, UserVisitWebEvent.class);
                    // 生成 Redis key，格式为 日期_pageId，如: 20191026_0
                    String redisKey = userVisitWebEvent.getDate() + "_"
                            + userVisitWebEvent.getPageId();
                    return Tuple2.of(redisKey, userVisitWebEvent.getUserId());
                })
                .returns(new TypeHint<Tuple2<String, String>>(){})
                .addSink(new RedisSink<>(conf, new RedisSaddSinkMapper()));

        env.execute("Redis Set UV Stat");
    }

    // 数据与 Redis key 的映射关系
    public static class RedisSaddSinkMapper 
            implements RedisMapper<Tuple2<String, String>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            //  这里必须是 sadd 操作
            return new RedisCommandDescription(RedisCommand.SADD);
        }

        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
    }
}
```

Redis 中统计结果如下图所示，左侧展示的 Redis key，20191026_1 表示 2019年10月26日浏览过 pageId 为 1 的页面对应的 key，右侧展示 key 对应的 set 集合，表示 userId 为 [0,6,27,30,66,67,79,88] 的用户在 2019年10月26日浏览过 pageId 为 1 的页面。

<img src="http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-31-174242.jpg" style="zoom:50%;" />

要想获取 20191026_1 对应的 UV 值，可通过 scard 命令获取 set 中 user_id 的数量，具体操作如下所示：

```shell
redis> scard 20191026_1
8
```

通过上述代码即可通过 Redis 的 set 数据结构来统计网站各页面的 UV。

#### 使用 Flink 的 KeyedState 来维护用户集合


#### 使用 Redis 的 HyperLogLog 来统计 UV


### 11.1.3 小结与反思



加入知识星球可以看到上面文章：https://t.zsxq.com/RBYj66M

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-09-25-zsxq.jpg)





