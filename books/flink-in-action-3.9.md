---
toc: true
title: 《Flink 实战与性能优化》—— Flink Connector —— ElasticSearch 的用法和分析
date: 2021-07-20
tags:
- Flink
- 大数据
- 流式计算
---


## 3.9 Flink Connector —— ElasticSearch 的用法和分析

ElasticSearch 现在也是非常火的一门技术，目前很多公司都有使用，本节将介绍 Flink ElasticSearch Connector 的实战使用和可能会遇到的问题。

<!--more-->


### 3.9.1 准备环境和依赖

首先准备 ElasticSearch 的环境和项目的环境依赖。

**ElasticSearch 安装**

因为在 2.1 节中已经讲过 ElasticSearch 的安装，这里就不做过多的重复，需要注意的一点就是 Flink 的 ElasticSearch Connector 是区分版本号的，官方支持的版本如下图所示。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-23-103746.png)

所以添加依赖的时候要区分一下，根据你安装的 ElasticSearch 来选择不一样的版本依赖，另外就是不同版本的 ElasticSearch 还会导致下面的数据写入到 ElasticSearch 中出现一些不同，我们这里使用的版本是 ElasticSearch6，如果你使用的是其他的版本可以参考官网的实现。

**添加依赖**

因为我们在 2.1 节中安装的 ElasticSearch 版本是 6.3.2 版本的，所有这里引入的依赖就选择 `flink-connector-elasticsearch6`，具体依赖如下所示。

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-elasticsearch6_${scala.binary.version}</artifactId>
    <version>${flink.version}</version>
</dependency>
```

上面这个 `scala.binary.version` 和 `flink.version` 版本号需要自己在使用的时候根据使用的版本做相应的改变。


### 3.9.2 使用 Flink 将数据写入到 ElasticSearch 应用程序

准备好环境和相关的依赖后，接下来开始编写 Flink 程序。

ESSinkUtil 工具类，代码如下所示，这个工具类是笔者封装的，getEsAddresses 方法将传入的配置文件 es 地址解析出来，可以是域名方式，也可以是 ip + port 形式。addSink 方法是利用了 Flink 自带的 ElasticsearchSink 来封装了一层，传入了一些必要的调优参数和 es 配置参数，下面章节还会再讲其他的配置。

```java
public class ESSinkUtil {
    /**
     * es sink
     *
     * @param hosts es hosts
     * @param bulkFlushMaxActions bulk flush size
     * @param parallelism 并行数
     * @param data 数据
     * @param func
     * @param <T>
     */
    public static <T> void addSink(List<HttpHost> hosts, int bulkFlushMaxActions, int parallelism,
                                   SingleOutputStreamOperator<T> data, ElasticsearchSinkFunction<T> func) {
        ElasticsearchSink.Builder<T> esSinkBuilder = new ElasticsearchSink.Builder<>(hosts, func);
        esSinkBuilder.setBulkFlushMaxActions(bulkFlushMaxActions);
        data.addSink(esSinkBuilder.build()).setParallelism(parallelism);
    }

    /**
     * 解析配置文件的 es hosts
     *
     * @param hosts
     * @return
     * @throws MalformedURLException
     */
    public static List<HttpHost> getEsAddresses(String hosts) throws MalformedURLException {
        String[] hostList = hosts.split(",");
        List<HttpHost> addresses = new ArrayList<>();
        for (String host : hostList) {
            if (host.startsWith("http")) {
                URL url = new URL(host);
                addresses.add(new HttpHost(url.getHost(), url.getPort()));
            } else {
                String[] parts = host.split(":", 2);
                if (parts.length > 1) {
                    addresses.add(new HttpHost(parts[0], Integer.parseInt(parts[1])));
                } else {
                    throw new MalformedURLException("invalid elasticsearch hosts format");
                }
            }
        }
        return addresses;
    }
}
```

Flink 程序会读取到 ElasticSearch 的配置，然后将从 Kafka 读取到的数据写入进 ElasticSearch，具体的写入代码如下所示。

```java
public class Sink2ES6Main {
    public static void main(String[] args) throws Exception {
        //获取所有参数
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        //准备好环境
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        //从kafka读取数据
        DataStreamSource<Metrics> data = KafkaConfigUtil.buildSource(env);

        //从配置文件中读取 es 的地址
        List<HttpHost> esAddresses = ESSinkUtil.getEsAddresses(parameterTool.get(ELASTICSEARCH_HOSTS));
        //从配置文件中读取 bulk flush size，代表一次批处理的数量，这个可是性能调优参数，特别提醒
        int bulkSize = parameterTool.getInt(ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS, 40);
        //从配置文件中读取并行 sink 数，这个也是性能调优参数，特别提醒，这样才能够更快的消费，防止 kafka 数据堆积
        int sinkParallelism = parameterTool.getInt(STREAM_SINK_PARALLELISM, 5);

        //自己再自带的 es sink 上一层封装了下
        ESSinkUtil.addSink(esAddresses, bulkSize, sinkParallelism, data,
                (Metrics metric, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
                    requestIndexer.add(Requests.indexRequest()
                            .index(ZHISHENG + "_" + metric.getName())  //es 索引名
                            .type(ZHISHENG) //es type
                            .source(GsonUtil.toJSONBytes(metric), XContentType.JSON)); 
                });
        env.execute("flink learning connectors es6");
    }
}
```

配置文件中包含了 Kafka 和 ElasticSearch 的配置，如下所示，地址都支持集群模式填写，注意用 `,` 分隔。

```properties
kafka.brokers=localhost:9092
kafka.group.id=zhisheng-metrics-group-test
kafka.zookeeper.connect=localhost:2181
metrics.topic=zhisheng-metrics
stream.parallelism=5
stream.checkpoint.interval=1000
stream.checkpoint.enable=false
elasticsearch.hosts=localhost:9200
elasticsearch.bulk.flush.max.actions=40
stream.sink.parallelism=5
```

### 3.9.3 验证数据是否写入 ElasticSearch？


### 3.9.4 如何保证在海量数据实时写入下 ElasticSearch 的稳定性？


### 3.9.5 使用 Flink-connector-elasticsearch 可能会遇到的问题


### 3.9.6 小结与反思

加入知识星球可以看到上面文章：https://t.zsxq.com/Jeqzfem

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-09-25-zsxq.jpg)


