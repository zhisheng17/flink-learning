### Flink connector gcp pubsub

关于 gcp pubsub 的介绍

https://cloud.google.com/pubsub/?hl=zh-cn

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-23-130544.jpg)

添加依赖：

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-gcp-pubsub_${scala.binary.version}</artifactId>
    <version>${flink.version}</version>
</dependency>
```