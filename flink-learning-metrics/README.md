### flink-learning-metrics

**Metrics 类型**：

+ Counter
+ Gauge
+ Histogram
+ Meter

在 `com.zhisheng.metrics.custom` 包下面有上面四种 Metrics 类型的自定义测试类，其中自定义 Histogram 和 Meter 需要引入依赖：

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-metrics-dropwizard</artifactId>
    <version>${flink.version}</version>
</dependency>
```

关于 Flink Metrics 的源码解析可以参考我的博客：

[Flink Metrics 源码解析](http://www.54tianzhisheng.cn/2019/07/02/Flink-code-metrics/)

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-07-26-150037.jpg)