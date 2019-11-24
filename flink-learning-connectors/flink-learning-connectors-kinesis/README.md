### Flink connector kinesis

**Kinesis 介绍**：

Kinesis 是一种在 AWS 上流式处理数据的平台，可提供托管服务，让您能够轻松地加载和分析流数据，同时还可让您根据具体需求来构建自定义流数据应用程序。

Amazon Kinesis Streams 是一项在实时处理大规模的数据流时可弹性扩展的托管服务。该服务收集大规模的数据记录流，随后供多个可在 EC2 实例上运行的数据处理应用程序实时使用。

**Kinesis 工作原理**：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-23-143442.jpg)

**Amazon Kinesis Data Streams**：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-23-143540.jpg)

**Amazon Kinesis Data Firehose**：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-23-143615.jpg)

**Amazon Kinesis Data Analytics**：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-23-143636.jpg)

**使用案例**：

构建视频分析应用程序，你可以使用 Amazon Kinesis 安全地将视频从家里、办公室、工厂和公共场所中配备摄像头的设备流式传输到 AWS。然后，您可以使用这些视频流进行视频回放、安全监控、人脸检测、机器学习和其他分析。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-23-143713.jpg)

从批量分析发展到实时分析，某些数据以前通过数据仓库批量处理的方式或通过 Hadoop 框架进行分析，而借助 Amazon Kinesis，您可以对此类数据进行实时分析。最常见的使用案例包括数据湖、数据科学和机器学习。您可以使用 Kinesis Data Firehose 将数据持续加载到 S3 数据湖中。您也可以在获得新数据时更频繁地更新机器学习模型，确保结果的准确性和可靠性。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-23-143820.jpg)

构建实时应用程序，你可以将 Amazon Kinesis 用于实时应用程序，例如监控、欺诈识别和直播排行榜应用程序。您可以使用 Kinesis Data Streams 接收流数据，使用 Kinesis Data Analytics 处理流数据，然后使用 Kinesis Data Streams 将结果发送到任何数据存储或应用程序，端到端延迟只有几毫秒。这可以帮助您了解自己的客户、应用程序和产品的当前状态并迅速做出反应。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-23-143915.jpg)

分析 IoT 设备数据，你可以使用 Amazon Kinesis 来处理来自消费电器、嵌入式传感器和电视机顶盒等 IoT 设备的流数据。然后您可以用编程方式使用这些数据，以便在传感器超过特定运行阈值时发送实时提醒或进行其他操作。使用我们的 IoT 分析示例代码来构建您的应用程序。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-23-143945.jpg)


**添加依赖**：

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kinesis_${scala.binary.version}</artifactId>
    <version>${flink.version}</version>
</dependency>
```