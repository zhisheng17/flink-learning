## FlinkLogKafkaAppender

+ Log4jKafkaAppender： 适用 Flink 1.10 版本（使用的是 log4j）
+ Log4j2KafkaAppender：适用 Flink 1.10 之后版本（使用的是 log4j2）

### 使用方式

1、将项目打出来打 kafka appender jar 包放到 flink lib 目录

2、按照项目提示的 flink log4j 配置去配置 flink conf 下面的 log4j.properties 文件，其中 k8s 的要配置 log4j-console.properties 文件