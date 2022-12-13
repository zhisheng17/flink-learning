### flink-metrics-kafka

compile the module and move the target `flink-metrics-kafka.jar` to flink lib folder, and add metrics reporter configuration in the `flink-config.xml`. eg:

```xml
#==============================================================================
#### Kafka Metrics Reporter
###==============================================================================

metrics.reporter.kafka.class: org.apache.flink.metrics.kafka.KafkaReporter

metrics.reporter.kafka.bootstrapServers: http://localhost:9092

metrics.reporter.kafka.topic: metrics-flink-jobs

metrics.reporter.kafka.acks: 0

metrics.reporter.kafka.compressionType: lz4

metrics.reporter.kafka.bufferMemory: 33554432

metrics.reporter.kafka.retries: 0

metrics.reporter.kafka.batchSize: 16384

metrics.reporter.kafka.lingerMs: 5

metrics.reporter.kafka.maxRequestSize: 1048576

metrics.reporter.kafka.requestTimeoutMs: 30000

```
