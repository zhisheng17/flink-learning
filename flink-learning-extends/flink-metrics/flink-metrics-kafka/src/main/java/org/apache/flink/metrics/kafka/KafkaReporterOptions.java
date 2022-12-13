package org.apache.flink.metrics.kafka;

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.metrics.MetricConfig;

/**
 * Config options for the {@link KafkaReporter}.
 */
@Documentation.SuffixOption
public class KafkaReporterOptions {

	//https://gjtmaster.com/2018/09/03/kafka%E7%94%9F%E4%BA%A7%E8%80%85Producer%E5%8F%82%E6%95%B0%E8%AE%BE%E7%BD%AE%E5%8F%8A%E8%B0%83%E4%BC%98/

	//https://blog.csdn.net/qq_28410283/article/details/88570141

	//通过 hms 申请的 topic，该 topic 批到 logs-kafka 集群
	public static final ConfigOption<String> BOOTSTRAP_SERVERS = ConfigOptions
		.key("bootstrapServers")
		.defaultValue("http://logs-kafka1.ttbike.com.cn:9092,http://logs-kafka2.ttbike.com.cn:9092,http://logs-kafka3.ttbike.com.cn:9092")
		.withDescription("the Kafka broker server host");

	public static final ConfigOption<String> TOPIC = ConfigOptions
		.key("topic")
		.defaultValue("metrics-yarn-flink-jobs")
		.withDescription("the Kafka topic to store metrics");

	public static final ConfigOption<String> ACKS = ConfigOptions
		.key("acks")
		.defaultValue("0")
		.withDescription("the Kafka producer acks(0/-1/1)");

	public static final ConfigOption<String> COMPRESSION_TYPE = ConfigOptions
		.key("compressionType")
		.defaultValue("none")
		.withDescription("Set whether the producer side compresses the message, the default value is none,"
			+ " that is, the message is not compressed，you can choose none/gzip/snappy/lz4");

	public static final ConfigOption<Integer> BUFFER_MEMORY = ConfigOptions
		.key("bufferMemory")
		.defaultValue(33554432)
		.withDescription("This parameter is used to specify the size of the buffer used by the Producer to cache messages, "
			+ "in bytes, the default value is 33554432 and the total is 32M.");

	public static final ConfigOption<Integer> RETRIES = ConfigOptions
		.key("retries")
		.defaultValue(0)
		.withDescription("This parameter indicates the number of retries. The default value is 0, which means no retries.");

	public static final ConfigOption<Integer> BATCH_SIZE = ConfigOptions
		.key("batchSize")
		.defaultValue(16384)
		.withDescription("The default value of the batch.size parameter is 16384, which is 16KB");

	public static final ConfigOption<Integer> LINGER_MS = ConfigOptions
		.key("lingerMs")
		.defaultValue(5)
		.withDescription("In order to reduce network IO, improve the overall TPS. Assuming that linger.ms=5 is set,"
			+ " it means that the producer request may be sent with a delay of 5ms.");

	public static final ConfigOption<Integer> MAX_REQUEST_SIZE = ConfigOptions
		.key("maxRequestSize")
		.defaultValue(1048576)
		.withDescription("This parameter controls the maximum message size that the producer can send, the default is 1048576 bytes (1MB)");

	public static final ConfigOption<Integer> REQUEST_TIMEOUT_MS = ConfigOptions
		.key("requestTimeoutMs")
		.defaultValue(30)
		.withDescription("After the producer sends a request to the broker, the broker needs to"
			+ " return the processing result to the producer within the specified time frame. The default is 30 seconds.");

	public static final ConfigOption<String> DING_DING_ALERT_REBOOT = ConfigOptions
		.key("dingDingAlertReboot")
		.noDefaultValue()
		.withDescription("alert to the flink administrator");


	static String getString(MetricConfig config, ConfigOption<String> key) {
		return config.getString(key.key(), key.defaultValue());
	}

	static int getInteger(MetricConfig config, ConfigOption<Integer> key) {
		return config.getInteger(key.key(), key.defaultValue());
	}
}
