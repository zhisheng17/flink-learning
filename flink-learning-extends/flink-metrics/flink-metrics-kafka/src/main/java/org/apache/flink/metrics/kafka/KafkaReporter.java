package org.apache.flink.metrics.kafka;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.*;
import org.apache.flink.metrics.kafka.util.JacksonUtil;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.FrontMetricGroup;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.apache.flink.metrics.kafka.KafkaReporterOptions.*;

/**
 * {@link MetricReporter} that exports {@link Metric Metrics} via Kafka.
 */
@PublicEvolving
@InstantiateViaFactory(factoryClassName = "org.apache.flink.metrics.kafka.KafkaReporterFactory")
public class KafkaReporter implements MetricReporter, Scheduled {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaReporter.class);

	private final Map<Gauge<?>, MetricEvent> gauges = new HashMap<>();
	private final Map<Counter, MetricEvent> counters = new HashMap<>();
	private final Map<Histogram, MetricEvent> histograms = new HashMap<>();
	private final Map<Meter, MetricEvent> meters = new HashMap<>();

	private static final Map<String, String> kafkaLagTimes = new HashMap<>();

	@VisibleForTesting
	static final char SCOPE_SEPARATOR = '_';

	private static final CharacterFilter CHARACTER_FILTER = new CharacterFilter() {
		private final Pattern notAllowedCharacters = Pattern.compile("[^a-zA-Z0-9:_]");

		@Override
		public String filterCharacters(String input) {
			return notAllowedCharacters.matcher(input).replaceAll("_");
		}
	};

	private Producer<String, String> producer;
	private String topic;
	private String appId;
	private String containerId;
	private String taskName;
	private String taskId;


	@Override
	public void open(MetricConfig config) {
		Map<String, String> envs = System.getenv();
		String clusterId = envs.get("CLUSTER_ID");
		if (clusterId != null) {
			//k8s cluster
			appId = clusterId;
			containerId = envs.get("HOSTNAME");
		} else {
			//yarn cluster
			String pwd = envs.get("PWD");
			String[] values = pwd.split(File.separator);
			if (values.length >= 2) {
				appId = values[values.length - 2];
				containerId = values[values.length - 1];
			} else {
				LOG.error("PWD env ({}) doesn't contains yarn application id or container id", pwd);
				throw new RuntimeException(
					"PWD env doesn't contains yarn application id or container id");
			}
		}

		Properties properties = System.getProperties();
		taskName = properties.getProperty("taskName", null);
		taskId = properties.getProperty("taskId", null);

		Properties props = new Properties();
		String clientIdPrefix = taskId != null ? taskId : appId;
		props.put("client.id", "flink_" + clientIdPrefix + "_metrics");
		props.put("bootstrap.servers", getString(config, BOOTSTRAP_SERVERS));
		props.put("acks", getString(config, ACKS));
		props.put("retries", getInteger(config, RETRIES));
		props.put("batch.size", getInteger(config, BATCH_SIZE));
		props.put("linger.ms", getInteger(config, LINGER_MS));
		props.put("buffer.memory", getInteger(config, BUFFER_MEMORY));
		props.put("max.request.size", getInteger(config, MAX_REQUEST_SIZE));
		props.put("request.timeout.ms", getInteger(config, REQUEST_TIMEOUT_MS));
		props.put("compression.type", getString(config, COMPRESSION_TYPE));
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<>(props);
		topic = getString(config, TOPIC);
	}

	@Override
	public void close() {
		producer.close();
	}

	@Override
	public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
		MetricEvent metricEvent = new MetricEvent(getScopedName(metricName, group), getTags(group));
		synchronized (this) {
			if (metric instanceof Counter) {
				counters.put((Counter) metric, metricEvent);
			} else if (metric instanceof Gauge) {
				gauges.put((Gauge<?>) metric, metricEvent);
			} else if (metric instanceof Histogram) {
				histograms.put((Histogram) metric, metricEvent);
			} else if (metric instanceof Meter) {
				meters.put((Meter) metric, metricEvent);
			} else {
				LOG.warn("Cannot add unknown metric type {}. This indicates that the reporter " +
					"does not support this metric type.", metric.getClass().getName());
			}
		}
	}

	@Override
	public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
		synchronized (this) {
			if (metric instanceof Counter) {
				counters.remove(metric);
			} else if (metric instanceof Gauge) {
				gauges.remove(metric);
			} else if (metric instanceof Histogram) {
				histograms.remove(metric);
			} else if (metric instanceof Meter) {
				meters.remove(metric);
			} else {
				LOG.warn("Cannot remove unknown metric type {}. This indicates that the reporter " +
					"does not support this metric type.", metric.getClass().getName());
			}
		}
	}

	@Override
	public void report() {
		try {
			long currentTimeMillis = System.currentTimeMillis();
			Map<String, String> tags = new HashMap<>();
			tags.put("app_id", appId);
			tags.put("container_id", containerId);
			tags.put("flink_version", "1.12.0");
			if (taskName != null) {
				tags.put("dataman_task_name", taskName);
			}
			if (taskId != null) {
				tags.put("dataman_task_id", taskId);
			}

			Map<String, String> envs = System.getenv();

			//k8s 集群，该值为物理机器 ip，和 pod ip 有区别
			if (envs.containsKey("_HOST_IP_ADDRESS")) {
				tags.put("node_ip", envs.get("_HOST_IP_ADDRESS"));
			}
			//后面的条件是为了 k8s cluster 区分
			if (envs.containsKey("_APP_ID") || !envs.get("HOSTNAME").contains("taskmanager")) {
				//JobManager
				tags.put("container_type", "jobmanager");
				MetricEvent jvmClassLoader = new MetricEvent("jobmanager_Status_JVM_ClassLoader", tags, currentTimeMillis);
				MetricEvent jvmGarbageCollector = new MetricEvent("jobmanager_Status_JVM_GarbageCollector", tags, currentTimeMillis);
				MetricEvent jvmMemory = new MetricEvent("jobmanager_Status_JVM_Memory", tags, currentTimeMillis);
				MetricEvent jvmCPU = new MetricEvent("jobmanager_Status_JVM_CPU", tags, currentTimeMillis);
				MetricEvent jvmThreadsCount = new MetricEvent("jobmanager_Status_JVM_Threads_Count", tags, currentTimeMillis);
				MetricEvent jobCheckpointing = new MetricEvent("jobmanager_Job_Checkpointing", tags, currentTimeMillis);
				MetricEvent cluster = new MetricEvent("jobmanager_Cluster", tags, currentTimeMillis);

				for (Map.Entry<Gauge<?>, MetricEvent> entry : gauges.entrySet()) {
					MetricEvent event = entry.getValue();
					String name = event.getName();
					if (name.contains("ClassLoader")) {
						jvmClassLoader.addTags(event.getTags());
						String[] groups = name.split("_");
						addFields(jvmClassLoader, groups[groups.length - 1], entry.getKey());
					} else if (name.contains("GarbageCollector")) {
						jvmGarbageCollector.addTags(event.getTags());
						String[] groups = name.split("GarbageCollector_");
						addFields(jvmGarbageCollector, groups[groups.length - 1], entry.getKey());
					} else if (name.contains("JVM_Memory")) {
						jvmMemory.addTags(event.getTags());
						String[] groups = name.split("JVM_Memory_");
						addFields(jvmMemory, groups[groups.length - 1], entry.getKey());
					} else if (name.contains("JVM_CPU")) {
						jvmCPU.addTags(event.getTags());
						String[] groups = name.split("JVM_CPU_");
						addFields(jvmCPU, groups[groups.length - 1], entry.getKey());
					} else if (name.contains("JVM_Threads_Count")) {
						jvmThreadsCount.addTags(event.getTags());
						String[] groups = name.split("Status_JVM_");
						addFields(jvmThreadsCount, groups[groups.length - 1], entry.getKey());
					} else if (name.contains("Checkpoint")) {
						jobCheckpointing.addTags(event.getTags());
						String[] groups = name.split("job_");
						addFields(jobCheckpointing, groups[groups.length - 1], entry.getKey());
					} else {
						cluster.addTags(event.getTags());
						String[] groups = name.split("jobmanager_");
						addFields(cluster, groups[groups.length - 1], entry.getKey());
					}
				}
				producer.send(new ProducerRecord<>(topic, appId, JacksonUtil.toJson(jvmClassLoader)));
				producer.send(new ProducerRecord<>(topic, appId, JacksonUtil.toJson(jvmGarbageCollector)));
				producer.send(new ProducerRecord<>(topic, appId, JacksonUtil.toJson(jvmMemory)));
				producer.send(new ProducerRecord<>(topic, appId, JacksonUtil.toJson(jvmCPU)));
				producer.send(new ProducerRecord<>(topic, appId, JacksonUtil.toJson(jvmThreadsCount)));
				producer.send(new ProducerRecord<>(topic, appId, JacksonUtil.toJson(jobCheckpointing)));
				producer.send(new ProducerRecord<>(topic, appId, JacksonUtil.toJson(cluster)));
			} else {
				//TaskManager
				tags.put("container_type", "taskmanager");
				MetricEvent jvmClassLoader = new MetricEvent("taskmanager_Status_JVM_ClassLoader", tags, currentTimeMillis);
				MetricEvent jvmGarbageCollector = new MetricEvent("taskmanager_Status_JVM_GarbageCollector", tags, currentTimeMillis);
				MetricEvent jvmMemory = new MetricEvent("taskmanager_Status_JVM_Memory", tags, currentTimeMillis);
				MetricEvent jvmCPU = new MetricEvent("taskmanager_Status_JVM_CPU", tags, currentTimeMillis);
				MetricEvent jvmThreadsCount = new MetricEvent("taskmanager_Status_JVM_Threads_Count", tags, currentTimeMillis);
				MetricEvent statusShuffleNetty = new MetricEvent("taskmanager_Status_Shuffle_Netty", tags, currentTimeMillis);

				for (Map.Entry<Gauge<?>, MetricEvent> entry : gauges.entrySet()) {
					MetricEvent event = entry.getValue();
					String name = event.getName();
					if (name.contains("ClassLoader")) {
						jvmClassLoader.addTags(event.getTags());
						String[] groups = name.split("_");
						addFields(jvmClassLoader, groups[groups.length - 1], entry.getKey());
					} else if (name.contains("GarbageCollector")) {
						jvmGarbageCollector.addTags(event.getTags());
						String[] groups = name.split("GarbageCollector_");
						addFields(jvmGarbageCollector, groups[groups.length - 1], entry.getKey());
					} else if (name.contains("_Memory_")) {
						jvmMemory.addTags(event.getTags());
						String[] groups = name.split("Memory_");
						addFields(jvmMemory, groups[groups.length - 1], entry.getKey());
					} else if (name.contains("JVM_CPU")) {
						jvmCPU.addTags(event.getTags());
						String[] groups = name.split("JVM_CPU_");
						addFields(jvmCPU, groups[groups.length - 1], entry.getKey());
					} else if (name.contains("JVM_Threads_Count")) {
						jvmThreadsCount.addTags(event.getTags());
						String[] groups = name.split("Status_JVM_");
						addFields(jvmThreadsCount, groups[groups.length - 1], entry.getKey());
					} else if (name.contains("Status_Shuffle_Netty")) {
						statusShuffleNetty.addTags(event.getTags());
						String[] groups = name.split("Shuffle_Netty_");
						addFields(statusShuffleNetty, groups[groups.length - 1], entry.getKey());
					} else if (name.contains("taskmanager_job_task_buffers") || name.contains("taskmanager_Status_Network")) {
						continue;
					} else if (name.startsWith("taskmanager_job_task_operator_KafkaConsumer")) {
						if (name.contains("currentDataTimestampOffsetsAndCommittedOffsets")) {
							MetricEvent metricEvent = addKafkaLagMetricFields(entry.getValue(), currentTimeMillis, entry.getKey());
							if (metricEvent == null) {
								continue;
							}
							//todo：可能 Kafka Lag Time 埋点可能需要额外发送到一个 topic 供 Kafka Lag 告警使用
							producer.send(new ProducerRecord<>(topic, appId, JacksonUtil.toJson(metricEvent.addTags(tags))));
						} else {
							continue;
						}
					} else {
						MetricEvent metricEvent = addFields(entry.getValue(), currentTimeMillis, entry.getKey());
						producer.send(new ProducerRecord<>(topic, appId, JacksonUtil.toJson(metricEvent.addTags(tags))));
					}
				}
				producer.send(new ProducerRecord<>(topic, appId, JacksonUtil.toJson(jvmClassLoader)));
				producer.send(new ProducerRecord<>(topic, appId, JacksonUtil.toJson(jvmGarbageCollector)));
				producer.send(new ProducerRecord<>(topic, appId, JacksonUtil.toJson(jvmMemory)));
				producer.send(new ProducerRecord<>(topic, appId, JacksonUtil.toJson(jvmCPU)));
				producer.send(new ProducerRecord<>(topic, appId, JacksonUtil.toJson(jvmThreadsCount)));
				producer.send(new ProducerRecord<>(topic, appId, JacksonUtil.toJson(statusShuffleNetty)));
			}

			for (Map.Entry<Counter, MetricEvent> entry : counters.entrySet()) {
				String name = entry.getValue().getName();
				if (name.contains("taskmanager_job_task_numBytesIn") && name.length() > 31) {
					continue;
				}
				MetricEvent metricEvent = addFields(entry.getValue(), currentTimeMillis, entry.getKey());
				metricEvent.addTags(tags);
				producer.send(new ProducerRecord<>(topic, appId, JacksonUtil.toJson(metricEvent)));
			}
			for (Map.Entry<Histogram, MetricEvent> entry : histograms.entrySet()) {
				MetricEvent metricEvent = addFields(entry.getValue(), currentTimeMillis, entry.getKey());
				metricEvent.addTags(tags);
				producer.send(new ProducerRecord<>(topic, appId, JacksonUtil.toJson(metricEvent)));
			}
			for (Map.Entry<Meter, MetricEvent> entry : meters.entrySet()) {
				String name = entry.getValue().getName();
				if (name.contains("taskmanager_job_task_numBytesIn") && name.length() > 31) {
					continue;
				}
				MetricEvent metricEvent = addFields(entry.getValue(), currentTimeMillis, entry.getKey());
				metricEvent.addTags(tags);
				producer.send(new ProducerRecord<>(topic, appId, JacksonUtil.toJson(metricEvent)));
			}
		} catch (Exception e) {
//			LOG.warn("producer the metrics to kafka has exception", e);
			//todo: 计数，当出现多少发送失败的时候给自己一个告警
		}
	}

	private static Map<String, String> getTags(MetricGroup group) {
		// Keys are surrounded by brackets: remove them, transforming "<name>" to "name".
		Map<String, String> tags = new HashMap<>();
		for (Map.Entry<String, String> variable : group.getAllVariables().entrySet()) {
			String name = variable.getKey();
			//remove TaskManager tm_id tag, because we will add container_id tag when report, the two tag value is same
			if (name.contains("tm_id")) {
				continue;
			}
			tags.put(name.substring(1, name.length() - 1), variable.getValue());
		}
		return tags;
	}

	private static String getScopedName(String metricName, MetricGroup group) {
		return getLogicalScope(group) + SCOPE_SEPARATOR + metricName;
	}

	private static String getLogicalScope(MetricGroup group) {
		return ((FrontMetricGroup<AbstractMetricGroup<?>>) group).getLogicalScope(
			CHARACTER_FILTER,
			SCOPE_SEPARATOR);
	}

	static MetricEvent addFields(MetricEvent metricEvent, String field, Gauge<?> gauge) {
		Object value = gauge.getValue();
		Map<String, Object> fields = metricEvent.getFields();
		if (fields != null) {
			if (value instanceof Number) {
				metricEvent.addField(field, (Number) value);
			} else {
				metricEvent.addField(field, String.valueOf(value));
			}
		} else {
			Map<String, Object> eventFields = new HashMap<>();
			if (value instanceof Number) {
				eventFields.put(field, (Number) value);
			} else {
				eventFields.put(field, String.valueOf(value));
			}
			metricEvent.setFields(eventFields);
		}
		return metricEvent;
	}

	static MetricEvent addKafkaLagMetricFields(MetricEvent metricEvent, Long timestamp, Gauge<?> gauge) {
		String gaugeValue = (String) gauge.getValue();
		String[] split = gaugeValue.split("_");
		Map<String, String> tags = metricEvent.getTags();
		if (split.length == 3) {
			Map<String, Object> fields = new HashMap<>(3);
			fields.put("currentOffsets", Long.valueOf(split[0]));
			fields.put("currentDataTimestamp", Long.valueOf(split[1]));
			fields.put("committedOffsets", Long.valueOf(split[2]));
			String key = tags.get("kafka") + tags.get("topic") + tags.get("group") + tags.get("partition");
			String value = split[0] + "_" +split[1];
			if (kafkaLagTimes.get(key) != null && kafkaLagTimes.get(key).equals(value)) {
				return null;
			} else {
				kafkaLagTimes.put(key, value);
			}
			metricEvent.setFields(fields);
		}
		metricEvent.setTimestamp(timestamp);
		return metricEvent;
	}

	static MetricEvent addFields(MetricEvent metricEvent, Long timestamp, Gauge<?> gauge) {
		Object value = gauge.getValue();
		Map<String, Object> fields = new HashMap<>(1);
		if (value instanceof Number) {
			fields.put("value", (Number) value);
		} else {
			fields.put("value", String.valueOf(value));
		}
		metricEvent.setFields(fields);
		metricEvent.setTimestamp(timestamp);
		return metricEvent;
	}

	static MetricEvent addFields(MetricEvent metricEvent, Long timestamp, Counter counter) {
		Map<String, Object> fields = new HashMap<>(1);
		fields.put("count", counter.getCount());
		metricEvent.setFields(fields);
		metricEvent.setTimestamp(timestamp);
		return metricEvent;
	}

	static MetricEvent addFields(MetricEvent metricEvent, Long timestamp, Histogram histogram) {
		HistogramStatistics statistics = histogram.getStatistics();
		Map<String, Object> fields = new HashMap<>(11);
		fields.put("count", statistics.size());
		fields.put("min", statistics.getMin());
		fields.put("max", statistics.getMax());
		fields.put("stddev", statistics.getStdDev());
		fields.put("mean", statistics.getMean());
		fields.put("p50", statistics.getQuantile(.50));
		fields.put("p75", statistics.getQuantile(.75));
		fields.put("p95", statistics.getQuantile(.95));
		fields.put("p98", statistics.getQuantile(.98));
		fields.put("p99", statistics.getQuantile(.99));
		fields.put("p999", statistics.getQuantile(.999));
		metricEvent.setFields(fields);
		metricEvent.setTimestamp(timestamp);
		return metricEvent;
	}

	static MetricEvent addFields(MetricEvent metricEvent, Long timestamp, Meter meter) {
		Map<String, Object> fields = new HashMap<>(2);
		fields.put("count", meter.getCount());
		fields.put("rate", meter.getRate());
		metricEvent.setFields(fields);
		metricEvent.setTimestamp(timestamp);
		return metricEvent;
	}

}
