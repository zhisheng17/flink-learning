package org.apache.flink.metrics.kafka;

import java.util.HashMap;
import java.util.Map;


public class MetricEvent {
	// measurement name
	private String name;

	// timestamp for metric
	private long timestamp;

	// values for metric
	private Map<String, Object> fields = new HashMap<>();

	// tags for metric
	private Map<String, String> tags = new HashMap<>();

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public MetricEvent(String name, Map<String, String> tags) {
		this.name = name;
		this.tags = tags;
	}

	public MetricEvent(String name, Map<String, String> tags, long metricsTimestamp) {
		this.name = name;
		this.tags = tags;
		this.timestamp = metricsTimestamp;
	}

	public String getName() {
		return name;
	}

	public Map<String, String> getTags() {
		return tags;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setFields(Map<String, Object> fields) {
		this.fields = fields;
	}


	public Map<String, Object> getFields() {
		return fields;
	}

	// add field whit ingore empty key
	public MetricEvent addField(String key, Object val) {
		if (key == null || "".equals(key)) return this;
		fields.put(key, val);
		return this;
	}

	// add tag whit ingore empty key and val
	public MetricEvent addTag(String key, String val) {
		if (key == null || "".equals(key)) return this;
		if (val == null || "".equals(val)) return this;
		tags.put(key, val);
		return this;
	}

	public MetricEvent addTags(Map<String, String> newTags) {
		if (newTags == null || newTags.size() == 0) return this;
		tags.putAll(newTags);
		return this;
	}

	@Override
	public String toString() {
		return "MetricEvent{" +
			"name='" + name + '\'' +
			", timestamp=" + timestamp +
			", fields=" + fields +
			", tags=" + tags +
			'}';
	}
}
