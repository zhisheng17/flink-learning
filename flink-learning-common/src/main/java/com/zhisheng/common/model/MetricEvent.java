package com.zhisheng.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MetricEvent {

	/**
	 * Metric name
	 */
	private String name;

	/**
	 * Metric timestamp
	 */
	private Long timestamp;

	/**
	 * Metric fields
	 */
	private Map<String, Object> fields;

	/**
	 * Metric tags
	 */
	private Map<String, String> tags;
}
