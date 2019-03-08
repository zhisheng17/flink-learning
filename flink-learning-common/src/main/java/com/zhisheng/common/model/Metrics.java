package com.zhisheng.common.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Metrics {
	private String name;
	private Long timestamp;
	private Map<String, Object> fields;
	private Map<String, String> tags;
}
