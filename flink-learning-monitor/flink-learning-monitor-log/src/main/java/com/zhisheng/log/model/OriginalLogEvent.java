package com.zhisheng.log.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

/*
Created by zhisheng on 2019/10/26 下午7:15
blog：http://www.54tianzhisheng.cn/
微信公众号：zhisheng
desc: Filebeat 发送到 Kafka 的日志如下

{
	"@timestamp": "2019-10-26T09:23:16.848Z",
	"@metadata": {
		"beat": "filebeat",
		"type": "doc",
		"version": "6.8.4",
		"topic": "zhisheng_log"
	},
	"host": {
		"name": "VM_0_2_centos"
	},
	"source": "/var/logs/controller.log",
	"message": "[2019-10-26 17:23:11,769] TRACE [Controller id=0] Leader imbalance ratio for broker 0 is 0.0 (kafka.controller.KafkaController)"
}
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OriginalLogEvent {

    @JsonProperty("@timestamp")
    private String timestamp;   //use Jackson JsonProperty

    @JsonProperty("@metadata")
    private Map<String, String> metadata;

    private Map<String, String> host;

    private String source;

    private String message;
}