package com.zhisheng.flink.model;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class LogEvent {

    private String source; // default is flink, maybe others will use this kafka appender in future

    private String id; // log id, default it is UUID

    private Long timestamp;

    private String content; // log message

    private Map<String, String> tags = new HashMap<>(); // tags of the log, eg: host_name, application_id, job_name etc

}
