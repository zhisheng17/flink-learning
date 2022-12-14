package com.zhisheng.log.appender;

import com.zhisheng.flink.model.LogEvent;
import com.zhisheng.flink.util.ExceptionUtil;
import com.zhisheng.flink.util.JacksonUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;

import java.io.File;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

@Slf4j
@Data
public class KafkaLog4jAppender extends AppenderSkeleton {

    private String bootstrapServers;

    private String source;

    private String topic;

    private String level;

    private String acks;

    private String compressionType;

    private String retries;

    private String batchSize;

    private String lingerMs;

    private String maxRequestSize;

    private String requestTimeoutMs;

    private Producer<String, String> producer;

    private String appId;

    private String containerId;

    private String containerType;

    private String taskId;

    private String taskName;

    @Override
    public void activateOptions() {
        super.activateOptions();

        Properties envProperties = System.getProperties();

        String logFile = envProperties.getProperty("log.file");
        String[] values = logFile.split(File.separator);
        if (values.length >= 3) {
            appId = values[values.length - 3];
            containerId = values[values.length - 2];
            String log = values[values.length - 1];
            if (log.contains("jobmanager")) {
                containerType = "jobmanager";
            } else if (log.contains("taskmanager")) {
                containerType = "taskmanager";
            } else {
                containerType = "others";
            }
        } else {
            log.error("log.file Property ({}) doesn't contains yarn application id or container id", logFile);
        }

        taskId = envProperties.getProperty("taskId", null);
        taskName = envProperties.getProperty("taskName", null);

        Properties props = new Properties();
        if (this.bootstrapServers != null) {
            props.setProperty("bootstrap.servers", this.bootstrapServers);
        } else {
            throw new ConfigException("The bootstrap servers property must be specified");
        }
        if (this.topic == null) {
            throw new ConfigException("Topic must be specified by the Kafka log4j appender");
        }
        if (this.source == null) {
            throw new ConfigException("Source must be specified by the Kafka log4j appender");
        }

        String clientIdPrefix = taskId != null ? taskId : appId;

        if (clientIdPrefix != null) {
            props.setProperty("client.id", clientIdPrefix + "_log");
        }

        if (this.acks != null) {
            props.setProperty("acks", this.acks);
        } else {
            props.setProperty("acks", "0");
        }

        if (this.retries != null) {
            props.setProperty("retries", this.retries);
        } else {
            props.setProperty("retries", "0");
        }

        if (this.batchSize != null) {
            props.setProperty("batch.size", this.batchSize);
        } else {
            props.setProperty("batch.size", "16384");
        }

        if (this.lingerMs != null) {
            props.setProperty("linger.ms", this.lingerMs);
        } else {
            props.setProperty("linger.ms", "5");
        }

        if (this.compressionType != null) {
            props.setProperty("compression.type", this.compressionType);
        } else {
            props.setProperty("compression.type", "lz4");
        }

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
    }

    @Override
    protected void append(LoggingEvent loggingEvent) {
        try {
            if (level.contains(loggingEvent.getLevel().toString().toUpperCase()) && !loggingEvent.getLoggerName().contains("xxx")) { //控制哪些类的日志不收集
                producer.send(new ProducerRecord<>(topic, appId, subAppend(loggingEvent)));
            }
        } catch (Exception e) {
            log.warn("Parsing the log event or send log event to kafka has exception", e);
        }
    }

    private String subAppend(LoggingEvent event) throws JsonProcessingException {
        LogEvent logEvent = new LogEvent();
        Map<String, String> tags = new HashMap<>();
        String logMessage = null;
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            tags.put("host_name", inetAddress.getHostName());
            tags.put("host_ip", inetAddress.getHostAddress());
        } catch (Exception e) {
            log.error("Error getting the ip and host name of the node where the job({}) is running", appId, e);
        } finally {
            try {
                logMessage = ExceptionUtil.stacktraceToString(event.getThrowableInformation().getThrowable());
                logEvent.setContent(logMessage);
            } catch (Exception e) {
                if (logMessage != null) {
                    logMessage = logMessage + "\n\t" + e.getMessage();
                }
                logEvent.setContent(logMessage);
            } finally {
                logEvent.setId(UUID.randomUUID().toString());
                logEvent.setTimestamp(event.getTimeStamp());
                logEvent.setSource(source);
                if (logMessage != null) {
                    logMessage = event.getMessage().toString() + "\n" + logMessage;
                } else {
                    logMessage = event.getMessage().toString();
                }
                logEvent.setContent(logMessage);
                LocationInfo locationInformation = event.getLocationInformation();
                tags.put("class_name", locationInformation.getClassName());
                tags.put("method_name", locationInformation.getMethodName());
                tags.put("file_name", locationInformation.getFileName());
                tags.put("line_number", locationInformation.getLineNumber());
                tags.put("logger_name", event.getLoggerName());
                tags.put("level", event.getLevel().toString());
                tags.put("thread_name", event.getThreadName());
                tags.put("app_id", appId);
                tags.put("container_id", containerId);
                tags.put("container_type", containerType);
                if (taskName != null) {
                    tags.put("task_name", taskName);
                }
                if (taskId != null) {
                    tags.put("task_id", taskId);
                }
                logEvent.setTags(tags);
            }
        }
        return JacksonUtil.toJson(logEvent);
    }

    @Override
    public void close() {
        if (!this.closed) {
            this.closed = true;
            this.producer.close();
        }
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }

}
