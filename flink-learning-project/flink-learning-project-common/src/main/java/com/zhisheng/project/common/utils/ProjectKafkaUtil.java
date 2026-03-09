package com.zhisheng.project.common.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

import com.zhisheng.project.common.constant.ProjectConstants;

/**
 * 项目 Kafka 工具类
 * 封装 Kafka Source 和 Sink 的创建逻辑，简化各模块的 Kafka 接入
 *
 * @author zhisheng
 */
public class ProjectKafkaUtil {

    private ProjectKafkaUtil() {
    }

    /**
     * 创建 Kafka String Source
     *
     * @param topic   消费的 topic
     * @param groupId 消费者组 ID
     * @param brokers Kafka broker 地址
     * @return KafkaSource 实例
     */
    public static KafkaSource<String> buildKafkaStringSource(String topic, String groupId, String brokers) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    /**
     * 使用默认配置创建 Kafka String Source
     */
    public static KafkaSource<String> buildKafkaStringSource(String topic, String groupId) {
        return buildKafkaStringSource(topic, groupId, ProjectConstants.DEFAULT_BROKER_LIST);
    }

    /**
     * 创建 Kafka String Sink
     *
     * @param topic   写入的 topic
     * @param brokers Kafka broker 地址
     * @return KafkaSink 实例
     */
    public static KafkaSink<String> buildKafkaStringSink(String topic, String brokers) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
    }

    /**
     * 使用默认配置创建 Kafka String Sink
     */
    public static KafkaSink<String> buildKafkaStringSink(String topic) {
        return buildKafkaStringSink(topic, ProjectConstants.DEFAULT_BROKER_LIST);
    }
}
