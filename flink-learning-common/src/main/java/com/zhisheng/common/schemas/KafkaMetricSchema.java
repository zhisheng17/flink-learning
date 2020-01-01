package com.zhisheng.common.schemas;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/**
 * Desc: 实现 KafkaDeserializationSchema 接口的反序列化类，可以获取数据的元数据，
 * 注意这种和 JSONKeyValueDeserializationSchema 有个区别就是，本类是不需要数据源的数据是 JSON
 * Created by zhisheng on 2019-09-23
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class KafkaMetricSchema implements KafkaDeserializationSchema<ObjectNode> {

    private static final long serialVersionUID = 1509391548173891955L;

    private final boolean includeMetadata;
    private ObjectMapper mapper;

    public KafkaMetricSchema(boolean includeMetadata) {
        this.includeMetadata = includeMetadata;
    }

    @Override
    public boolean isEndOfStream(ObjectNode metricEvent) {
        return false;
    }

    @Override
    public ObjectNode deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        ObjectNode node = mapper.createObjectNode();
        if (consumerRecord.key() != null) {
            node.put("key", new String(consumerRecord.key()));
        }
        if (consumerRecord.value() != null) {
            node.put("value", new String(consumerRecord.value()));
        }
        if (includeMetadata) {
            node.putObject("metadata")
                    .put("offset", consumerRecord.offset())
                    .put("topic", consumerRecord.topic())
                    .put("partition", consumerRecord.partition());
        }
        return node;
    }

    @Override
    public TypeInformation<ObjectNode> getProducedType() {
        return getForClass(ObjectNode.class);
    }
}
