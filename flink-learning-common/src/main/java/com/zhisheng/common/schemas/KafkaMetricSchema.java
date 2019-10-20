package com.zhisheng.common.schemas;

import com.zhisheng.common.model.MetricEvent;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Desc:
 * Created by zhisheng on 2019-09-23
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class KafkaMetricSchema implements KafkaDeserializationSchema<MetricEvent> {

    @Override
    public boolean isEndOfStream(MetricEvent metricEvent) {
        return false;
    }

    @Override
    public MetricEvent deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        return null;
    }

    @Override
    public TypeInformation<MetricEvent> getProducedType() {
        return TypeInformation.of(new TypeHint<MetricEvent>() {
        });
    }
}
