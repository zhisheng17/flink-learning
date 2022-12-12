package com.zhisheng.common.schemas;

import com.google.gson.Gson;
import com.zhisheng.common.model.MetricEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Metric Schema ，支持序列化和反序列化
 * <p>
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class MetricSchema implements DeserializationSchema<MetricEvent>, SerializationSchema<MetricEvent> {

    private static final Gson gson = new Gson();

    @Override
    public MetricEvent deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), MetricEvent.class);
    }

    @Override
    public boolean isEndOfStream(MetricEvent metricEvent) {
        return false;
    }

    @Override
    public byte[] serialize(MetricEvent metricEvent) {
        return gson.toJson(metricEvent).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public TypeInformation<MetricEvent> getProducedType() {
        return TypeInformation.of(MetricEvent.class);
    }
}
