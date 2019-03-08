package com.zhisheng.common.schemas;

import com.google.gson.Gson;
import com.zhisheng.common.model.Metrics;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Metric Schema ，支持序列化和反序列化
 *
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 *
 */
public class MetricSchema implements DeserializationSchema<Metrics>, SerializationSchema<Metrics> {

    private static final Gson gson = new Gson();

    @Override
    public Metrics deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), Metrics.class);
    }

    @Override
    public boolean isEndOfStream(Metrics metricEvent) {
        return false;
    }

    @Override
    public byte[] serialize(Metrics metricEvent) {
        return gson.toJson(metricEvent).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public TypeInformation<Metrics> getProducedType() {
        return TypeInformation.of(Metrics.class);
    }
}
