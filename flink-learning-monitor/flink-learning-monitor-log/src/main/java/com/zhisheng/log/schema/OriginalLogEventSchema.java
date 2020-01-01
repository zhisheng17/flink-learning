package com.zhisheng.log.schema;

import com.zhisheng.log.model.OriginalLogEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Desc: OriginalLogEvent Deserialization Schema
 * Created by zhisheng on 2019/10/26 下午7:15
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class OriginalLogEventSchema implements DeserializationSchema<OriginalLogEvent> {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public OriginalLogEvent deserialize(byte[] bytes) throws IOException {
        return mapper.readValue(new String(bytes), OriginalLogEvent.class);
    }

    @Override
    public boolean isEndOfStream(OriginalLogEvent originalLogEvent) {
        return false;
    }


    @Override
    public TypeInformation<OriginalLogEvent> getProducedType() {
        return TypeInformation.of(OriginalLogEvent.class);
    }
}