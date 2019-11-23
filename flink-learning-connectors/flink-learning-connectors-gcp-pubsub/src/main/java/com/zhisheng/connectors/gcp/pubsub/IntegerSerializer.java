package com.zhisheng.connectors.gcp.pubsub;

import com.google.pubsub.v1.PubsubMessage;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubDeserializationSchema;

import java.io.IOException;
import java.math.BigInteger;

/**
 * Desc: Deserialization schema to deserialize messages produced by PubSubPublisher
 * Created by zhisheng on 2019/11/23 下午9:22
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class IntegerSerializer implements PubSubDeserializationSchema<Integer>, SerializationSchema<Integer> {

    @Override
    public Integer deserialize(PubsubMessage message) throws IOException {
        return new BigInteger(message.getData().toByteArray()).intValue();
    }

    @Override
    public boolean isEndOfStream(Integer integer) {
        return false;
    }

    @Override
    public TypeInformation<Integer> getProducedType() {
        return TypeInformation.of(Integer.class);
    }

    @Override
    public byte[] serialize(Integer integer) {
        return BigInteger.valueOf(integer).toByteArray();
    }
}
