package com.zhisheng.common.schemas;

import com.google.gson.Gson;
import com.zhisheng.common.model.ProductEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Product Schema ，支持序列化和反序列化
 * <p>
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class ProductSchema implements DeserializationSchema<ProductEvent>, SerializationSchema<ProductEvent> {

    private static final Gson gson = new Gson();

    @Override
    public ProductEvent deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), ProductEvent.class);
    }

    @Override
    public boolean isEndOfStream(ProductEvent productEvent) {
        return false;
    }

    @Override
    public byte[] serialize(ProductEvent productEvent) {
        return gson.toJson(productEvent).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public TypeInformation<ProductEvent> getProducedType() {
        return TypeInformation.of(ProductEvent.class);
    }
}
