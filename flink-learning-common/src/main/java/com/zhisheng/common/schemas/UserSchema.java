package com.zhisheng.common.schemas;

import com.google.gson.Gson;
import com.zhisheng.common.model.UserEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * User Schema ，支持序列化和反序列化
 * <p>
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class UserSchema implements DeserializationSchema<UserEvent>, SerializationSchema<UserEvent> {

    private static final Gson gson = new Gson();

    @Override
    public UserEvent deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), UserEvent.class);
    }

    @Override
    public boolean isEndOfStream(UserEvent userEvent) {
        return false;
    }

    @Override
    public byte[] serialize(UserEvent userEvent) {
        return gson.toJson(userEvent).getBytes(Charset.forName("UTF-8"));
    }

    @Override
    public TypeInformation<UserEvent> getProducedType() {
        return TypeInformation.of(UserEvent.class);
    }
}
