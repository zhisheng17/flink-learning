package com.zhisheng.connectors.rocketmq.common.serialization;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Desc:
 * Created by zhisheng on 2019-06-05
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class SimpleKeyValueSerializationSchema implements KeyValueSerializationSchema<Map> {

    public static final String DEFAULT_KEY_FIELD = "key";
    public static final String DEFAULT_VALUE_FIELD = "value";

    public String keyField;
    public String valueField;

    public SimpleKeyValueSerializationSchema() {
        this(DEFAULT_KEY_FIELD, DEFAULT_VALUE_FIELD);
    }

    public SimpleKeyValueSerializationSchema(String keyField, String valueField) {
        this.keyField = keyField;
        this.valueField = valueField;
    }

    @Override
    public byte[] serializeKey(Map tuple) {
        return getBytes(tuple, keyField);
    }

    @Override
    public byte[] serializeValue(Map tuple) {
        return getBytes(tuple, valueField);
    }

    private byte[] getBytes(Map tuple, String key) {
        if (tuple == null || key == null) {
            return null;
        }
        Object value = tuple.get(key);
        return value != null ? value.toString().getBytes(StandardCharsets.UTF_8) : null;
    }
}
