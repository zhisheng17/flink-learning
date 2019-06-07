package com.zhisheng.connectors.rocketmq.common.serialization;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Desc:
 * Created by zhisheng on 2019-06-05
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class SimpleKeyValueDeserializationSchema implements KeyValueDeserializationSchema<Map> {

    public static final String DEFAULT_KEY_FIELD = "key";
    public static final String DEFAULT_VALUE_FIELD = "value";

    public String keyField;
    public String valueField;

    public SimpleKeyValueDeserializationSchema() {
        this(DEFAULT_KEY_FIELD, DEFAULT_VALUE_FIELD);
    }

    public SimpleKeyValueDeserializationSchema(String keyField, String valueField) {
        this.keyField = keyField;
        this.valueField = valueField;
    }

    @Override
    public Map deserializeKeyAndValue(byte[] key, byte[] value) {
        HashMap map = new HashMap(2);
        if (keyField != null) {
            String k = key != null ? new String(key, StandardCharsets.UTF_8) : null;
            map.put(keyField, k);
        }

        if (valueField != null) {
            String v = value != null ? new String(value, StandardCharsets.UTF_8) : null;
            map.put(valueField, v);
        }
        return map;
    }

    @Override
    public TypeInformation<Map> getProducedType() {
        return null;
    }
}
