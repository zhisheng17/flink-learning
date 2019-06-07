package com.zhisheng.connectors.rocketmq.common.serialization;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.Serializable;

/**
 * Desc:
 * Created by zhisheng on 2019-06-05
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public interface KeyValueDeserializationSchema<T> extends ResultTypeQueryable<T>, Serializable {
    T deserializeKeyAndValue(byte[] key, byte[] value);
}
