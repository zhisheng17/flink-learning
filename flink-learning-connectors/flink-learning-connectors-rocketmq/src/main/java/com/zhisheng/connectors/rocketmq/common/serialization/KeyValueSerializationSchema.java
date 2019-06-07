package com.zhisheng.connectors.rocketmq.common.serialization;

import java.io.Serializable;

/**
 * Desc:
 * Created by zhisheng on 2019-06-05
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public interface KeyValueSerializationSchema<T> extends Serializable {
    byte[] serializeKey(T tuple);

    byte[] serializeValue(T tuple);
}
