package com.zhisheng.connectors.kudu.serde;

import com.zhisheng.connectors.kudu.connector.KuduRow;
import org.apache.kudu.Schema;

import java.io.Serializable;

/**
 * Desc:
 * Created by zhisheng on 2019-06-08
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public interface KuduSerialization<T> extends Serializable {
    KuduRow serialize(T value);

    KuduSerialization<T> withSchema(Schema schema);
}