package com.zhisheng.connectors.kudu.serde;

import com.zhisheng.connectors.kudu.connector.KuduRow;
import org.apache.kudu.Schema;
/**
 * Desc:
 * Created by zhisheng on 2019-06-08
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class DefaultSerDe implements KuduSerialization<KuduRow>, KuduDeserialization<KuduRow> {

    @Override
    public KuduRow deserialize(KuduRow row) {
        return row;
    }

    @Override
    public KuduRow serialize(KuduRow value) {
        return value;
    }

    @Override
    public DefaultSerDe withSchema(Schema schema) {
        return this;
    }

}

