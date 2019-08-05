package com.zhisheng.connectors.kudu.example;

import com.zhisheng.common.model.MetricEvent;
import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.common.utils.KafkaConfigUtil;
import com.zhisheng.connectors.kudu.KuduSink;
import com.zhisheng.connectors.kudu.connector.KuduColumnInfo;
import com.zhisheng.connectors.kudu.connector.KuduTableInfo;
import com.zhisheng.connectors.kudu.serde.PojoSerDe;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Desc: 本个 test 类不一定能运行，仅做演示
 * Created by zhisheng on 2019-08-05
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class KuduSinkTest {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        DataStreamSource<MetricEvent> data = KafkaConfigUtil.buildSource(env);

        List<KuduColumnInfo> kuduColumnInfos = new ArrayList<>();
        KuduColumnInfo columnInfo1 = KuduColumnInfo.Builder.createLong("zhisheng").rangeKey(true).build();
        KuduColumnInfo columnInfo2 = KuduColumnInfo.Builder.createFloat("zhisheng").rangeKey(true).build();
        KuduColumnInfo columnInfo3 = KuduColumnInfo.Builder.createString("zhisheng").rangeKey(true).build();
        kuduColumnInfos.add(columnInfo1);
        kuduColumnInfos.add(columnInfo2);
        kuduColumnInfos.add(columnInfo3);

        KuduTableInfo zhisheng = new KuduTableInfo.Builder("zhisheng")
                .replicas(1)
                .createIfNotExist(true)
                .columns(kuduColumnInfos)
                .build();

        data.addSink(new KuduSink<>("127.0.0.1", zhisheng, new PojoSerDe<>(MetricEvent.class)).withInsertWriteMode());
    }
}
