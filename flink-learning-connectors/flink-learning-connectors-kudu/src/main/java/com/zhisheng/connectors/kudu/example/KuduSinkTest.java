package com.zhisheng.connectors.kudu.example;

import com.zhisheng.common.model.MetricEvent;
import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.common.utils.KafkaConfigUtil;
import com.zhisheng.connectors.kudu.KuduSink;
import com.zhisheng.connectors.kudu.connector.KuduTableInfo;
import com.zhisheng.connectors.kudu.serde.PojoSerDe;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * Desc:
 * Created by zhisheng on 2019-08-05
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class KuduSinkTest {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        DataStreamSource<MetricEvent> data = KafkaConfigUtil.buildSource(env);

        KuduTableInfo zhisheng = new KuduTableInfo.Builder("zhisheng").replicas(1).createIfNotExist(true).columns(new ArrayList<>()).build();

        data.addSink(new KuduSink<>("127.0.0.1", zhisheng, new PojoSerDe<>(MetricEvent.class)).withInsertWriteMode());
    }
}
