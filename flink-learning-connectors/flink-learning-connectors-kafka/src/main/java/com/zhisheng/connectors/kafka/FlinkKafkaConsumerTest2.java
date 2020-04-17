package com.zhisheng.connectors.kafka;

import com.zhisheng.common.utils.ExecutionEnvUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

import static com.zhisheng.common.utils.KafkaConfigUtil.buildKafkaProps;

/**
 * Desc:
 * Created by zhisheng on 2020-03-13 15:20
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class FlinkKafkaConsumerTest2 {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env.setParallelism(1);
        Properties props = buildKafkaProps(parameterTool);

        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>("user_behavior_sink", new SimpleStringSchema(), props);

        env.addSource(consumer).print();

        env.execute("flink kafka connector test");
    }
}
