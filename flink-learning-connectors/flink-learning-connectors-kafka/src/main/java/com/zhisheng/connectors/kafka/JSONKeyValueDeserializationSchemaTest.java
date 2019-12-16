package com.zhisheng.connectors.kafka;

import com.zhisheng.common.utils.ExecutionEnvUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

import static com.zhisheng.common.utils.KafkaConfigUtil.buildKafkaProps;

/**
 * Desc: 该 Schema 可以反序列化 JSON 成对象，并包含数据的元数据信息
 * Created by zhisheng on 2019-12-16 20:37
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class JSONKeyValueDeserializationSchemaTest {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        Properties props = buildKafkaProps(parameterTool);

        FlinkKafkaConsumer011<ObjectNode> kafkaConsumer = new FlinkKafkaConsumer011<>("zhisheng",
                new JSONKeyValueDeserializationSchema(true), //可以控制是否需要元数据字段
                props);

        env.addSource(kafkaConsumer)
                .print();
        //读取到的数据在 value 字段中，对应的元数据在 metadata 字段中
        env.execute();
    }
}
