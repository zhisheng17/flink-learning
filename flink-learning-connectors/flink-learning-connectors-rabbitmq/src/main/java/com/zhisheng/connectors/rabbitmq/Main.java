package com.zhisheng.connectors.rabbitmq;


import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.common.utils.KafkaConfigUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.util.Properties;


/**
 * 从 rabbitmq 读取数据
 */
public class Main {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        Properties props = KafkaConfigUtil.buildKafkaProps(parameterTool);

        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig
                .Builder().setHost("localhost").setPort(5000).build();

        DataStreamSource<String> zhisheng = env.addSource(new RMQSource<>(connectionConfig,
                "zhisheng",
                true,
                new SimpleStringSchema()))
                .setParallelism(1);
        zhisheng.print();

        //如果想保证 exactly-once 或 at-least-once 需要把 checkpoint 开启
        env.enableCheckpointing(10000);
        env.execute("flink learning connectors rabbitmq");
    }
}
