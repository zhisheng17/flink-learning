package com.zhisheng.connectors.kinesis;

import com.zhisheng.common.utils.ExecutionEnvUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * Desc:
 * Created by zhisheng on 2019/11/24 上午10:55
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class KinesisConsumeFromDynamoDBStreams {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);

        Properties dynamodbStreamsConsumerConfig = new Properties();
        final String streamName = parameterTool.getRequired("stream.name");

        dynamodbStreamsConsumerConfig.setProperty("aws.region", parameterTool.getRequired("aws.region"));
        dynamodbStreamsConsumerConfig.setProperty("aws.credentials.provider.basic.accesskeyid", parameterTool.getRequired("aws.accesskey"));
        dynamodbStreamsConsumerConfig.setProperty("aws.credentials.provider.basic.secretkey", parameterTool.getRequired("aws.secretkey"));


        DataStream<String> dynamodbStreams = env.addSource(new FlinkDynamoDBStreamsConsumer<>(
                streamName,
                new SimpleStringSchema(),
                dynamodbStreamsConsumerConfig));

        dynamodbStreams.print();

        env.execute();
    }
}
