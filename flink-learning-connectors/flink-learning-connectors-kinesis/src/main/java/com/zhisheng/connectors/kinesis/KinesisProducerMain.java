package com.zhisheng.connectors.kinesis;

import com.zhisheng.common.utils.ExecutionEnvUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;

/**
 * Desc: Kinesis Producer
 * Created by zhisheng on 2019/11/24 上午10:24
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class KinesisProducerMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);

        DataStream<String> simpleStringStream = env.addSource(new EventsGenerator());


        Properties kinesisConsumerConfig = new Properties();
        kinesisConsumerConfig.setProperty("aws.region", parameterTool.getRequired("aws.region"));
        kinesisConsumerConfig.setProperty("aws.credentials.provider.basic.accesskeyid", parameterTool.getRequired("aws.accesskey"));
        kinesisConsumerConfig.setProperty("aws.credentials.provider.basic.secretkey", parameterTool.getRequired("aws.secretkey"));

        FlinkKinesisProducer<String> kinesis = new FlinkKinesisProducer<>(
                new SimpleStringSchema(), kinesisConsumerConfig);

        kinesis.setFailOnError(true);
        kinesis.setDefaultStream("zhisheng");
        kinesis.setDefaultPartition("0");

        simpleStringStream.addSink(kinesis);

        env.execute();
    }


    public static class EventsGenerator implements SourceFunction<String> {
        private boolean running = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            long seq = 0;
            while (running) {
                Thread.sleep(10);
                ctx.collect((seq++) + "-" + RandomStringUtils.randomAlphabetic(12));
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
