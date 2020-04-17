package com.zhisheng.connectors.kafka;

import com.zhisheng.common.utils.ExecutionEnvUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.time.Instant;
import java.util.Random;
import java.util.TimeZone;

/**
 * Desc: Flink 发送数据到 topic
 * Created by zhisheng on 2019-09-22
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class FlinkKafkaProducerTest1 {

    public static final Random random = new Random();

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env.setParallelism(1);
        env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> context) throws Exception {

                while (true) {
                    TimeZone tz = TimeZone.getTimeZone("Asia/Shanghai");
                    Instant instant = Instant.ofEpochMilli(System.currentTimeMillis() + tz.getOffset(System.currentTimeMillis()));

                    String outline = String.format(
                            "{\"user_id\": \"%s\", \"item_id\":\"%s\", \"category_id\": \"%s\", \"behavior\": \"%s\", \"ts\": \"%s\"}",
                            random.nextInt(10),
                            random.nextInt(100),
                            random.nextInt(1000),
                            "pv",
                            instant.toString());
                    context.collect(outline);
                    Thread.sleep(200);
                }
            }

            @Override
            public void cancel() {

            }
        })
                .addSink(new FlinkKafkaProducer011<>(
                        "localhost:9092",
                        "user_behavior",
                        new SimpleStringSchema()
                )).name("flink-connectors-kafka");

        env.execute("flink kafka connector test");
    }
}
