package com.zhisheng.connectors.kafka;

import com.zhisheng.common.model.MetricEvent;
import com.zhisheng.common.schemas.MetricSchema;
import com.zhisheng.common.utils.ExecutionEnvUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.zhisheng.common.utils.KafkaConfigUtil.buildKafkaProps;

/**
 * Desc: Flink 消费 kafka 多个 topic、Pattern 类型的 topic
 * Created by zhisheng on 2019-09-22
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class FlinkKafkaConsumerTest1 {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        Properties props = buildKafkaProps(parameterTool);
        //kafka topic list
        List<String> topics = Arrays.asList(parameterTool.get("metrics.topic"), parameterTool.get("logs.topic"));
        FlinkKafkaConsumer011<MetricEvent> consumer = new FlinkKafkaConsumer011<>(topics, new MetricSchema(), props);
        //kafka topic Pattern
        //FlinkKafkaConsumer011<MetricEvent> consumer = new FlinkKafkaConsumer011<>(java.utils.regex.Pattern.compile("test-topic-[0-9]"), new MetricSchema(), props);


//        consumer.setStartFromLatest();
//        consumer.setStartFromEarliest()
        DataStreamSource<MetricEvent> data = env.addSource(consumer);

        data.print();

        env.execute("flink kafka connector test");
    }
}
