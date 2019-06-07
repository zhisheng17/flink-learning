package com.zhisheng.connectors.rocketmq.example;

import com.zhisheng.connectors.rocketmq.RocketMQConfig;
import com.zhisheng.connectors.rocketmq.RocketMQSink;
import com.zhisheng.connectors.rocketmq.RocketMQSource;
import com.zhisheng.connectors.rocketmq.common.selector.DefaultTopicSelector;
import com.zhisheng.connectors.rocketmq.common.serialization.SimpleKeyValueDeserializationSchema;
import com.zhisheng.connectors.rocketmq.common.serialization.SimpleKeyValueSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Desc: 从 RocketMQ 中获取数据后写入到 RocketMQ
 * Created by zhisheng on 2019-06-07
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class RocketMQFlinkExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(3000);

        Properties consumerProps = new Properties();
        consumerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "localhost:9876");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_GROUP, "c002");
        consumerProps.setProperty(RocketMQConfig.CONSUMER_TOPIC, "zhisheng");


        Properties producerProps = new Properties();
        producerProps.setProperty(RocketMQConfig.NAME_SERVER_ADDR, "localhost:9876");
        int msgDelayLevel = RocketMQConfig.MSG_DELAY_LEVEL05;
        producerProps.setProperty(RocketMQConfig.MSG_DELAY_LEVEL, String.valueOf(msgDelayLevel));
        // TimeDelayLevel is not supported for batching
        boolean batchFlag = msgDelayLevel <= 0;

        env.addSource(new RocketMQSource(new SimpleKeyValueDeserializationSchema("id", "address"), consumerProps))
                .name("rocketmq-source")
                .setParallelism(2)
                .process(new ProcessFunction<Map, Map>() {
                    @Override
                    public void processElement(Map in, Context ctx, Collector<Map> out) throws Exception {
                        HashMap result = new HashMap();
                        result.put("id", in.get("id"));
                        String[] arr = in.get("address").toString().split("\\s+");
                        result.put("province", arr[arr.length - 1]);
                        out.collect(result);
                    }
                })
                .name("upper-processor")
                .setParallelism(2)
                .addSink(new RocketMQSink(new SimpleKeyValueSerializationSchema("id", "province"),
                        new DefaultTopicSelector("zhisheng"), producerProps).withBatchFlushOnCheckpoint(batchFlag))
                .name("rocketmq-sink")
                .setParallelism(2);

        env.execute("rocketmq-flink-example");
    }
}
