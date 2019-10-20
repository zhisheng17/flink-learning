package com.zhisheng.state.operator.state.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * @author fanrui
 * @date 2019-10-10 12:29:47
 * @desc 用于给 UnionListStateExample 生成数据
 */
public class UnionListStateUtil {
    public static final String broker_list = "192.168.30.215:9092,192.168.30.216:9092,192.168.30.220:9092";
    /**
     * kafka topic，Flink 程序中需要和这个统一
     */
    public static final String topic = "app-topic";

    public static void writeToKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        // 生成 0~9 的随机数做为 appId
        for(int i = 0; i<5; i++){
            String value = "" + new Random().nextInt(10);
            ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, value);
            producer.send(record);
        }

        System.out.println("发送数据: " );
        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            Thread.sleep(1000);
            writeToKafka();
        }
    }
}
