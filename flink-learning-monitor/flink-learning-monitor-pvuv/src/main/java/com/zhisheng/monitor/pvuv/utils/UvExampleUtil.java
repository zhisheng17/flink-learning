package com.zhisheng.monitor.pvuv.utils;

import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.monitor.pvuv.model.UserVisitWebEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * @author fanrui
 * @date 2019-10-25 13:21:11
 * @desc 用于给统计 UV 的案例生成数据
 */
public class UvExampleUtil {
    public static final String broker_list = "192.168.30.215:9092,192.168.30.216:9092,192.168.30.220:9092";

    /**
     * kafka topic，Flink 程序中需要和这个统一
     */
    public static final String topic = "user-visit-log-topic";

    public static final Random random = new Random();

    public static void writeToKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        // 生成 0~9 的随机数做为 appId
        for(int i = 0; i<10; i++){
            String yyyyMMdd = new DateTime(System.currentTimeMillis()).toString("yyyyMMdd");
            int pageId = random.nextInt(10);    // 随机生成页面 id
            int userId = random.nextInt(100);   // 随机生成用户 id

            UserVisitWebEvent userVisitWebEvent = UserVisitWebEvent.builder()
                    .id(UUID.randomUUID().toString())   // 日志的唯一 id
                    .date(yyyyMMdd)                     // 日期
                    .pageId(pageId)                     // 页面 id
                    .userId(Integer.toString(userId))   // 用户 id
                    .url("url/" + pageId)               // 页面的 url
                    .build();
            // 对象序列化为 JSON 发送到 Kafka
            ProducerRecord record = new ProducerRecord<String, String>(topic,
                    null, null, GsonUtil.toJson(userVisitWebEvent));
            producer.send(record);
            System.out.println("发送数据: " + GsonUtil.toJson(userVisitWebEvent));
        }
        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            Thread.sleep(100);
            writeToKafka();
        }
    }
}
