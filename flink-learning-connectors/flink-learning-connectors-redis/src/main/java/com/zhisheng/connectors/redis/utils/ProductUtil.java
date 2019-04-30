package com.zhisheng.connectors.redis.utils;

import com.zhisheng.common.model.ProductEvent;
import com.zhisheng.common.utils.GsonUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * Desc:
 * Created by zhisheng on 2019-04-29
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class ProductUtil {
    public static final String broker_list = "localhost:9092";
    public static final String topic = "zhisheng";  //kafka topic 需要和 flink 程序用同一个 topic

    public static final Random random = new Random();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        for (int i = 1; i <= 10000; i++) {
            ProductEvent product = ProductEvent.builder().id((long) i)  //商品的 id
                    .name("product" + i)    //商品 name
                    .price(random.nextLong() / 10000000000000L) //商品价格（以分为单位）
                    .code("code" + i).build();  //商品编码

            ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, GsonUtil.toJson(product));
            producer.send(record);
            System.out.println("发送数据: " + GsonUtil.toJson(product));
        }
        producer.flush();
    }
}
