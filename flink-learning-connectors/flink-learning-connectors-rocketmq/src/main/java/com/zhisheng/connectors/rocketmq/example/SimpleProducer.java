package com.zhisheng.connectors.rocketmq.example;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

/**
 * Desc:
 * Created by zhisheng on 2019-06-07
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class SimpleProducer {
    public static void main(String[] args) {
        DefaultMQProducer producer = new DefaultMQProducer("p001");
        producer.setNamesrvAddr("localhost:9876");
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < 10000; i++) {
            Message msg = new Message("zhisheng", "", "id_" + i, ("country_X province_" + i).getBytes());
            try {
                producer.send(msg);
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("send " + i);
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
