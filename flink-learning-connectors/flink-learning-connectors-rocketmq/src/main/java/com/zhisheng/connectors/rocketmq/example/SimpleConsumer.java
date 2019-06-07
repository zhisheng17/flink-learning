package com.zhisheng.connectors.rocketmq.example;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * Desc:
 * Created by zhisheng on 2019-06-07
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class SimpleConsumer {
    public static void main(String[] args) {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("g00003");
        consumer.setNamesrvAddr("localhost:9876");
        try {
            consumer.subscribe("zhisheng", "*");
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.println(msg.getKeys() + ":" + new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        try {
            consumer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }
}
