package com.zhisheng.connectors.rabbitmq.utils;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class RabbitMQProducerUtil {
    public final static String QUEUE_NAME = "zhisheng";

    public static void main(String[] args) throws Exception {
        //创建连接工厂
        ConnectionFactory factory = new ConnectionFactory();

        //设置RabbitMQ相关信息
        factory.setHost("localhost");
        factory.setUsername("admin");
        factory.setPassword("admin");
        factory.setPort(5672);

        //创建一个新的连接
        Connection connection = factory.newConnection();

        //创建一个通道
        Channel channel = connection.createChannel();

        // 声明一个队列
//        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        //发送消息到队列中
        String message = "Hello zhisheng";

        for (int i = 0; i < 1000; i++) {
            channel.basicPublish("", QUEUE_NAME, null, (message + i).getBytes("UTF-8"));
            System.out.println("Producer Send +'" + message + i);
        }

        //关闭通道和连接
        channel.close();
        connection.close();
    }
}
