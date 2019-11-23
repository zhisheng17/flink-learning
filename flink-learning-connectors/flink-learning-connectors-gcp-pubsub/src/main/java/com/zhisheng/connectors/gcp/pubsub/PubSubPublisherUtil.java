package com.zhisheng.connectors.gcp.pubsub;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import lombok.Data;

import java.math.BigInteger;

/**
 * Desc: send PubSubMessages to a PubSub topic
 * Created by zhisheng on 2019/11/23 下午9:25
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Data
public class PubSubPublisherUtil {

    private final String projectName;
    private final String topicName;

    /**
     * Publish messages with as payload a single integer.
     * The integers inside the messages start from 0 and increase by one for each message send.
     *
     * @param messagesCount message count
     */
    void publish(int messagesCount) {
        Publisher publisher = null;
        try {
            publisher = Publisher.newBuilder(ProjectTopicName.of(projectName, topicName)).build();
            for (int i = 0; i < messagesCount; i++) {
                ByteString messageData = ByteString.copyFrom(BigInteger.valueOf(i).toByteArray());
                PubsubMessage message = PubsubMessage.newBuilder().setData(messageData).build();
                publisher.publish(message).get();

                System.out.println("Published message: " + i);
                Thread.sleep(100L);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (publisher != null) {
                    publisher.shutdown();
                }
            } catch (Exception e) {
            }
        }
    }



}
