package com.zhisheng.connectors.gcp.pubsub;

import com.zhisheng.common.constant.PropertiesConstants;
import com.zhisheng.common.utils.ExecutionEnvUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSink;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource;

/**
 * Desc: Flink connector gcp pubsub test
 * Created by zhisheng on 2019/11/23 下午9:16
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class Main {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);

        if (parameterTool.getNumberOfParameters() < 4) {
            System.out.println("Missing parameters!\n" +
                    "Usage: flink run PubSub.jar --input-subscription <subscription> --input-topicName <topic> --output-topicName <output-topic> " +
                    "--google-project <google project name> ");
            return;
        }

        String projectName = parameterTool.getRequired("stream.project.name");
        String inputTopicName = parameterTool.getRequired("stream.input.topicName");
        String subscriptionName = parameterTool.getRequired("stream.input.subscription");
        String outputTopicName = parameterTool.getRequired("stream.output.topicName");

        PubSubPublisherUtil pubSubPublisher = new PubSubPublisherUtil(projectName, inputTopicName);
        pubSubPublisher.publish(10);


        env.addSource(PubSubSource.newBuilder()
                .withDeserializationSchema(new IntegerSerializer())
                .withProjectName(projectName)
                .withSubscriptionName(subscriptionName)
                .withMessageRateLimit(1)
                .build())
                .map(Main::printAndReturn).disableChaining()
                .addSink(PubSubSink.newBuilder()
                        .withSerializationSchema(new IntegerSerializer())
                        .withProjectName(projectName)
                        .withTopicName(outputTopicName).build());
        env.enableCheckpointing(parameterTool.getLong(PropertiesConstants.STREAM_CHECKPOINT_INTERVAL, 1000L));
        env.execute("Flink connector gcp pubsub test");
    }

    private static Integer printAndReturn(Integer i) {
        log.info("Processed message with payload: " + i);
        return i;
    }
}
