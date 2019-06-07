package com.zhisheng.connectors.rocketmq;

import com.zhisheng.connectors.rocketmq.common.selector.TopicSelector;
import com.zhisheng.connectors.rocketmq.common.serialization.KeyValueSerializationSchema;
import org.apache.commons.lang3.Validate;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Desc:
 * Created by zhisheng on 2019-06-07
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class RocketMQSink<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSink.class);

    private transient DefaultMQProducer producer;
    private boolean async; // false by default

    private Properties props;
    private TopicSelector<IN> topicSelector;
    private KeyValueSerializationSchema<IN> serializationSchema;

    private boolean batchFlushOnCheckpoint; // false by default
    private int batchSize = 1000;
    private List<Message> batchList;

    private int messageDeliveryDelayLevel = RocketMQConfig.MSG_DELAY_LEVEL00;


    public RocketMQSink(KeyValueSerializationSchema<IN> schema, TopicSelector<IN> topicSelector, Properties props) {
        this.serializationSchema = schema;
        this.topicSelector = topicSelector;
        this.props = props;

        if (this.props != null) {
            this.messageDeliveryDelayLevel = RocketMQUtils.getInteger(this.props, RocketMQConfig.MSG_DELAY_LEVEL,
                    RocketMQConfig.MSG_DELAY_LEVEL00);
            if (this.messageDeliveryDelayLevel < RocketMQConfig.MSG_DELAY_LEVEL00) {
                this.messageDeliveryDelayLevel = RocketMQConfig.MSG_DELAY_LEVEL00;
            } else if (this.messageDeliveryDelayLevel > RocketMQConfig.MSG_DELAY_LEVEL18) {
                this.messageDeliveryDelayLevel = RocketMQConfig.MSG_DELAY_LEVEL18;
            }
        }
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        Validate.notEmpty(props, "Producer properties can not be empty");
        Validate.notNull(topicSelector, "TopicSelector can not be null");
        Validate.notNull(serializationSchema, "KeyValueSerializationSchema can not be null");

        producer = new DefaultMQProducer();
        producer.setInstanceName(String.valueOf(getRuntimeContext().getIndexOfThisSubtask()));
        RocketMQConfig.buildProducerConfigs(props, producer);

        batchList = new LinkedList<>();

        if (batchFlushOnCheckpoint && !((StreamingRuntimeContext) getRuntimeContext()).isCheckpointingEnabled()) {
            LOG.warn("Flushing on checkpoint is enabled, but checkpointing is not enabled. Disabling flushing.");
            batchFlushOnCheckpoint = false;
        }

        try {
            producer.start();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void invoke(IN input, Context context) throws Exception {
        Message msg = prepareMessage(input);

        if (batchFlushOnCheckpoint) {
            batchList.add(msg);
            if (batchList.size() >= batchSize) {
                flushSync();
            }
            return;
        }

        if (async) {
            try {
                producer.send(msg, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        LOG.debug("Async send message success! result: {}", sendResult);
                    }

                    @Override
                    public void onException(Throwable throwable) {
                        if (throwable != null) {
                            LOG.error("Async send message failure!", throwable);
                        }
                    }
                });
            } catch (Exception e) {
                LOG.error("Async send message failure!", e);
            }
        } else {
            try {
                SendResult result = producer.send(msg);
                LOG.debug("Sync send message result: {}", result);
            } catch (Exception e) {
                LOG.error("Sync send message failure!", e);
            }
        }
    }

    /**
     * 解析消息
     *
     * @param input
     * @return
     */
    private Message prepareMessage(IN input) {
        String topic = topicSelector.getTopic(input);
        String tag = topicSelector.getTag(input) != null ? topicSelector.getTag(input) : "";

        byte[] k = serializationSchema.serializeKey(input);
        String key = k != null ? new String(k, StandardCharsets.UTF_8) : "";
        byte[] value = serializationSchema.serializeValue(input);

        Validate.notNull(topic, "the message topic is null");
        Validate.notNull(value, "the message body is null");

        Message msg = new Message(topic, tag, key, value);
        if (this.messageDeliveryDelayLevel > RocketMQConfig.MSG_DELAY_LEVEL00) {
            msg.setDelayTimeLevel(this.messageDeliveryDelayLevel);
        }
        return msg;
    }

    public RocketMQSink<IN> withAsync(boolean async) {
        this.async = async;
        return this;
    }

    public RocketMQSink<IN> withBatchFlushOnCheckpoint(boolean batchFlushOnCheckpoint) {
        this.batchFlushOnCheckpoint = batchFlushOnCheckpoint;
        return this;
    }

    public RocketMQSink<IN> withBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    @Override
    public void close() throws Exception {
        if (producer != null) {
            flushSync();
            producer.shutdown();
        }
    }

    private void flushSync() throws Exception {
        if (batchFlushOnCheckpoint) {
            synchronized (batchList) {
                if (batchList.size() > 0) {
                    producer.send(batchList);
                    batchList.clear();
                }
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        flushSync();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // Nothing to do
    }
}
