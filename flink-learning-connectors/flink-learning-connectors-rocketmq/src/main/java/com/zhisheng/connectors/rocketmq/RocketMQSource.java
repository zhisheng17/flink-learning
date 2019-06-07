package com.zhisheng.connectors.rocketmq;

import com.zhisheng.connectors.rocketmq.common.serialization.KeyValueDeserializationSchema;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.rocketmq.client.consumer.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.zhisheng.connectors.rocketmq.RocketMQConfig.*;
import static com.zhisheng.connectors.rocketmq.RocketMQUtils.getInteger;
import static com.zhisheng.connectors.rocketmq.RocketMQUtils.getLong;

/**
 * Desc:
 * Created by zhisheng on 2019-06-05
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class RocketMQSource <OUT> extends RichParallelSourceFunction<OUT>
        implements CheckpointedFunction, ResultTypeQueryable<OUT> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RocketMQSource.class);


    private transient MQPullConsumerScheduleService pullConsumerScheduleService;
    private DefaultMQPullConsumer consumer;

    private KeyValueDeserializationSchema<OUT> schema;

    private RunningChecker runningChecker;

    private transient ListState<Tuple2<MessageQueue, Long>> unionOffsetStates;
    private Map<MessageQueue, Long> offsetTable;
    private Map<MessageQueue, Long> restoredOffsets;

    private Properties props;
    private String topic;
    private String group;

    private static final String OFFSETS_STATE_NAME = "topic-partition-offset-states";

    private transient volatile boolean restored;

    public RocketMQSource(KeyValueDeserializationSchema<OUT> schema, Properties props) {
        this.schema = schema;
        this.props = props;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.debug("source open....");
        Validate.notEmpty(props, "Consumer properties can not be empty");
        Validate.notNull(schema, "KeyValueDeserializationSchema can not be null");

        this.topic = props.getProperty(RocketMQConfig.CONSUMER_TOPIC);
        this.group = props.getProperty(RocketMQConfig.CONSUMER_GROUP);

        Validate.notEmpty(topic, "Consumer topic can not be empty");
        Validate.notEmpty(group, "Consumer group can not be empty");

        if (offsetTable == null) {
            offsetTable = new ConcurrentHashMap<>();
        }
        if (restoredOffsets == null) {
            restoredOffsets = new ConcurrentHashMap<>();
        }

        runningChecker = new RunningChecker();

        pullConsumerScheduleService = new MQPullConsumerScheduleService(group);
        consumer = pullConsumerScheduleService.getDefaultMQPullConsumer();

        consumer.setInstanceName(String.valueOf(getRuntimeContext().getIndexOfThisSubtask()));
        RocketMQConfig.buildConsumerConfigs(props, consumer);
    }

    @Override
    public void run(SourceContext<OUT> context) throws Exception {
        LOG.debug("source run....");
        // The lock that guarantees that record emission and state updates are atomic,
        // from the view of taking a checkpoint.
        final Object lock = context.getCheckpointLock();

        int delayWhenMessageNotFound = getInteger(props, RocketMQConfig.CONSUMER_DELAY_WHEN_MESSAGE_NOT_FOUND,
                RocketMQConfig.DEFAULT_CONSUMER_DELAY_WHEN_MESSAGE_NOT_FOUND);

        String tag = props.getProperty(RocketMQConfig.CONSUMER_TAG, RocketMQConfig.DEFAULT_CONSUMER_TAG);

        int pullPoolSize = getInteger(props, RocketMQConfig.CONSUMER_PULL_POOL_SIZE,
                RocketMQConfig.DEFAULT_CONSUMER_PULL_POOL_SIZE);

        int pullBatchSize = getInteger(props, RocketMQConfig.CONSUMER_BATCH_SIZE,
                RocketMQConfig.DEFAULT_CONSUMER_BATCH_SIZE);

        pullConsumerScheduleService.setPullThreadNums(pullPoolSize);
        pullConsumerScheduleService.registerPullTaskCallback(topic, new PullTaskCallback() {

            @Override
            public void doPullTask(MessageQueue mq, PullTaskContext pullTaskContext) {
                try {
                    long offset = getMessageQueueOffset(mq);
                    if (offset < 0) {
                        return;
                    }

                    PullResult pullResult = consumer.pull(mq, tag, offset, pullBatchSize);
                    boolean found = false;
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            List<MessageExt> messages = pullResult.getMsgFoundList();
                            for (MessageExt msg : messages) {
                                byte[] key = msg.getKeys() != null ? msg.getKeys().getBytes(StandardCharsets.UTF_8) : null;
                                byte[] value = msg.getBody();
                                OUT data = schema.deserializeKeyAndValue(key, value);

                                // output and state update are atomic
                                synchronized (lock) {
                                    context.collectWithTimestamp(data, msg.getBornTimestamp());
                                }
                            }
                            found = true;
                            break;
                        case NO_MATCHED_MSG:
                            LOG.debug("No matched message after offset {} for queue {}", offset, mq);
                            break;
                        case NO_NEW_MSG:
                            break;
                        case OFFSET_ILLEGAL:
                            LOG.warn("Offset {} is illegal for queue {}", offset, mq);
                            break;
                        default:
                            break;
                    }

                    synchronized (lock) {
                        putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                    }

                    if (found) {
                        pullTaskContext.setPullNextDelayTimeMillis(0); // no delay when messages were found
                    } else {
                        pullTaskContext.setPullNextDelayTimeMillis(delayWhenMessageNotFound);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try {
            pullConsumerScheduleService.start();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }

        runningChecker.setRunning(true);

        awaitTermination();
    }

    private void awaitTermination() throws InterruptedException {
        while (runningChecker.isRunning()) {
            Thread.sleep(50);
        }
    }

    private long getMessageQueueOffset(MessageQueue mq) throws MQClientException {
        Long offset = offsetTable.get(mq);
        // restoredOffsets(unionOffsetStates) is the restored global union state;
        // should only snapshot mqs that actually belong to us
        if (restored && offset == null) {
            offset = restoredOffsets.get(mq);
        }
        if (offset == null) {
            offset = consumer.fetchConsumeOffset(mq, false);
            if (offset < 0) {
                String initialOffset = props.getProperty(RocketMQConfig.CONSUMER_OFFSET_RESET_TO, CONSUMER_OFFSET_LATEST);
                switch (initialOffset) {
                    case CONSUMER_OFFSET_EARLIEST:
                        offset = consumer.minOffset(mq);
                        break;
                    case CONSUMER_OFFSET_LATEST:
                        offset = consumer.maxOffset(mq);
                        break;
                    case CONSUMER_OFFSET_TIMESTAMP:
                        offset = consumer.searchOffset(mq, getLong(props,
                                RocketMQConfig.CONSUMER_OFFSET_FROM_TIMESTAMP, System.currentTimeMillis()));
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown value for CONSUMER_OFFSET_RESET_TO.");
                }
            }
        }
        offsetTable.put(mq, offset);
        return offsetTable.get(mq);
    }

    private void putMessageQueueOffset(MessageQueue mq, long offset) throws MQClientException {
        offsetTable.put(mq, offset);
        consumer.updateConsumeOffset(mq, offset);
    }

    @Override
    public void cancel() {
        LOG.debug("cancel ...");
        runningChecker.setRunning(false);

        if (pullConsumerScheduleService != null) {
            pullConsumerScheduleService.shutdown();
        }

        offsetTable.clear();
        restoredOffsets.clear();
    }

    @Override
    public void close() throws Exception {
        LOG.debug("close ...");
        try {
            cancel();
        } finally {
            super.close();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // called when a snapshot for a checkpoint is requested

        if (!runningChecker.isRunning()) {
            LOG.debug("snapshotState() called on closed source; returning null.");
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Snapshotting state {} ...", context.getCheckpointId());
        }

        unionOffsetStates.clear();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Snapshotted state, last processed offsets: {}, checkpoint id: {}, timestamp: {}",
                    offsetTable, context.getCheckpointId(), context.getCheckpointTimestamp());
        }

        // remove the unassigned queues in order to avoid read the wrong offset when the source restart
        Set<MessageQueue> assignedQueues = consumer.fetchMessageQueuesInBalance(topic);
        offsetTable.entrySet().removeIf(item -> !assignedQueues.contains(item.getKey()));

        for (Map.Entry<MessageQueue, Long> entry : offsetTable.entrySet()) {
            unionOffsetStates.add(Tuple2.of(entry.getKey(), entry.getValue()));
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // called every time the user-defined function is initialized,
        // be that when the function is first initialized or be that
        // when the function is actually recovering from an earlier checkpoint.
        // Given this, initializeState() is not only the place where different types of state are initialized,
        // but also where state recovery logic is included.
        LOG.debug("initialize State ...");
        this.unionOffsetStates = context.getOperatorStateStore().getUnionListState(new ListStateDescriptor<>(
                OFFSETS_STATE_NAME, TypeInformation.of(new TypeHint<Tuple2<MessageQueue, Long>>() {
        })));

        this.restored = context.isRestored();

        if (restored) {
            if (restoredOffsets == null) {
                restoredOffsets = new ConcurrentHashMap<>();
            }
            for (Tuple2<MessageQueue, Long> mqOffsets : unionOffsetStates.get()) {
                if (!restoredOffsets.containsKey(mqOffsets.f0) || restoredOffsets.get(mqOffsets.f0) < mqOffsets.f1) {
                    restoredOffsets.put(mqOffsets.f0, mqOffsets.f1);
                }
            }
            LOG.info("Setting restore state in the consumer. Using the following offsets: {}", restoredOffsets);
        } else {
            LOG.info("No restore state for the consumer.");
        }
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return schema.getProducedType();
    }
}
