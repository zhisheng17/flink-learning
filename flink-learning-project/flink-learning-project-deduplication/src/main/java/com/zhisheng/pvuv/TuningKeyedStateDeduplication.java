package com.zhisheng.pvuv;


import com.google.common.hash.Hashing;
import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.pvuv.model.UserVisitWebEvent;
import com.zhisheng.pvuv.utils.DeduplicationExampleUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author fanrui
 * @date 2019-11-01 01:34:13
 */
public class TuningKeyedStateDeduplication {

    private static boolean enableIncrementalCheckpointing = true;
    private static int numberOfTransferingThreads = 3;

    public static void main(String[] args) throws Exception{

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(10));
        env.setParallelism(6);

        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("", enableIncrementalCheckpointing);
        rocksDBStateBackend.setNumberOfTransferingThreads(numberOfTransferingThreads);
        rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        rocksDBStateBackend.enableTtlCompactionFilter();
        env.setStateBackend(rocksDBStateBackend);

        CheckpointConfig checkpointConf = env.getCheckpointConfig();
        checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConf.setMinPauseBetweenCheckpoints(TimeUnit.MINUTES.toMillis(8));
        checkpointConf.setCheckpointTimeout(TimeUnit.MINUTES.toMillis(20));
        checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DeduplicationExampleUtil.broker_list);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "keyed-state-deduplication");
        FlinkKafkaConsumerBase<String> kafkaConsumer = new FlinkKafkaConsumer011<>(
                DeduplicationExampleUtil.topic, new SimpleStringSchema(), props)
                .setStartFromLatest();

        env.addSource(kafkaConsumer)
                .filter((FilterFunction<String>) Objects::nonNull)
                .map(string -> GsonUtil.fromJson(string, UserVisitWebEvent.class))  // 反序列化 JSON
                .keyBy((KeySelector<UserVisitWebEvent, Long>) log -> Hashing.murmur3_128(5).hashUnencodedChars(log.getId()).asLong())
                .addSink(new RichSinkFunction<UserVisitWebEvent>() {
                    private ValueState<Boolean> isExist;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        StateTtlConfig ttlConfig = StateTtlConfig
                                .newBuilder(Time.hours(36))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .cleanupInRocksdbCompactFilter(50000000L)
                                .build();

                        ValueStateDescriptor<Boolean> keyedStateDuplicated = new ValueStateDescriptor<>("TuningKeyedStateDeduplication",
                                TypeInformation.of(new TypeHint<Boolean>() {}));
                        keyedStateDuplicated.enableTimeToLive(ttlConfig);

                        isExist = getRuntimeContext().getState(keyedStateDuplicated);
                    }

                    @Override
                    public void invoke(UserVisitWebEvent value, Context context) throws Exception {
                        // 当前 key 第一次出现，按照业务要求正常处理
                        if( null == isExist.value()) {
                            isExist.update(true);
                        } else {
                            // 当前 key 之前出现过，所以应该被过滤
                        }
                    }
                });

        env.execute("TuningKeyedStateDeduplication");
    }
}
