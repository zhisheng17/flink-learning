package com.zhisheng.project.deduplication;


import com.google.common.hash.Hashing;
import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.project.deduplication.model.UserVisitWebEvent;
import com.zhisheng.project.deduplication.utils.DeduplicationExampleUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;

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

        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs:///flink/checkpoints", enableIncrementalCheckpointing);
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
            .map(string -> GsonUtil.fromJson(string, UserVisitWebEvent.class))  // 反序列化 JSON
            // 这里将日志的主键 id 通过 murmur3_128 hash 后，将生成 long 类型数据当做 key
            .keyBy((KeySelector<UserVisitWebEvent, Long>) log ->
                    Hashing.murmur3_128(5).hashUnencodedChars(log.getId()).asLong())
            .addSink(new KeyedStateDeduplication.KeyedStateSink());

        env.execute("TuningKeyedStateDeduplication");
    }
}
