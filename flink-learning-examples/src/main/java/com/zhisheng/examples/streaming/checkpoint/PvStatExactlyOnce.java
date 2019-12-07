package com.zhisheng.examples.streaming.checkpoint;


import com.zhisheng.examples.streaming.checkpoint.util.PvStatExactlyOnceKafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import scala.Tuple2;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author fanrui
 * @date 2019-10-03 21:05:33
 * @desc 代码中设置 1 分钟一次CheckPoint，CheckPoint 语义 EXACTLY ONCE，从 Kafka 中读取数据，
 * 这里为了简化代码，所以 Kafka 中读取的直接就是 String 类型的 appId，按照 appId KeyBy 后，执行 RichMapFunction，
 * RichMapFunction 的 open 方法中会初始化 ValueState<Long> 类型的 pvState，pvState 就是上文一直强调的状态信息，
 * 每次 CheckPoint 的时候，会把 pvState 的状态信息快照一份到 hdfs 来提供恢复。
 * 这里按照 appId 进行 keyBy，所以每一个 appId 都会对应一个 pvState，pvState 里存储着该 appId 对应的 pv 值。
 * 每来一条数据都会执行一次 map 方法，
 * 当这条数据对应的 appId 是新 app 时，pvState 里就没有存储这个 appId 当前的 pv 值，将 pv 值赋值为 1，
 * 当 pvState 里存储的 value 不为 null 时，拿出 pv 值 +1后 update 到 pvState 里。
 * map 方法再将 appId 和 pv 值发送到下游算子，下游直接调用了 print 进行输出，这里完全可以替换成相应的 RedisSink 或 HBaseSink。
 * 本案例中计算 pv 的工作交给了 Flink 内部的 ValueState，不依赖外部存储介质进行累加，外部介质承担的角色仅仅是提供数据给业务方查询，
 * 所以无论下游使用什么形式的 Sink，只要 Sink 端能够按照主键去重，该统计方案就可以保障 Exactly Once。
 */
@Slf4j
public class PvStatExactlyOnce {
    // 生产数据： kafka-console-producer --topic app-topic --broker-list 192.168.30.215:9092,192.168.30.216:9092,192.168.30.220:9092

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1 分钟一次CheckPoint
        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(1));
        env.setParallelism(2);

        CheckpointConfig checkpointConf = env.getCheckpointConfig();
        // CheckPoint 语义 EXACTLY ONCE
        checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "app-pv-stat");

        DataStreamSource<String> appInfoSource = env.addSource(new FlinkKafkaConsumer011<>(
                // kafka topic， String 序列化
                PvStatExactlyOnceKafkaUtil.topic, new SimpleStringSchema(), props));

        // 按照 appId 进行 keyBy
        appInfoSource.keyBy((KeySelector<String, String>) appId -> appId)
                .map(new RichMapFunction<String, Tuple2<String, Long>>() {
                    private ValueState<Long> pvState;
                    private long pv = 0;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 初始化状态
                        pvState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("pvStat",
                                        TypeInformation.of(new TypeHint<Long>() {
                                        })));
                    }

                    @Override
                    public Tuple2<String, Long> map(String appId) throws Exception {
                        // 从状态中获取该 app 的pv值，+1后，update 到状态中
                        if (null == pvState.value()) {
                            log.info("{} is new, pv is 1", appId);
                            pv = 1;
                        } else {
                            pv = pvState.value();
                            pv += 1;
                            log.info("{} is old , pv is {}", appId, pv);
                        }
                        pvState.update(pv);
                        return new Tuple2<>(appId, pv);
                    }
                })
                .print();

        env.execute("Flink pv stat");
    }
}
