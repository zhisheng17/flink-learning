package com.zhisheng.examples.streaming.checkpoint;


import com.zhisheng.examples.streaming.checkpoint.util.PvStatExactlyOnceKafkaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * @author fanrui
 * @date 2019-10-20 13:45:46
 * @desc 本案例在 PvStatExactlyOnce 的基础上增加了 LocalKeyBy 的优化,
 * 优化场景如下：统计各 app 的 pv，需要有个前提，将相同 app 的数据必须发送到相同的 subtask，
 * 否则，多个 subtask 都会统计同一个 app 的数据，就会造成 pv 值计算错误。
 * 问题就在于当某个 app 比较热门时，这个app 对应的数据量较大，可能单个 subtask 根本处理不了这么多的数据
 * 或者热门 app 占整个数据量的 99%，就算计算pv 的 task 设置的并行度为 10，
 * 但是这个 app 的数据只能被分到同一个 subtask 上去处理，
 * 问题就出现了，1个 subtask 要处理 99%的数据，其余 9 个 subtask 处理 1%的数据，
 * 发生了严重的数据倾斜，怎么处理呢？ 本案例使用 LocalKeyBy 的思想来处理数据倾斜
 */
@Slf4j
public class PvStatLocalKeyByExactlyOnce {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1 分钟一次 Checkpoint
        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(1));
        env.setParallelism(2);

        CheckpointConfig checkpointConf = env.getCheckpointConfig();
        // Checkpoint 语义 EXACTLY ONCE
        checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, PvStatExactlyOnceKafkaUtil.broker_list);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "app-pv-stat");

        FlinkKafkaConsumerBase<String> appKafkaConsumer = new FlinkKafkaConsumer011<>(
                // kafka topic， String 序列化
                PvStatExactlyOnceKafkaUtil.topic, new SimpleStringSchema(), props).setStartFromLatest();


        env.addSource(appKafkaConsumer)
                .flatMap(new LocalKeyByFlatMap(10))
                // 按照 appId 进行 keyBy
                .keyBy((KeySelector<Tuple2<String, Long>, String>) appIdPv -> appIdPv.f0)
                .map(new RichMapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
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
                    public Tuple2<String, Long> map(Tuple2<String, Long> tuple2) throws Exception {
                        // 从状态中获取该 app 的pv值，加上新收到的 pv 值以后后，update 到状态中
                        if (null == pvState.value()) {
                            log.info("{} is new, PV is {}", tuple2.f0, tuple2.f1);
                            pv = tuple2.f1;
                        } else {
                            pv = pvState.value();
                            pv += tuple2.f1;
                            log.info("{} is old, PV is {}", tuple2.f0, pv);
                        }
                        pvState.update(pv);
                        tuple2.setField(pv, 1);
                        return tuple2;
                    }
                })
                .print();

        env.execute("Flink pv stat LocalKeyBy");
    }
}


/**
 * LocalKeyByFlatMap 中实现了在 shuffle 的上游端对数据进行预聚合，
 * 从而减少发送到下游的数据量，使得热点数据量大大降低。
 * 注：本案例中积攒批次使用数据量来积攒，当长时间数据量较少时，由于数据量积攒不够，
 * 可能导致上游buffer中数据不往下游发送，可以加定时策略，
 * 例如：如果数据量少但是时间超过了 200ms，也会强制将数据发送到下游
 */
@Slf4j
class LocalKeyByFlatMap extends RichFlatMapFunction<String, Tuple2<String, Long>> implements CheckpointedFunction {

    /**
     * 由于加了 buffer，所以 Checkpoint 的时候，
     * 可能还有 Checkpoint 之前的数据缓存在 buffer 中没有发送到下游被处理
     * 把这部分数据放到 localPvStatListState 中，当 Checkpoint 恢复时，
     * 把这部分数据从状态中恢复到 buffer 中
     */
    private ListState<Tuple2<String, Long>> localPvStatListState;

    /**
     * 本地 buffer，存放 local 端缓存的 app 的 pv 信息
     */
    private HashMap<String, Long> localPvStat;

    /**
     * 缓存的数据量大小，即：缓存多少数据再向下游发送
     */
    private int batchSize;

    /**
     * 计数器，获取当前批次接收的数据量
     */
    private AtomicInteger currentSize;

    private int subtaskIndex;


    LocalKeyByFlatMap(int batchSize) {
        checkArgument(batchSize >= 0,
                "Cannot define a negative value for the batchSize.");
        this.batchSize = batchSize;
    }


    @Override
    public void flatMap(String appId, Collector<Tuple2<String, Long>> collector) throws Exception {
        //  将新来的数据添加到 buffer 中
        Long pv = localPvStat.getOrDefault(appId, 0L);
        localPvStat.put(appId, pv + 1);
        log.info("invoke   subtask: {}  appId: {}   pv: {}", subtaskIndex, appId, localPvStat.get(appId));

        // 如果到达设定的批次，则将 buffer 中的数据发送到下游
        if (currentSize.incrementAndGet() >= batchSize) {
            for (Map.Entry<String, Long> appIdPv : localPvStat.entrySet()) {
                collector.collect(Tuple2.of(appIdPv.getKey(), appIdPv.getValue()));
                log.info("batchSend   subtask: {}   appId: {}   PV: {}", subtaskIndex, appIdPv.getKey(), appIdPv.getValue());
            }
            localPvStat.clear();
            currentSize.set(0);
        }
    }


    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        // 将 buffer 中的数据保存到状态中，来保证 Exactly Once
        localPvStatListState.clear();
        for (Map.Entry<String, Long> appIdPv : localPvStat.entrySet()) {
            localPvStatListState.add(Tuple2.of(appIdPv.getKey(), appIdPv.getValue()));
            log.info("snapshot   subtask: {}    appId: {}   pv: {}", subtaskIndex, appIdPv.getKey(), appIdPv.getValue());
        }
    }


    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        // 从状态中恢复 buffer 中的数据
        localPvStatListState = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>("localPvStat",
                        TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                        })));
        localPvStat = new HashMap<>();
        if (context.isRestored()) {
            // 从状态中恢复 buffer 中的数据
            for (Tuple2<String, Long> appIdPv : localPvStatListState.get()) {
                long pv = localPvStat.getOrDefault(appIdPv.f0, 0L);
                // 如果出现 pv != 0,说明改变了并行度，
                // ListState 中的数据会被均匀分发到新的 subtask中
                // 所以单个 subtask 恢复的状态中可能包含两个相同的 app 的数据
                localPvStat.put(appIdPv.f0, pv + appIdPv.f1);
                log.info("init   subtask: {}    appId: {}   pv:{}", subtaskIndex, appIdPv.f0, appIdPv.f1);
            }
            //  从状态恢复时，默认认为 buffer 中数据量达到了 batchSize，需要向下游发送数据了
            currentSize = new AtomicInteger(batchSize);
        } else {
            currentSize = new AtomicInteger(0);
        }
    }
}
