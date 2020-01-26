package com.zhisheng.state.operator.state;


import com.zhisheng.state.operator.state.util.UnionListStateUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * @author fanrui
 * @date 2019-10-10 11:30:15
 * @desc 通过本案例可能看到 getUnionListState() 和 getListState() 的区别
 * 通过 UnionListStateUtil 类来生产数据
 */
public class UnionListStateExample {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1 分钟一次CheckPoint
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(15));
        env.setParallelism(3);

        CheckpointConfig checkpointConf = env.getCheckpointConfig();
        // CheckPoint 语义 EXACTLY ONCE
        checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, UnionListStateUtil.broker_list);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "app-pv-stat");

        FlinkKafkaConsumer011<String> kafkaConsumer011 = new FlinkKafkaConsumer011<>(
                // kafka topic， String 序列化
                UnionListStateUtil.topic, new SimpleStringSchema(), props);

        env.addSource(kafkaConsumer011)
                .uid(UnionListStateUtil.topic)
                .addSink(new MySink())
                .uid("MySink")
                .name("MySink");

        env.execute("Flink unionListState");
    }
}


/**
 * 当并行度改变后，getListState 恢复策略是均匀分配，
 * 将 ListState 中保存的所有元素均匀地分配到所有并行度中，每个 subtask 获取到其中一部分状态信息。
 * getUnionListState 策略是将所有的状态信息合并后，每个 subtask 都获取到全量的状态信息。
 */
class MySink extends RichSinkFunction implements CheckpointedFunction {

    private ListState<Tuple2<Integer, Long>> unionListState;

    private ListState<Tuple2<Integer, Long>> listState;

    private int subtaskIndex = 0;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void invoke(Object value, Context context) throws Exception {
    }


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        unionListState.clear();
        unionListState.add(Tuple2.of(subtaskIndex, context.getCheckpointId()));

        listState.clear();
        listState.add(Tuple2.of(subtaskIndex, context.getCheckpointId()));
        System.out.println("snapshotState  subtask: " + subtaskIndex + " --  CheckPointId: " + context.getCheckpointId());
    }


    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        // 通过 getUnionListState 获取 ListState
        unionListState = context.getOperatorStateStore().getUnionListState(
                new ListStateDescriptor<>("unionListState",
                        TypeInformation.of(new TypeHint<Tuple2<Integer, Long>>() {
                        })));

        // 通过 getListState 获取 ListState
        listState = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>("listState",
                        TypeInformation.of(new TypeHint<Tuple2<Integer, Long>>() {
                        })));

        System.out.println("subtask: " + subtaskIndex + "  start restore state");

        if (context.isRestored()) {
            TreeMap<Integer, Long> restoredUnionListState = new TreeMap<>();
            for (Tuple2<Integer, Long> indexOfSubtaskState : unionListState.get()) {
                restoredUnionListState.put(indexOfSubtaskState.f0, indexOfSubtaskState.f1);
                System.out.println("restore UnionListState  currentSubtask: " + subtaskIndex + " restoreSubtask "
                        + indexOfSubtaskState.f0 + " restoreCheckPointId " + indexOfSubtaskState.f1);
            }

            TreeMap<Integer, Long> restoredListState = new TreeMap<>();
            for (Tuple2<Integer, Long> indexOfSubtaskState : listState.get()) {
                restoredListState.put(indexOfSubtaskState.f0, indexOfSubtaskState.f1);
                System.out.println("restore ListState  currentSubtask: " + subtaskIndex + " restoreSubtask "
                        + indexOfSubtaskState.f0 + " restoreCheckPointId " + indexOfSubtaskState.f1);
            }
        }

        System.out.println("subtask: " + subtaskIndex + "  complete restore");
    }
}
