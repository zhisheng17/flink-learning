package com.zhisheng.project.scaffold;

import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.project.common.constant.ProjectConstants;
import com.zhisheng.project.common.model.ServerMetric;
import com.zhisheng.project.common.utils.ProjectKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Apache Flink 实时作业脚手架
 *
 * <p>本作业展示了生产环境 Flink 作业的最佳实践，包含：
 * <ul>
 *     <li>Checkpoint 配置（Exactly-Once、间隔、超时、保留策略）</li>
 *     <li>重启策略（固定延迟重启、指数退避）</li>
 *     <li>Watermark 策略（处理乱序数据、允许延迟）</li>
 *     <li>Keyed State 使用（ValueState 实现去重/缓存）</li>
 *     <li>自定义 AggregateFunction（增量聚合计算）</li>
 *     <li>窗口操作（Tumbling Window + Event Time）</li>
 *     <li>从 Kafka 消费数据</li>
 * </ul>
 *
 * <p>使用方式：将此类作为新 Flink 作业的模板，根据业务需求修改数据源、处理逻辑和输出
 *
 * @author zhisheng
 */
public class FlinkJobScaffold {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkJobScaffold.class);

    public static void main(String[] args) throws Exception {

        // ===================== 1. 创建执行环境 =====================
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ===================== 2. 配置 Checkpoint =====================
        // 开启 Checkpoint，间隔 60 秒
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(60));
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 设置 Exactly-Once 语义，确保数据精确一次处理
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 两次 Checkpoint 之间的最小间隔为 30 秒，防止 Checkpoint 过于频繁
        checkpointConfig.setMinPauseBetweenCheckpoints(TimeUnit.SECONDS.toMillis(30));
        // Checkpoint 超时时间为 10 分钟
        checkpointConfig.setCheckpointTimeout(TimeUnit.MINUTES.toMillis(10));
        // 同时只允许 1 个 Checkpoint 在进行
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 作业取消时保留 Checkpoint，用于手动恢复
        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // ===================== 3. 配置重启策略 =====================
        // 固定延迟重启策略：最多重启 3 次，每次间隔 10 秒
        env.setRestartStrategy(
                org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart(
                        3, org.apache.flink.api.common.time.Time.seconds(10)));

        // ===================== 4. 设置并行度 =====================
        env.setParallelism(4);

        // ===================== 5. 配置 Kafka Source =====================
        KafkaSource<String> kafkaSource = ProjectKafkaUtil.buildKafkaStringSource(
                ProjectConstants.TOPIC_METRIC,
                "scaffold-group");

        // ===================== 6. 配置 Watermark 策略 =====================
        // 允许最大 5 秒的乱序，用于处理网络延迟等导致的数据乱序
        WatermarkStrategy<ServerMetric> watermarkStrategy = WatermarkStrategy
                .<ServerMetric>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<ServerMetric>) (event, recordTimestamp) ->
                                event.getTimestamp())
                // 如果某个分区超过 30 秒没有数据，标记为 idle，避免阻塞 Watermark 推进
                .withIdleness(Duration.ofSeconds(30));

        // ===================== 7. 构建数据处理流水线 =====================
        DataStream<ServerMetric> metricStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                // 反序列化 JSON 为 ServerMetric 对象
                .map(json -> GsonUtil.fromJson(json, ServerMetric.class))
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // 7.1 使用 Keyed State 进行数据去重/过滤
        DataStream<ServerMetric> dedupStream = metricStream
                .keyBy(ServerMetric::getHost)
                .flatMap(new DeduplicateFunction());

        // 7.2 按照主机和指标名称分组，使用滚动窗口计算每分钟平均值
        DataStream<Tuple2<String, Double>> avgMetricStream = dedupStream
                .keyBy(metric -> metric.getHost() + "_" + metric.getMetricName())
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new MetricAvgAggregateFunction());

        // 7.3 输出结果（生产环境可替换为写入 Kafka、ES、MySQL 等）
        avgMetricStream.print("avg-metric");

        // ===================== 8. 启动作业 =====================
        env.execute("Flink Job Scaffold - 生产级作业模板");
    }

    /**
     * 去重函数：使用 ValueState 记录上次处理的数据时间戳，
     * 过滤掉重复或过期的数据
     *
     * <p>知识点：
     * <ul>
     *     <li>RichFlatMapFunction 可以访问 RuntimeContext，用于操作 Keyed State</li>
     *     <li>ValueState 是最基本的 Keyed State，存储单个值</li>
     *     <li>open() 方法在算子初始化时调用，用于注册 State</li>
     * </ul>
     */
    public static class DeduplicateFunction extends RichFlatMapFunction<ServerMetric, ServerMetric> {
        // 记录每个 key 上次处理的时间戳
        private transient ValueState<Long> lastTimestampState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                    "last-timestamp", Long.class);
            lastTimestampState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(ServerMetric value, Collector<ServerMetric> out) throws Exception {
            Long lastTimestamp = lastTimestampState.value();
            // 如果是第一条数据或者时间戳比上次更新，则输出
            if (lastTimestamp == null || value.getTimestamp() > lastTimestamp) {
                lastTimestampState.update(value.getTimestamp());
                out.collect(value);
            } else {
                LOG.debug("过滤重复数据: host={}, metric={}, timestamp={}",
                        value.getHost(), value.getMetricName(), value.getTimestamp());
            }
        }
    }

    /**
     * 指标平均值聚合函数
     *
     * <p>知识点：
     * <ul>
     *     <li>AggregateFunction 是增量聚合函数，内存占用小，适合大数据量场景</li>
     *     <li>与 ReduceFunction 相比，AggregateFunction 的输入、中间状态、输出类型可以不同</li>
     *     <li>ACC (累加器) 存储中间状态：(sum, count)</li>
     *     <li>getResult() 在窗口触发时调用，计算最终结果</li>
     *     <li>merge() 用于会话窗口的合并</li>
     * </ul>
     */
    public static class MetricAvgAggregateFunction
            implements AggregateFunction<ServerMetric, Tuple2<Double, Long>, Tuple2<String, Double>> {

        @Override
        public Tuple2<Double, Long> createAccumulator() {
            // 初始化累加器：(sum=0.0, count=0)
            return Tuple2.of(0.0, 0L);
        }

        @Override
        public Tuple2<Double, Long> add(ServerMetric value, Tuple2<Double, Long> accumulator) {
            // 累加：sum += value, count += 1
            return Tuple2.of(accumulator.f0 + value.getValue(), accumulator.f1 + 1);
        }

        @Override
        public Tuple2<String, Double> getResult(Tuple2<Double, Long> accumulator) {
            // 计算平均值
            double avg = accumulator.f1 > 0 ? accumulator.f0 / accumulator.f1 : 0.0;
            return Tuple2.of("avg", avg);
        }

        @Override
        public Tuple2<Double, Long> merge(Tuple2<Double, Long> a, Tuple2<Double, Long> b) {
            // 合并两个累加器（会话窗口场景）
            return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
        }
    }
}
