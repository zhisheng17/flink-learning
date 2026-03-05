package com.zhisheng.project.log;

import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.project.common.constant.ProjectConstants;
import com.zhisheng.project.common.utils.ProjectKafkaUtil;
import com.zhisheng.project.log.model.AppLogEvent;
import com.zhisheng.project.log.model.LogStatistics;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * 基于 Apache Flink 的实时日志分析系统
 *
 * <p>功能描述：
 * <ul>
 *     <li>从 Kafka 实时消费应用日志</li>
 *     <li>使用侧输出流（Side Output）将 ERROR 日志分流</li>
 *     <li>按服务名称和日志级别进行窗口聚合统计</li>
 *     <li>使用 AggregateFunction + ProcessWindowFunction 获取窗口信息</li>
 * </ul>
 *
 * <p>涉及知识点：
 * <ul>
 *     <li>Side Output（侧输出流）：将不同类型数据分流处理</li>
 *     <li>Event Time + Watermark：基于事件时间的窗口处理</li>
 *     <li>AggregateFunction + ProcessWindowFunction：增量聚合 + 全量窗口信息</li>
 *     <li>Tumbling Window：滚动窗口按时间切分数据</li>
 * </ul>
 *
 * @author zhisheng
 */
public class LogAnalysisJob {

    private static final Logger LOG = LoggerFactory.getLogger(LogAnalysisJob.class);

    /** ERROR 日志侧输出标签 */
    private static final OutputTag<AppLogEvent> ERROR_LOG_TAG =
            new OutputTag<AppLogEvent>("error-log") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 配置 Kafka Source
        KafkaSource<String> kafkaSource = ProjectKafkaUtil.buildKafkaStringSource(
                ProjectConstants.TOPIC_LOG, "log-analysis-group");

        // Watermark 策略：允许 3 秒乱序
        WatermarkStrategy<AppLogEvent> watermarkStrategy = WatermarkStrategy
                .<AppLogEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<AppLogEvent>) (event, ts) -> event.getTimestamp());

        // ========== 1. 数据源：从 Kafka 消费日志 ==========
        SingleOutputStreamOperator<AppLogEvent> logStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Log Source")
                .map(json -> GsonUtil.fromJson(json, AppLogEvent.class))
                .assignTimestampsAndWatermarks(watermarkStrategy)
                // ========== 2. 使用 Side Output 分流 ERROR 日志 ==========
                .process(new org.apache.flink.streaming.api.functions.ProcessFunction<AppLogEvent, AppLogEvent>() {
                    @Override
                    public void processElement(AppLogEvent log, Context ctx, Collector<AppLogEvent> out) {
                        // 所有日志输出到主流
                        out.collect(log);
                        // ERROR 和 FATAL 级别日志额外输出到侧输出流
                        if ("ERROR".equals(log.getLevel()) || "FATAL".equals(log.getLevel())) {
                            ctx.output(ERROR_LOG_TAG, log);
                        }
                    }
                });

        // ========== 3. 获取 ERROR 侧输出流，单独处理 ==========
        DataStream<AppLogEvent> errorLogStream = logStream.getSideOutput(ERROR_LOG_TAG);
        errorLogStream.print("error-log");

        // ========== 4. 按 serviceName + level 分组，每分钟统计日志数量 ==========
        DataStream<LogStatistics> logStats = logStream
                .keyBy(log -> log.getServiceName() + "|" + log.getLevel())
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new LogCountAgg(), new LogStatsWindowFunction());

        logStats.print("log-stats");

        env.execute("实时日志分析系统");
    }

    /**
     * 日志计数增量聚合函数
     * 每条数据到来时只做 +1 操作，内存高效
     */
    public static class LogCountAgg implements AggregateFunction<AppLogEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AppLogEvent value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    /**
     * 日志统计窗口函数
     *
     * <p>知识点：AggregateFunction + ProcessWindowFunction 组合使用
     * <ul>
     *     <li>AggregateFunction 负责增量聚合，内存高效</li>
     *     <li>ProcessWindowFunction 在窗口触发时获取窗口元信息（开始/结束时间）</li>
     *     <li>两者组合既保证了性能，又能获取窗口信息</li>
     * </ul>
     */
    public static class LogStatsWindowFunction
            extends ProcessWindowFunction<Long, LogStatistics, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Long> elements,
                            Collector<LogStatistics> out) {
            Long count = elements.iterator().next();
            String[] parts = key.split("\\|", 2);
            String serviceName = parts.length > 0 ? parts[0] : "unknown";
            String level = parts.length > 1 ? parts[1] : "unknown";
            out.collect(LogStatistics.builder()
                    .serviceName(serviceName)
                    .level(level)
                    .count(count)
                    .windowStart(context.window().getStart())
                    .windowEnd(context.window().getEnd())
                    .build());
        }
    }
}
