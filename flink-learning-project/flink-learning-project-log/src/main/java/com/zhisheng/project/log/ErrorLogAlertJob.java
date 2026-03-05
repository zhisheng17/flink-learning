package com.zhisheng.project.log;

import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.project.common.constant.ProjectConstants;
import com.zhisheng.project.common.model.AlertEvent;
import com.zhisheng.project.common.utils.ProjectKafkaUtil;
import com.zhisheng.project.log.model.AppLogEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.UUID;

/**
 * 错误日志告警作业
 *
 * <p>功能描述：
 * 监控每个服务在一定时间窗口内的 ERROR 日志数量，当 ERROR 数量超过阈值时触发告警
 *
 * <p>涉及知识点：
 * <ul>
 *     <li>KeyedProcessFunction：带有定时器的有状态处理</li>
 *     <li>Timer（定时器）：基于事件时间的定时触发</li>
 *     <li>ValueState：键控状态存储 ERROR 计数</li>
 * </ul>
 *
 * @author zhisheng
 */
public class ErrorLogAlertJob {

    private static final Logger LOG = LoggerFactory.getLogger(ErrorLogAlertJob.class);

    /** 告警阈值：1 分钟内超过此数量的 ERROR 日志触发告警 */
    private static final int ERROR_THRESHOLD = 10;
    /** 统计窗口大小：60 秒 */
    private static final long WINDOW_SIZE_MS = 60_000L;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        KafkaSource<String> kafkaSource = ProjectKafkaUtil.buildKafkaStringSource(
                ProjectConstants.TOPIC_LOG, "error-log-alert-group");

        WatermarkStrategy<AppLogEvent> watermarkStrategy = WatermarkStrategy
                .<AppLogEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<AppLogEvent>) (event, ts) -> event.getTimestamp());

        // 消费日志数据
        DataStream<AppLogEvent> logStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Log Source")
                .map(json -> GsonUtil.fromJson(json, AppLogEvent.class))
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // 过滤 ERROR 日志，按服务名分组，使用定时器统计 ERROR 数量
        DataStream<AlertEvent> alertStream = logStream
                .filter(log -> "ERROR".equals(log.getLevel()) || "FATAL".equals(log.getLevel()))
                .keyBy(AppLogEvent::getServiceName)
                .process(new ErrorCountAlertFunction());

        alertStream.print("error-alert");

        env.execute("错误日志告警系统");
    }

    /**
     * 使用 KeyedProcessFunction + Timer 实现滑动计数告警
     *
     * <p>实现思路：
     * <ol>
     *     <li>每当收到一条 ERROR 日志，错误计数 +1</li>
     *     <li>如果是窗口内第一条 ERROR，注册一个事件时间定时器（当前时间 + 窗口大小）</li>
     *     <li>定时器触发时，检查窗口内累计的 ERROR 数量</li>
     *     <li>如果超过阈值则输出告警事件，然后重置计数器</li>
     * </ol>
     */
    public static class ErrorCountAlertFunction
            extends KeyedProcessFunction<String, AppLogEvent, AlertEvent> {

        /** 当前窗口内的 ERROR 计数 */
        private transient ValueState<Long> errorCountState;
        /** 当前活跃的定时器时间戳 */
        private transient ValueState<Long> timerTimestampState;

        @Override
        public void open(Configuration parameters) throws Exception {
            errorCountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("error-count", Long.class));
            timerTimestampState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("timer-timestamp", Long.class));
        }

        @Override
        public void processElement(AppLogEvent log, Context ctx, Collector<AlertEvent> out) throws Exception {
            // 累加 ERROR 计数
            Long currentCount = errorCountState.value();
            long newCount = (currentCount == null ? 0 : currentCount) + 1;
            errorCountState.update(newCount);

            // 如果还没有注册定时器，注册一个
            if (timerTimestampState.value() == null) {
                long timerTs = log.getTimestamp() + WINDOW_SIZE_MS;
                ctx.timerService().registerEventTimeTimer(timerTs);
                timerTimestampState.update(timerTs);
            }

            // 如果当前计数已经超过阈值，立即触发告警（不等待定时器）
            if (newCount == ERROR_THRESHOLD) {
                out.collect(buildAlertEvent(ctx.getCurrentKey(), newCount));
                LOG.warn("服务 {} 在 1 分钟内 ERROR 数量达到 {} 条，触发告警",
                        ctx.getCurrentKey(), newCount);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AlertEvent> out) throws Exception {
            Long count = errorCountState.value();
            if (count != null && count > 0) {
                LOG.info("服务 {} 定时器触发，窗口内 ERROR 数量: {}", ctx.getCurrentKey(), count);
            }
            // 重置状态，开始新的窗口
            errorCountState.clear();
            timerTimestampState.clear();
        }

        private AlertEvent buildAlertEvent(String serviceName, long errorCount) {
            return AlertEvent.builder()
                    .alertId(UUID.randomUUID().toString())
                    .level(ProjectConstants.ALERT_LEVEL_CRITICAL)
                    .ruleName("error-log-spike")
                    .message(String.format("服务 [%s] 在 1 分钟内产生 %d 条 ERROR 日志，超过阈值 %d",
                            serviceName, errorCount, ERROR_THRESHOLD))
                    .metricName("error_log_count")
                    .metricValue((double) errorCount)
                    .threshold((double) ERROR_THRESHOLD)
                    .timestamp(System.currentTimeMillis())
                    .host(serviceName)
                    .build();
        }
    }
}
