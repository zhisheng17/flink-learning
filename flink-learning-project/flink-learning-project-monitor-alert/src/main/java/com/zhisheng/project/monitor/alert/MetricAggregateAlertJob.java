package com.zhisheng.project.monitor.alert;

import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.project.common.constant.ProjectConstants;
import com.zhisheng.project.common.model.AlertEvent;
import com.zhisheng.project.common.model.ServerMetric;
import com.zhisheng.project.common.utils.ProjectKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.UUID;

/**
 * 基于滑动窗口的指标聚合告警
 *
 * <p>功能描述：
 * <ul>
 *     <li>按主机和指标名称分组</li>
 *     <li>使用滑动窗口（5分钟窗口，1分钟滑动步长）计算指标最大值</li>
 *     <li>ReduceFunction + ProcessWindowFunction 组合实现增量聚合</li>
 *     <li>窗口触发时判断最大值是否超过阈值</li>
 * </ul>
 *
 * <p>知识点：
 * <ul>
 *     <li>SlidingEventTimeWindow：滑动窗口（窗口大小 > 滑动步长，有重叠）</li>
 *     <li>ReduceFunction：增量聚合函数，输入输出类型相同</li>
 *     <li>ReduceFunction + ProcessWindowFunction：增量聚合 + 窗口元信息</li>
 * </ul>
 *
 * @author zhisheng
 */
public class MetricAggregateAlertJob {

    private static final Logger LOG = LoggerFactory.getLogger(MetricAggregateAlertJob.class);

    /** CPU 使用率告警阈值 */
    private static final double CPU_THRESHOLD = 90.0;
    /** 内存使用率告警阈值 */
    private static final double MEMORY_THRESHOLD = 85.0;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        KafkaSource<String> kafkaSource = ProjectKafkaUtil.buildKafkaStringSource(
                ProjectConstants.TOPIC_METRIC, "metric-aggregate-alert-group");

        WatermarkStrategy<ServerMetric> watermarkStrategy = WatermarkStrategy
                .<ServerMetric>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<ServerMetric>) (event, ts) -> event.getTimestamp());

        DataStream<ServerMetric> metricStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Metric Source")
                .map(json -> GsonUtil.fromJson(json, ServerMetric.class))
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // 使用滑动窗口聚合：窗口大小 5 分钟，滑动步长 1 分钟
        // ReduceFunction 取最大值 + ProcessWindowFunction 获取窗口信息并判断阈值
        DataStream<AlertEvent> alertStream = metricStream
                .keyBy(metric -> metric.getHost() + "|" + metric.getMetricName())
                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .reduce(new MaxMetricReduceFunction(), new AlertWindowFunction());

        alertStream.print("aggregate-alert");

        env.execute("指标聚合告警系统");
    }

    /**
     * 使用 ReduceFunction 增量计算最大值
     *
     * <p>知识点：ReduceFunction 要求输入和输出类型相同
     * 每条数据到来时只做一次比较操作，非常高效
     */
    public static class MaxMetricReduceFunction implements ReduceFunction<ServerMetric> {
        @Override
        public ServerMetric reduce(ServerMetric v1, ServerMetric v2) {
            // 保留值更大的那条指标
            return v1.getValue() >= v2.getValue() ? v1 : v2;
        }
    }

    /**
     * 窗口处理函数：在窗口触发时判断最大值是否超过阈值
     *
     * <p>与 ReduceFunction 组合使用时，Iterable 中只有一个元素（增量聚合的结果）
     */
    public static class AlertWindowFunction
            extends ProcessWindowFunction<ServerMetric, AlertEvent, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<ServerMetric> elements,
                            Collector<AlertEvent> out) {
            ServerMetric maxMetric = elements.iterator().next();
            double threshold = getThreshold(maxMetric.getMetricName());

            // 判断最大值是否超过阈值
            if (threshold > 0 && maxMetric.getValue() > threshold) {
                AlertEvent alert = AlertEvent.builder()
                        .alertId(UUID.randomUUID().toString())
                        .level(ProjectConstants.ALERT_LEVEL_WARNING)
                        .ruleName("metric-aggregate-" + maxMetric.getMetricName())
                        .message(String.format("主机 [%s] 的 %s 在最近 5 分钟内最大值 %.2f%% 超过阈值 %.2f%%",
                                maxMetric.getHost(), maxMetric.getMetricName(),
                                maxMetric.getValue(), threshold))
                        .metricName(maxMetric.getMetricName())
                        .metricValue(maxMetric.getValue())
                        .threshold(threshold)
                        .timestamp(context.window().getEnd())
                        .host(maxMetric.getHost())
                        .build();
                out.collect(alert);
            }
        }

        private double getThreshold(String metricName) {
            switch (metricName) {
                case "cpu_usage":
                    return CPU_THRESHOLD;
                case "memory_usage":
                    return MEMORY_THRESHOLD;
                default:
                    return -1;
            }
        }
    }
}
