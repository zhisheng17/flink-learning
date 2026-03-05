package com.zhisheng.project.monitor.alert;

import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.project.common.constant.ProjectConstants;
import com.zhisheng.project.common.model.AlertEvent;
import com.zhisheng.project.common.model.AlertRule;
import com.zhisheng.project.common.model.ServerMetric;
import com.zhisheng.project.common.utils.ProjectKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

/**
 * 基于广播状态的动态告警规则系统
 *
 * <p>功能描述：
 * <ul>
 *     <li>从 Kafka 消费服务器指标数据（CPU、内存、磁盘等）</li>
 *     <li>从另一个 Kafka Topic 消费告警规则配置（支持动态更新）</li>
 *     <li>使用 Broadcast State 将规则广播到所有算子实例</li>
 *     <li>根据规则实时判断指标是否触发告警</li>
 * </ul>
 *
 * <p>核心知识点：
 * <ul>
 *     <li>Broadcast State（广播状态）：将配置/规则数据广播到所有并行实例</li>
 *     <li>BroadcastProcessFunction：处理广播流和数据流的连接</li>
 *     <li>MapStateDescriptor：广播状态的描述符</li>
 *     <li>动态配置更新：无需重启作业即可更新告警规则</li>
 * </ul>
 *
 * <p>架构设计：
 * <pre>
 *   指标数据流 (Kafka: metric-topic) ──┐
 *                                      ├── connect ── BroadcastProcessFunction ── AlertEvent
 *   规则配置流 (Kafka: alert-topic) ───┘ (broadcast)
 * </pre>
 *
 * @author zhisheng
 */
public class DynamicAlertRuleJob {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicAlertRuleJob.class);

    /** 广播状态描述符：key=规则ID, value=规则对象 */
    private static final MapStateDescriptor<String, AlertRule> RULE_STATE_DESCRIPTOR =
            new MapStateDescriptor<>(
                    "alert-rules",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(AlertRule.class));

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // ========== 1. 指标数据流 ==========
        KafkaSource<String> metricSource = ProjectKafkaUtil.buildKafkaStringSource(
                ProjectConstants.TOPIC_METRIC, "monitor-alert-metric-group");

        WatermarkStrategy<ServerMetric> watermarkStrategy = WatermarkStrategy
                .<ServerMetric>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<ServerMetric>) (event, ts) -> event.getTimestamp());

        DataStream<ServerMetric> metricStream = env
                .fromSource(metricSource, WatermarkStrategy.noWatermarks(), "Metric Source")
                .map(json -> GsonUtil.fromJson(json, ServerMetric.class))
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // ========== 2. 告警规则配置流（广播流） ==========
        KafkaSource<String> ruleSource = ProjectKafkaUtil.buildKafkaStringSource(
                ProjectConstants.TOPIC_ALERT, "monitor-alert-rule-group");

        // 将规则流转换为广播流
        BroadcastStream<AlertRule> ruleBroadcastStream = env
                .fromSource(ruleSource, WatermarkStrategy.noWatermarks(), "Rule Source")
                .map(json -> GsonUtil.fromJson(json, AlertRule.class))
                .broadcast(RULE_STATE_DESCRIPTOR);

        // ========== 3. 连接数据流和广播流，进行规则匹配 ==========
        DataStream<AlertEvent> alertStream = metricStream
                .connect(ruleBroadcastStream)
                .process(new AlertRuleBroadcastFunction());

        alertStream.print("alert-output");

        env.execute("动态告警规则系统");
    }

    /**
     * 广播处理函数：处理指标数据流和规则广播流
     *
     * <p>核心逻辑：
     * <ul>
     *     <li>processElement：处理每条指标数据，遍历所有规则进行匹配</li>
     *     <li>processBroadcastElement：处理规则更新，动态添加/删除/修改规则</li>
     * </ul>
     *
     * <p>知识点：
     * <ul>
     *     <li>processElement 中只能以只读方式访问广播状态（ReadOnlyBroadcastState）</li>
     *     <li>processBroadcastElement 中可以读写广播状态（BroadcastState）</li>
     *     <li>广播状态会自动同步到所有并行实例，保证数据一致性</li>
     * </ul>
     */
    public static class AlertRuleBroadcastFunction
            extends BroadcastProcessFunction<ServerMetric, AlertRule, AlertEvent> {

        @Override
        public void processElement(ServerMetric metric, ReadOnlyContext ctx,
                                   Collector<AlertEvent> out) throws Exception {
            // 以只读方式获取广播状态中的所有规则
            ReadOnlyBroadcastState<String, AlertRule> ruleState =
                    ctx.getBroadcastState(RULE_STATE_DESCRIPTOR);

            // 遍历所有规则，检查当前指标是否匹配
            for (Map.Entry<String, AlertRule> entry : ruleState.immutableEntries()) {
                AlertRule rule = entry.getValue();

                // 只处理启用的规则，并且指标名称匹配
                if (rule.getEnabled() && rule.getMetricName().equals(metric.getMetricName())) {
                    // 根据比较操作符判断是否触发告警
                    boolean triggered = evaluateRule(metric.getValue(), rule.getOperator(), rule.getThreshold());

                    if (triggered) {
                        AlertEvent alert = AlertEvent.builder()
                                .alertId(UUID.randomUUID().toString())
                                .level(rule.getAlertLevel())
                                .ruleName(rule.getRuleName())
                                .message(String.format("规则 [%s] 触发：%s 的 %s = %.2f %s %.2f",
                                        rule.getRuleName(), metric.getHost(),
                                        metric.getMetricName(), metric.getValue(),
                                        rule.getOperator(), rule.getThreshold()))
                                .metricName(metric.getMetricName())
                                .metricValue(metric.getValue())
                                .threshold(rule.getThreshold())
                                .timestamp(System.currentTimeMillis())
                                .host(metric.getHost())
                                .build();
                        out.collect(alert);
                        LOG.warn("告警触发: {}", alert.getMessage());
                    }
                }
            }
        }

        @Override
        public void processBroadcastElement(AlertRule rule, Context ctx,
                                            Collector<AlertEvent> out) throws Exception {
            // 获取可写的广播状态
            BroadcastState<String, AlertRule> ruleState =
                    ctx.getBroadcastState(RULE_STATE_DESCRIPTOR);

            if (rule.getEnabled()) {
                // 添加或更新规则
                ruleState.put(rule.getRuleId(), rule);
                LOG.info("规则更新: {} - {}", rule.getRuleId(), rule.getRuleName());
            } else {
                // 规则禁用时，从状态中移除
                ruleState.remove(rule.getRuleId());
                LOG.info("规则禁用: {} - {}", rule.getRuleId(), rule.getRuleName());
            }
        }

        /**
         * 根据操作符和阈值评估规则
         */
        private boolean evaluateRule(double metricValue, String operator, double threshold) {
            switch (operator) {
                case "GT":
                    return metricValue > threshold;
                case "LT":
                    return metricValue < threshold;
                case "GTE":
                    return metricValue >= threshold;
                case "LTE":
                    return metricValue <= threshold;
                default:
                    LOG.warn("未知的操作符: {}", operator);
                    return false;
            }
        }
    }
}
