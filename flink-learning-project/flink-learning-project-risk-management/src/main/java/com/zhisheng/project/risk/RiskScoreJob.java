package com.zhisheng.project.risk;

import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.project.common.constant.ProjectConstants;
import com.zhisheng.project.common.model.TransactionEvent;
import com.zhisheng.project.common.utils.ProjectKafkaUtil;
import com.zhisheng.project.risk.model.RiskEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
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
 * 实时风险评分系统
 *
 * <p>功能描述：
 * 对每笔交易进行实时风险评分，综合考虑多个风险因素：
 * <ul>
 *     <li>交易金额异常：与用户历史平均金额相比的偏离程度</li>
 *     <li>交易频率：短时间内交易次数</li>
 *     <li>IP/城市变更：交易地点突然变化</li>
 * </ul>
 *
 * <p>核心知识点：
 * <ul>
 *     <li>多种 Keyed State 组合使用（ValueState + MapState）</li>
 *     <li>ValueState：存储单个值（交易计数、金额总和）</li>
 *     <li>MapState：存储键值对（IP 地址、城市出现次数）</li>
 *     <li>KeyedProcessFunction：有状态的键控处理函数</li>
 *     <li>实时评分算法：多因素加权评分</li>
 * </ul>
 *
 * @author zhisheng
 */
public class RiskScoreJob {

    private static final Logger LOG = LoggerFactory.getLogger(RiskScoreJob.class);

    /** 高风险阈值 */
    private static final double HIGH_RISK_THRESHOLD = 80.0;
    /** 中风险阈值 */
    private static final double MEDIUM_RISK_THRESHOLD = 50.0;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        KafkaSource<String> kafkaSource = ProjectKafkaUtil.buildKafkaStringSource(
                ProjectConstants.TOPIC_TRANSACTION, "risk-score-group");

        WatermarkStrategy<TransactionEvent> watermarkStrategy = WatermarkStrategy
                .<TransactionEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<TransactionEvent>) (event, ts) ->
                                event.getTimestamp());

        DataStream<TransactionEvent> transactionStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Transaction Source")
                .map(json -> GsonUtil.fromJson(json, TransactionEvent.class))
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // 按用户 ID 分组，进行风险评分
        DataStream<RiskEvent> riskStream = transactionStream
                .keyBy(TransactionEvent::getUserId)
                .process(new RiskScoreFunction());

        riskStream.print("risk-score");

        env.execute("实时风险评分系统");
    }

    /**
     * 风险评分函数
     *
     * <p>使用多种 Keyed State 维护用户的历史行为画像：
     * <ul>
     *     <li>transactionCountState：用户历史交易次数</li>
     *     <li>totalAmountState：用户历史交易总金额</li>
     *     <li>cityFrequencyState：用户在各城市的交易频次</li>
     *     <li>lastTransactionTimeState：上次交易时间</li>
     * </ul>
     */
    public static class RiskScoreFunction
            extends KeyedProcessFunction<String, TransactionEvent, RiskEvent> {

        /** 交易次数状态 */
        private transient ValueState<Long> transactionCountState;
        /** 交易总金额状态 */
        private transient ValueState<Double> totalAmountState;
        /** 城市交易频次状态（MapState 示例） */
        private transient MapState<String, Long> cityFrequencyState;
        /** 上次交易时间状态 */
        private transient ValueState<Long> lastTransactionTimeState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 注册各种 State
            transactionCountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("txn-count", Long.class));
            totalAmountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("total-amount", Double.class));
            cityFrequencyState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("city-frequency", String.class, Long.class));
            lastTransactionTimeState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("last-txn-time", Long.class));
        }

        @Override
        public void processElement(TransactionEvent txn, Context ctx,
                                   Collector<RiskEvent> out) throws Exception {
            // ========== 1. 更新用户画像状态 ==========
            Long count = transactionCountState.value();
            long newCount = (count == null ? 0 : count) + 1;
            transactionCountState.update(newCount);

            Double totalAmount = totalAmountState.value();
            double newTotalAmount = (totalAmount == null ? 0.0 : totalAmount) + txn.getAmount();
            totalAmountState.update(newTotalAmount);

            // 读取城市交易频次（更新前），用于后续风险评分
            String city = txn.getCity() != null ? txn.getCity() : "unknown";
            Long cityCount = cityFrequencyState.get(city);
            long previousCityCount = cityCount == null ? 0 : cityCount;
            // 更新城市交易频次
            cityFrequencyState.put(city, previousCityCount + 1);

            // ========== 2. 计算风险评分（0-100 分，分越高越危险） ==========
            double riskScore = 0.0;

            // 因素 1：金额异常度（权重 40%）
            // 如果交易金额超过历史平均的 3 倍，得满分
            if (newCount > 1) {
                double avgAmount = newTotalAmount / newCount;
                double amountRatio = txn.getAmount() / Math.max(avgAmount, 1.0);
                // 金额比率超过 3 倍，得满分 40 分；1~3 倍按比例得分
                riskScore += Math.min(40.0, (amountRatio - 1) * 20.0);
            }

            // 因素 2：交易频率（权重 30%）
            // 如果距离上次交易不到 1 分钟，得满分
            Long lastTime = lastTransactionTimeState.value();
            if (lastTime != null) {
                long interval = txn.getTimestamp() - lastTime;
                if (interval < 60_000) { // 不到 1 分钟
                    riskScore += 30.0;
                } else if (interval < 300_000) { // 不到 5 分钟
                    riskScore += 15.0;
                }
            }
            lastTransactionTimeState.update(txn.getTimestamp());

            // 因素 3：城市变更（权重 30%）
            // 如果用户从未在此城市交易过（更新前计数为 0），加分
            if (previousCityCount == 0 && newCount > 3) {
                riskScore += 30.0;
            }

            // 确保风险评分在 0-100 之间
            riskScore = Math.max(0, Math.min(100, riskScore));

            // ========== 3. 根据风险评分决定是否输出风险事件 ==========
            if (riskScore >= MEDIUM_RISK_THRESHOLD) {
                String riskLevel;
                if (riskScore >= HIGH_RISK_THRESHOLD) {
                    riskLevel = "HIGH";
                } else {
                    riskLevel = "MEDIUM";
                }

                out.collect(RiskEvent.builder()
                        .riskId(UUID.randomUUID().toString())
                        .userId(txn.getUserId())
                        .riskType("SUSPICIOUS")
                        .riskLevel(riskLevel)
                        .description(String.format(
                                "交易风险评分 %.1f：金额=%.2f, 城市=%s, 交易次数=%d",
                                riskScore, txn.getAmount(), city, newCount))
                        .amount(txn.getAmount())
                        .timestamp(txn.getTimestamp())
                        .relatedTransactionIds(txn.getTransactionId())
                        .build());
            }
        }
    }
}
