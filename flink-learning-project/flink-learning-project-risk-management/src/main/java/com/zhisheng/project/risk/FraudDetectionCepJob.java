package com.zhisheng.project.risk;

import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.project.common.constant.ProjectConstants;
import com.zhisheng.project.common.model.TransactionEvent;
import com.zhisheng.project.common.utils.ProjectKafkaUtil;
import com.zhisheng.project.risk.model.RiskEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * 基于 Flink CEP 的实时欺诈检测系统
 *
 * <p>功能描述：
 * 使用复杂事件处理（CEP）检测交易中的异常行为模式
 *
 * <p>检测规则 - 短时间内连续小额交易后跟随大额交易：
 * <ol>
 *     <li>用户在 5 分钟内进行 3 次以上小额交易（< 10 元）</li>
 *     <li>紧接着进行一笔大额交易（> 5000 元）</li>
 *     <li>这种模式通常是欺诈者在试探账户后进行大额盗刷</li>
 * </ol>
 *
 * <p>核心知识点：
 * <ul>
 *     <li>Flink CEP：复杂事件处理库</li>
 *     <li>Pattern API：定义事件匹配模式</li>
 *     <li>SimpleCondition：简单条件过滤</li>
 *     <li>Pattern 量词：times()、oneOrMore()、timesOrMore()</li>
 *     <li>Pattern 时间约束：within() 设置模式匹配的时间窗口</li>
 *     <li>超时处理：处理部分匹配但超时的事件</li>
 * </ul>
 *
 * @author zhisheng
 */
public class FraudDetectionCepJob {

    private static final Logger LOG = LoggerFactory.getLogger(FraudDetectionCepJob.class);

    /** 小额交易阈值 */
    private static final double SMALL_AMOUNT_THRESHOLD = 10.0;
    /** 大额交易阈值 */
    private static final double LARGE_AMOUNT_THRESHOLD = 5000.0;

    /** 超时事件侧输出标签 */
    private static final OutputTag<RiskEvent> TIMEOUT_TAG = new OutputTag<RiskEvent>("timeout") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        KafkaSource<String> kafkaSource = ProjectKafkaUtil.buildKafkaStringSource(
                ProjectConstants.TOPIC_TRANSACTION, "fraud-detection-group");

        WatermarkStrategy<TransactionEvent> watermarkStrategy = WatermarkStrategy
                .<TransactionEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<TransactionEvent>) (event, ts) ->
                                event.getTimestamp());

        DataStream<TransactionEvent> transactionStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Transaction Source")
                .map(json -> GsonUtil.fromJson(json, TransactionEvent.class))
                .assignTimestampsAndWatermarks(watermarkStrategy)
                // 按用户 ID 分组，检测每个用户的交易模式
                .keyBy(TransactionEvent::getUserId);

        // ========== 定义 CEP 模式 ==========
        // 模式：3 次以上小额交易 → 1 次大额交易，在 5 分钟内完成
        Pattern<TransactionEvent, ?> fraudPattern = Pattern
                .<TransactionEvent>begin("small-amounts")
                .where(new SimpleCondition<TransactionEvent>() {
                    @Override
                    public boolean filter(TransactionEvent event) {
                        return event.getAmount() < SMALL_AMOUNT_THRESHOLD;
                    }
                })
                .timesOrMore(3)           // 3 次或更多小额交易
                .greedy()                  // 贪婪匹配，尽可能多匹配
                .followedBy("large-amount") // 后面跟着一笔大额交易
                .where(new SimpleCondition<TransactionEvent>() {
                    @Override
                    public boolean filter(TransactionEvent event) {
                        return event.getAmount() > LARGE_AMOUNT_THRESHOLD;
                    }
                })
                .within(Time.minutes(5));  // 整个模式在 5 分钟内完成

        // ========== 应用 CEP 模式到数据流 ==========
        PatternStream<TransactionEvent> patternStream = CEP.pattern(transactionStream, fraudPattern);

        // ========== 处理匹配结果和超时事件 ==========
        SingleOutputStreamOperator<RiskEvent> fraudStream = patternStream.select(
                TIMEOUT_TAG,
                // 处理超时事件（只匹配到小额交易，没等到大额交易）
                (PatternTimeoutFunction<TransactionEvent, RiskEvent>) (pattern, timeoutTimestamp) -> {
                    List<TransactionEvent> smallTxns = pattern.get("small-amounts");
                    String txnIds = smallTxns.stream()
                            .map(TransactionEvent::getTransactionId)
                            .collect(Collectors.joining(","));

                    return RiskEvent.builder()
                            .riskId(UUID.randomUUID().toString())
                            .userId(smallTxns.get(0).getUserId())
                            .riskType("SUSPICIOUS")
                            .riskLevel("MEDIUM")
                            .description(String.format("用户在 5 分钟内进行了 %d 次小额交易（模式超时，未检测到大额交易）",
                                    smallTxns.size()))
                            .amount(smallTxns.stream().mapToDouble(TransactionEvent::getAmount).sum())
                            .timestamp(timeoutTimestamp)
                            .relatedTransactionIds(txnIds)
                            .build();
                },
                // 处理完全匹配的事件（小额 + 大额）
                (PatternSelectFunction<TransactionEvent, RiskEvent>) pattern -> {
                    List<TransactionEvent> smallTxns = pattern.get("small-amounts");
                    TransactionEvent largeTxn = pattern.get("large-amount").get(0);

                    String allTxnIds = smallTxns.stream()
                            .map(TransactionEvent::getTransactionId)
                            .collect(Collectors.joining(","))
                            + "," + largeTxn.getTransactionId();

                    return RiskEvent.builder()
                            .riskId(UUID.randomUUID().toString())
                            .userId(largeTxn.getUserId())
                            .riskType("FRAUD")
                            .riskLevel("HIGH")
                            .description(String.format(
                                    "疑似盗刷：用户先进行了 %d 次小额试探交易，随后进行了 %.2f 元的大额交易",
                                    smallTxns.size(), largeTxn.getAmount()))
                            .amount(largeTxn.getAmount())
                            .timestamp(largeTxn.getTimestamp())
                            .relatedTransactionIds(allTxnIds)
                            .build();
                }
        );

        // 输出欺诈检测结果
        fraudStream.print("fraud-detected");

        // 输出超时的可疑事件
        DataStream<RiskEvent> timeoutStream = fraudStream.getSideOutput(TIMEOUT_TAG);
        timeoutStream.print("suspicious-timeout");

        env.execute("CEP 欺诈检测系统");
    }
}
