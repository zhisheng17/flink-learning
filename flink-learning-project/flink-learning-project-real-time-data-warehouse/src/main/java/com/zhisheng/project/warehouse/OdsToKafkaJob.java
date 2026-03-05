package com.zhisheng.project.warehouse;

import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.project.common.constant.ProjectConstants;
import com.zhisheng.project.common.utils.ProjectKafkaUtil;
import com.zhisheng.project.warehouse.model.OrderDetail;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ODS 层 → DWD 层：原始数据清洗与标准化
 *
 * <p>功能描述：
 * <ul>
 *     <li>从 Kafka ODS 层消费原始订单数据</li>
 *     <li>进行数据清洗（过滤无效数据、标准化字段）</li>
 *     <li>将清洗后的数据写入 DWD 层的 Kafka Topic</li>
 *     <li>使用 Side Output 将脏数据分流到专门的 Topic</li>
 * </ul>
 *
 * <p>核心知识点：
 * <ul>
 *     <li>数据清洗：使用 filter/map 进行 ETL</li>
 *     <li>Side Output：将脏数据分流处理</li>
 *     <li>KafkaSink（新 API）：写入 Kafka</li>
 *     <li>数据质量保证：空值检查、字段标准化</li>
 * </ul>
 *
 * <p>数仓分层：
 * <pre>
 *   ODS（原始层）→ DWD（明细层）→ DWS（汇总层）→ ADS（应用层）
 *   本作业负责 ODS → DWD 的清洗过程
 * </pre>
 *
 * @author zhisheng
 */
public class OdsToKafkaJob {

    private static final Logger LOG = LoggerFactory.getLogger(OdsToKafkaJob.class);

    /** 脏数据侧输出标签 */
    private static final OutputTag<String> DIRTY_DATA_TAG =
            new OutputTag<String>("dirty-data") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // ========== 1. ODS 层：消费原始订单数据 ==========
        KafkaSource<String> odsSource = ProjectKafkaUtil.buildKafkaStringSource(
                ProjectConstants.TOPIC_ORDER, "ods-order-group");

        // ========== 2. 数据清洗：ODS → DWD ==========
        SingleOutputStreamOperator<OrderDetail> processedStream = env
                .fromSource(odsSource, WatermarkStrategy.noWatermarks(), "ODS Order Source")
                .process(new ProcessFunction<String, OrderDetail>() {
                    @Override
                    public void processElement(String json,
                                               Context ctx,
                                               Collector<OrderDetail> out) {
                        try {
                            OrderDetail order = GsonUtil.fromJson(json, OrderDetail.class);
                            // 数据质量校验
                            if (order.getOrderId() == null || order.getOrderId().isEmpty()) {
                                ctx.output(DIRTY_DATA_TAG, "缺少订单ID: " + json);
                                return;
                            }
                            if (order.getTotalAmount() == null || order.getTotalAmount() < 0) {
                                ctx.output(DIRTY_DATA_TAG, "金额异常: " + json);
                                return;
                            }
                            // 字段标准化
                            if (order.getProvince() == null || order.getProvince().isEmpty()) {
                                order.setProvince("未知");
                            }
                            if (order.getPaymentMethod() == null) {
                                order.setPaymentMethod("OTHER");
                            }
                            out.collect(order);
                        } catch (Exception e) {
                            ctx.output(DIRTY_DATA_TAG, "解析失败: " + json);
                        }
                    }
                });

        // ========== 3. 清洗后的数据写入 DWD 层 Kafka ==========
        DataStream<String> dwdStream = processedStream
                .map((MapFunction<OrderDetail, String>) GsonUtil::toJson);

        KafkaSink<String> dwdSink = ProjectKafkaUtil.buildKafkaStringSink(
                ProjectConstants.TOPIC_DWD_ORDER_DETAIL);
        dwdStream.sinkTo(dwdSink);

        // ========== 4. 脏数据单独处理（可写入另一个 Kafka Topic 或日志） ==========
        DataStream<String> dirtyStream = processedStream.getSideOutput(DIRTY_DATA_TAG);
        dirtyStream.print("dirty-data");

        env.execute("ODS → DWD 数据清洗作业");
    }
}
