package com.zhisheng.project.warehouse;

import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.project.common.constant.ProjectConstants;
import com.zhisheng.project.common.utils.ProjectKafkaUtil;
import com.zhisheng.project.warehouse.model.OrderDetail;
import com.zhisheng.project.warehouse.model.OrderStats;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/**
 * DWD 层 → DWS 层：订单汇总统计
 *
 * <p>功能描述：
 * <ul>
 *     <li>从 DWD 层消费清洗后的订单明细数据</li>
 *     <li>按商品类别分组，每 5 分钟一个窗口进行聚合统计</li>
 *     <li>计算订单数、商品总量、销售总额、独立用户数等指标</li>
 * </ul>
 *
 * <p>核心知识点：
 * <ul>
 *     <li>复杂 AggregateFunction：多维度聚合（订单数、商品数、金额、UV）</li>
 *     <li>在 AggregateFunction 中使用 Set 进行 UV 去重</li>
 *     <li>TumblingEventTimeWindow：基于事件时间的滚动窗口</li>
 *     <li>数仓 DWS 层设计：按维度汇总的轻度汇总表</li>
 * </ul>
 *
 * @author zhisheng
 */
public class DwsOrderStatsJob {

    private static final Logger LOG = LoggerFactory.getLogger(DwsOrderStatsJob.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 从 DWD 层消费清洗后的订单数据
        KafkaSource<String> dwdSource = ProjectKafkaUtil.buildKafkaStringSource(
                ProjectConstants.TOPIC_DWD_ORDER_DETAIL, "dws-order-stats-group");

        WatermarkStrategy<OrderDetail> watermarkStrategy = WatermarkStrategy
                .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<OrderDetail>) (event, ts) ->
                                event.getCreateTime());

        DataStream<OrderDetail> orderStream = env
                .fromSource(dwdSource, WatermarkStrategy.noWatermarks(), "DWD Order Source")
                .map(json -> GsonUtil.fromJson(json, OrderDetail.class))
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // 按商品类别分组，每 5 分钟统计
        DataStream<OrderStats> statsStream = orderStream
                .keyBy(OrderDetail::getCategory)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(new OrderStatsAggFunction(), new OrderStatsWindowFunction());

        statsStream.print("dws-order-stats");

        env.execute("DWD → DWS 订单汇总统计");
    }

    /**
     * 订单汇总增量聚合函数
     *
     * <p>累加器使用 Tuple4 + Set 存储中间状态：
     * <ul>
     *     <li>f0: 订单数 (Long)</li>
     *     <li>f1: 商品总量 (Long)</li>
     *     <li>f2: 总金额 (Double)</li>
     *     <li>f3: 用户去重集合 (Set&lt;String&gt;)</li>
     * </ul>
     */
    public static class OrderStatsAggFunction
            implements AggregateFunction<OrderDetail, Tuple4<Long, Long, Double, Set<String>>,
            Tuple4<Long, Long, Double, Long>> {

        @Override
        public Tuple4<Long, Long, Double, Set<String>> createAccumulator() {
            return Tuple4.of(0L, 0L, 0.0, new HashSet<>());
        }

        @Override
        public Tuple4<Long, Long, Double, Set<String>> add(
                OrderDetail order, Tuple4<Long, Long, Double, Set<String>> acc) {
            acc.f0 += 1;  // 订单数 +1
            acc.f1 += (order.getQuantity() != null ? order.getQuantity() : 0);  // 商品数量累加
            acc.f2 += (order.getTotalAmount() != null ? order.getTotalAmount() : 0);  // 金额累加
            acc.f3.add(order.getUserId());  // 用户 ID 去重
            return acc;
        }

        @Override
        public Tuple4<Long, Long, Double, Long> getResult(
                Tuple4<Long, Long, Double, Set<String>> acc) {
            return Tuple4.of(acc.f0, acc.f1, acc.f2, (long) acc.f3.size());
        }

        @Override
        public Tuple4<Long, Long, Double, Set<String>> merge(
                Tuple4<Long, Long, Double, Set<String>> a,
                Tuple4<Long, Long, Double, Set<String>> b) {
            a.f0 += b.f0;
            a.f1 += b.f1;
            a.f2 += b.f2;
            a.f3.addAll(b.f3);
            return a;
        }
    }

    /**
     * 窗口函数：将聚合结果与窗口信息组合成 OrderStats 对象
     */
    public static class OrderStatsWindowFunction
            extends ProcessWindowFunction<Tuple4<Long, Long, Double, Long>, OrderStats,
            String, TimeWindow> {

        @Override
        public void process(String category, Context context,
                            Iterable<Tuple4<Long, Long, Double, Long>> elements,
                            Collector<OrderStats> out) {
            Tuple4<Long, Long, Double, Long> result = elements.iterator().next();
            out.collect(OrderStats.builder()
                    .category(category)
                    .orderCount(result.f0)
                    .totalQuantity(result.f1)
                    .totalAmount(result.f2)
                    .uniqueUserCount(result.f3)
                    .windowStart(context.window().getStart())
                    .windowEnd(context.window().getEnd())
                    .build());
        }
    }
}
