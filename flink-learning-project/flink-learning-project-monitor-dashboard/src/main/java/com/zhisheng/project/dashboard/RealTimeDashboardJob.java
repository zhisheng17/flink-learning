package com.zhisheng.project.dashboard;

import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.project.common.constant.ProjectConstants;
import com.zhisheng.project.common.model.PageAccessEvent;
import com.zhisheng.project.common.utils.ProjectKafkaUtil;
import com.zhisheng.project.dashboard.model.PageViewStats;
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
 * 实时大屏 - PV/UV 统计作业
 *
 * <p>功能描述：
 * <ul>
 *     <li>从 Kafka 消费页面访问事件</li>
 *     <li>按页面类别分组</li>
 *     <li>使用 AggregateFunction 进行增量聚合，同时计算 PV、UV、平均停留时长</li>
 *     <li>每分钟输出一次统计结果</li>
 * </ul>
 *
 * <p>核心知识点：
 * <ul>
 *     <li>AggregateFunction 的复杂累加器设计（同时维护多个统计维度）</li>
 *     <li>使用 Set 在累加器中进行 UV 去重</li>
 *     <li>AggregateFunction + ProcessWindowFunction 组合获取窗口信息</li>
 * </ul>
 *
 * @author zhisheng
 */
public class RealTimeDashboardJob {

    private static final Logger LOG = LoggerFactory.getLogger(RealTimeDashboardJob.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        KafkaSource<String> kafkaSource = ProjectKafkaUtil.buildKafkaStringSource(
                ProjectConstants.TOPIC_PAGE_ACCESS, "dashboard-pv-uv-group");

        WatermarkStrategy<PageAccessEvent> watermarkStrategy = WatermarkStrategy
                .<PageAccessEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<PageAccessEvent>) (event, ts) ->
                                event.getTimestamp());

        DataStream<PageAccessEvent> accessStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Page Access Source")
                .map(json -> GsonUtil.fromJson(json, PageAccessEvent.class))
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // 按页面类别分组，每分钟统计 PV/UV/平均停留时长
        DataStream<PageViewStats> statsStream = accessStream
                .keyBy(PageAccessEvent::getPageCategory)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new PvUvAggFunction(), new PvUvWindowFunction());

        statsStream.print("dashboard-stats");

        env.execute("实时大屏 - PV/UV 统计");
    }

    /**
     * PV/UV 增量聚合函数
     *
     * <p>累加器 Tuple4 的含义：
     * <ul>
     *     <li>f0: PV 计数 (Long)</li>
     *     <li>f1: UV 去重集合 (Set&lt;String&gt;)</li>
     *     <li>f2: 总停留时长 (Long)</li>
     *     <li>f3: 页面类别 (String)</li>
     * </ul>
     *
     * <p>知识点：AggregateFunction 的累加器可以是任意复杂类型
     * 这里使用 Tuple4 同时维护 PV、UV、停留时长和页面类别
     */
    public static class PvUvAggFunction
            implements AggregateFunction<PageAccessEvent, Tuple4<Long, Set<String>, Long, String>,
            Tuple4<Long, Long, Long, String>> {

        @Override
        public Tuple4<Long, Set<String>, Long, String> createAccumulator() {
            return Tuple4.of(0L, new HashSet<>(), 0L, "");
        }

        @Override
        public Tuple4<Long, Set<String>, Long, String> add(
                PageAccessEvent event, Tuple4<Long, Set<String>, Long, String> acc) {
            // PV +1
            acc.f0 += 1;
            // UV 去重：将 userId 加入 Set
            acc.f1.add(event.getUserId());
            // 累计停留时长
            acc.f2 += (event.getStayDuration() != null ? event.getStayDuration() : 0);
            // 记录页面类别
            acc.f3 = event.getPageCategory();
            return acc;
        }

        @Override
        public Tuple4<Long, Long, Long, String> getResult(
                Tuple4<Long, Set<String>, Long, String> acc) {
            // 输出：(PV, UV, avgStayDuration, pageCategory)
            long avgStay = acc.f0 > 0 ? acc.f2 / acc.f0 : 0;
            return Tuple4.of(acc.f0, (long) acc.f1.size(), avgStay, acc.f3);
        }

        @Override
        public Tuple4<Long, Set<String>, Long, String> merge(
                Tuple4<Long, Set<String>, Long, String> a,
                Tuple4<Long, Set<String>, Long, String> b) {
            a.f0 += b.f0;
            a.f1.addAll(b.f1);
            a.f2 += b.f2;
            return a;
        }
    }

    /**
     * 窗口处理函数：将聚合结果与窗口信息组合
     */
    public static class PvUvWindowFunction
            extends ProcessWindowFunction<Tuple4<Long, Long, Long, String>, PageViewStats,
            String, TimeWindow> {

        @Override
        public void process(String key, Context context,
                            Iterable<Tuple4<Long, Long, Long, String>> elements,
                            Collector<PageViewStats> out) {
            Tuple4<Long, Long, Long, String> result = elements.iterator().next();
            out.collect(PageViewStats.builder()
                    .pageCategory(key)
                    .pageUrl("")
                    .pv(result.f0)
                    .uv(result.f1)
                    .avgStayDuration(result.f2)
                    .windowStart(context.window().getStart())
                    .windowEnd(context.window().getEnd())
                    .build());
        }
    }
}
