package com.zhisheng.project.dashboard;

import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.project.common.constant.ProjectConstants;
import com.zhisheng.project.common.model.PageAccessEvent;
import com.zhisheng.project.common.utils.ProjectKafkaUtil;
import com.zhisheng.project.dashboard.model.TopNResult;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 实时热门页面 TopN 排行榜
 *
 * <p>功能描述：
 * 实时计算最近 5 分钟内访问量最高的 Top 10 热门页面
 *
 * <p>实现思路：
 * <ol>
 *     <li>按页面 URL 分组，滑动窗口聚合计算每个页面的访问量</li>
 *     <li>将窗口结果按窗口结束时间重新分组</li>
 *     <li>使用 KeyedProcessFunction + Timer 收集同一窗口的所有页面数据</li>
 *     <li>定时器触发时排序输出 TopN</li>
 * </ol>
 *
 * <p>核心知识点：
 * <ul>
 *     <li>SlidingEventTimeWindow：滑动窗口</li>
 *     <li>AggregateFunction + WindowFunction 组合</li>
 *     <li>ListState：列表状态，收集同一窗口的所有分组结果</li>
 *     <li>KeyedProcessFunction + Timer：定时器触发 TopN 排序</li>
 *     <li>两阶段聚合：先分组聚合，再全局排序</li>
 * </ul>
 *
 * @author zhisheng
 */
public class TopNHotPagesJob {

    private static final Logger LOG = LoggerFactory.getLogger(TopNHotPagesJob.class);
    private static final int TOP_N = 10;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        KafkaSource<String> kafkaSource = ProjectKafkaUtil.buildKafkaStringSource(
                ProjectConstants.TOPIC_PAGE_ACCESS, "dashboard-topn-group");

        WatermarkStrategy<PageAccessEvent> watermarkStrategy = WatermarkStrategy
                .<PageAccessEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<PageAccessEvent>) (event, ts) ->
                                event.getTimestamp());

        DataStream<PageAccessEvent> accessStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Page Access Source")
                .map(json -> GsonUtil.fromJson(json, PageAccessEvent.class))
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // ========== 第一阶段：按页面 URL 分组，滑动窗口聚合 ==========
        DataStream<Tuple2<String, Long>> pageCountStream = accessStream
                .keyBy(PageAccessEvent::getPageUrl)
                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .aggregate(new PageCountAgg(), new PageCountWindowFunction());

        // ========== 第二阶段：按窗口结束时间分组，收集后排序输出 TopN ==========
        DataStream<TopNResult> topNStream = pageCountStream
                .keyBy(t -> t.f1)  // 按窗口结束时间分组
                .process(new TopNProcessFunction());

        topNStream.print("top-n-pages");

        env.execute("实时热门页面 TopN");
    }

    /**
     * 页面访问量计数
     */
    public static class PageCountAgg
            implements AggregateFunction<PageAccessEvent, Long, Long> {
        @Override
        public Long createAccumulator() { return 0L; }
        @Override
        public Long add(PageAccessEvent value, Long acc) { return acc + 1; }
        @Override
        public Long getResult(Long acc) { return acc; }
        @Override
        public Long merge(Long a, Long b) { return a + b; }
    }

    /**
     * WindowFunction 用于将聚合结果与窗口信息关联
     * 输出 Tuple2(pageUrl_count, windowEnd)
     * 这里将 pageUrl 和 count 编码到第一个字段中
     */
    public static class PageCountWindowFunction
            implements WindowFunction<Long, Tuple2<String, Long>, String, TimeWindow> {
        @Override
        public void apply(String pageUrl, TimeWindow window, Iterable<Long> input,
                          Collector<Tuple2<String, Long>> out) {
            Long count = input.iterator().next();
            // 编码为 "pageUrl|count" 和窗口结束时间
            out.collect(Tuple2.of(pageUrl + "|" + count, window.getEnd()));
        }
    }

    /**
     * TopN 处理函数
     *
     * <p>实现思路：
     * <ol>
     *     <li>每条数据到来时存入 ListState</li>
     *     <li>注册一个窗口结束时间 +1 的定时器（等待同一窗口所有数据到齐）</li>
     *     <li>定时器触发时，从 ListState 取出所有数据，排序输出 TopN</li>
     * </ol>
     */
    public static class TopNProcessFunction
            extends KeyedProcessFunction<Long, Tuple2<String, Long>, TopNResult> {

        private transient ListState<String> pageCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            pageCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("page-count-list", String.class));
        }

        @Override
        public void processElement(Tuple2<String, Long> value, Context ctx,
                                   Collector<TopNResult> out) throws Exception {
            // 将 "pageUrl|count" 存入 ListState
            pageCountListState.add(value.f0);
            // 注册定时器：窗口结束时间 + 1ms，确保同一窗口的所有数据都到齐
            ctx.timerService().registerEventTimeTimer(value.f1 + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx,
                            Collector<TopNResult> out) throws Exception {
            // 从 ListState 中取出所有数据
            List<Tuple2<String, Long>> pageCountList = new ArrayList<>();
            for (String encoded : pageCountListState.get()) {
                String[] parts = encoded.split("\\|");
                if (parts.length == 2) {
                    pageCountList.add(Tuple2.of(parts[0], Long.parseLong(parts[1])));
                }
            }

            // 清空 ListState
            pageCountListState.clear();

            // 按访问量降序排序
            pageCountList.sort(Comparator.comparingLong((Tuple2<String, Long> t) -> t.f1).reversed());

            // 构建 TopN 结果
            List<TopNResult.RankItem> items = new ArrayList<>();
            for (int i = 0; i < Math.min(TOP_N, pageCountList.size()); i++) {
                items.add(TopNResult.RankItem.builder()
                        .rank(i + 1)
                        .name(pageCountList.get(i).f0)
                        .count(pageCountList.get(i).f1)
                        .build());
            }

            out.collect(TopNResult.builder()
                    .rankName("热门页面 Top " + TOP_N)
                    .items(items)
                    .windowEnd(ctx.getCurrentKey())
                    .build());
        }
    }
}
