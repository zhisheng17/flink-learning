package com.zhisheng.examples.streaming.broadcast;

import com.zhisheng.common.utils.ExecutionEnvUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * Desc:
 * Created by zhisheng on 2020-02-26 18:38
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class Main2 {
    final static MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
            "alarm_rules",
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO);

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStreamSource<String> rule = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (true) {
                    sourceContext.collect("AAA");
                    Thread.sleep(60000);
                }
            }

            @Override
            public void cancel() {

            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> flatMap = env.socketTextStream("127.0.0.1", 9001)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new Tuple2<>(split[0], Long.valueOf(split[1]));
                    }
                }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {

                    private long currentTimestamp = Long.MIN_VALUE;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        Watermark watermark = new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 5000);
                        log.info("watermark is {}", watermark.getTimestamp());
                        return watermark;
                    }

                    @Override
                    public long extractTimestamp(Tuple2<String, Long> tuple2, long l) {
                        long timestamp = tuple2.f1;
                        currentTimestamp = Math.max(timestamp, currentTimestamp);
                        return timestamp;
                    }
                }).connect(rule.broadcast())
                .flatMap(new CoFlatMapFunction<Tuple2<String, Long>, String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap1(Tuple2<String, Long> tuple2, Collector<Tuple2<String, Long>> collector) throws Exception {
                        System.out.println("flatMap1 " + tuple2.f0 + " " + tuple2.f1);
                        collector.collect(tuple2);
                    }

                    @Override
                    public void flatMap2(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        System.out.println("flatMap2 " + s);
                        collector.collect(new Tuple2<>(s, System.currentTimeMillis()));
                    }
                })/*.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {

                    private long currentTimestamp = Long.MIN_VALUE;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        Watermark watermark = new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 5000);
                        log.info("watermark is {}", watermark.getTimestamp());
                        return watermark;
                    }

                    @Override
                    public long extractTimestamp(Tuple2<String, Long> tuple2, long l) {
                        long timestamp = tuple2.f1;
                        currentTimestamp = Math.max(timestamp, currentTimestamp);
                        return timestamp;
                    }
                })*/;

        flatMap.keyBy(0).timeWindow(Time.minutes(2)).sum(1).print();


        env.execute();
    }
}
