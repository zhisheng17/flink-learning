package com.zhisheng.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

/**
 * Desc: window event time
 */
public class Main4 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(2);
        DataStreamSource<String> data = env.socketTextStream("localhost", 9001);

        data.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<>(split[0], Long.valueOf(split[1]));
            }
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
            private long currentTimestamp;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentTimestamp);
            }

            @Override
            public long extractTimestamp(Tuple2<String, Long> tuple2, long l) {
                long timestamp = tuple2.f1;
                currentTimestamp = Math.max(timestamp, currentTimestamp);
                return timestamp;
            }
        }).keyBy(0)
                .window(EventTimeSessionWindows.withGap(Time.minutes(5)))
                .sum(1)
                .print("session ");
        System.out.println(env.getExecutionPlan());
        env.execute();
    }
}
