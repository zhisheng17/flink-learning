package com.zhisheng.window;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Desc: for test https://t.zsxq.com/3RjY3FU
 * Created by zhisheng on 2020-04-21 23:45
 */
public class Main5 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source1 = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (true) {
                    sourceContext.collect("11");
                    Thread.sleep(200);
                }
            }

            @Override
            public void cancel() {
            }
        });

        DataStreamSource<String> source2 = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (true) {
                    sourceContext.collect("22");
                    Thread.sleep(400);
                }
            }

            @Override
            public void cancel() {
            }
        });

        SingleOutputStreamOperator<String> window1 = source1.timeWindowAll(Time.seconds(10))
                .process(new ProcessAllWindowFunction<String, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {
                        for (String s : iterable) {
                            collector.collect(s);
                        }
                    }
                });

        SingleOutputStreamOperator<String> window2 = source2.timeWindowAll(Time.seconds(10))
                .process(new ProcessAllWindowFunction<String, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {
                        for (String s : iterable) {
                            collector.collect(s);
                        }
                    }
                });

//        window1.union(window2).print();

        window1.union(window2)
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new ProcessAllWindowFunction<String, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {
                        for (String s : iterable) {
//                            System.out.println(s);
                            collector.collect(s);
                        }
                    }
                }).print();



        env.execute("test");
    }
}
