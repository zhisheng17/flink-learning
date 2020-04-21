package com.zhisheng.window;

import com.zhisheng.common.model.WordEvent;
import com.zhisheng.function.CustomSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Desc:
 * Created by zhisheng on 2019-12-12 21:29
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class Main3 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new CustomSource())
                .keyBy(WordEvent::getWord)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new WindowFunction<WordEvent, WordEvent, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<WordEvent> input, Collector<WordEvent> out) throws Exception {
                        System.out.println(window.getStart() + " " + window.getEnd());
                        for (WordEvent word : input) {
                            out.collect(word);
                        }
                    }
                })
                .print();
        env.execute();
    }
}
