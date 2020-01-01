package com.zhisheng.window;

import com.zhisheng.common.model.WordEvent;
import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.function.CustomSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

/**
 * Desc: Flink Window & Watermark
 * Created by zhisheng on 2019-05-14
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class Main2 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //如果不指定时间的话，默认是 ProcessingTime，但是如果指定为事件事件的话，需要事件中带有时间或者添加时间水印
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        DataStream<WordEvent> data = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<WordEvent>() {
                    private long currentTimestamp = Long.MIN_VALUE;

                    private final long maxTimeLag = 5000;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag);
                    }

                    @Override
                    public long extractTimestamp(WordEvent element, long previousElementTimestamp) {
                        long timestamp = element.getTimestamp();
                        currentTimestamp = Math.max(timestamp, currentTimestamp);
                        return timestamp;
                    }
                });

        data.keyBy(WordEvent::getWord)
                .timeWindow(Time.seconds(5))
                .sum("count")
                .print();

        env.execute("zhisheng —— flink window example");
    }
}
