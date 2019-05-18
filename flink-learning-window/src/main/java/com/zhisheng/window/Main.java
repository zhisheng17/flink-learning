package com.zhisheng.window;

import com.zhisheng.common.utils.ExecutionEnvUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import static com.zhisheng.constant.WindowConstant.HOST_NAME;
import static com.zhisheng.constant.WindowConstant.PORT;

/**
 * Desc: Flink Window 学习
 * 操作：在终端执行 nc -l 9000 ，然后输入 long text 类型的数据
 * Created by zhisheng on 2019-05-14
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class Main {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //如果不指定时间的话，默认是 ProcessingTime，但是如果指定为事件事件的话，需要事件中带有时间或者添加时间水印
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        DataStreamSource<String> data = env.socketTextStream(parameterTool.get(HOST_NAME), parameterTool.getInt(PORT));

        //基于时间窗口
/*        data.flatMap(new LineSplitter())
                .keyBy(1)
                .timeWindow(Time.seconds(30))
                .sum(0)
                .print();*/

        //基于滑动时间窗口
/*        data.flatMap(new LineSplitter())
                .keyBy(1)
                .timeWindow(Time.seconds(60), Time.seconds(30))
                .sum(0)
                .print();*/


        //基于事件数量窗口
/*        data.flatMap(new LineSplitter())
                .keyBy(1)
                .countWindow(3)
                .sum(0)
                .print();*/


        //基于事件数量滑动窗口
/*        data.flatMap(new LineSplitter())
                .keyBy(1)
                .countWindow(4, 3)
                .sum(0)
                .print();*/


        //基于会话时间窗口
        data.flatMap(new LineSplitter())
                .keyBy(1)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))) //表示如果 5s 内没出现数据则认为超出会话时长，然后计算这个窗口的和
                .sum(0)
                .print();

        env.execute("zhisheng —— flink window example");
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<Long, String>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<Long, String>> collector) {
            String[] tokens = s.split(" ");

            if (tokens.length >= 2 && isValidLong(tokens[0])) {
                collector.collect(new Tuple2<>(Long.valueOf(tokens[0]), tokens[1]));
            }
        }
    }

    private static boolean isValidLong(String str) {
        try {
            long _v = Long.parseLong(str);
            return true;
        } catch (NumberFormatException e) {
            log.info("the str = {} is not a number", str);
            return false;
        }
    }
}
