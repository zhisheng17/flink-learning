package com.zhisheng.window;

import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.function.LineSplitter;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import static com.zhisheng.constant.WindowConstant.HOST_NAME;
import static com.zhisheng.constant.WindowConstant.PORT;

/**
 * Desc: timeWindow All
 * Created by zhisheng on 2019-06-18
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class WindowAll {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //如果不指定时间的话，默认是 ProcessingTime，但是如果指定为事件事件的话，需要事件中带有时间或者添加时间水印
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        DataStreamSource<String> data = env.socketTextStream(parameterTool.get(HOST_NAME), parameterTool.getInt(PORT));

        data.flatMap(new LineSplitter())
                .keyBy(0)
                .timeWindowAll(Time.seconds(10));

        env.execute("zhisheng —— flink windowAll example");
    }
}
