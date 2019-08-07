package com.zhisheng.window;

import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.function.CustomTrigger;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import static com.zhisheng.constant.WindowConstant.HOST_NAME;
import static com.zhisheng.constant.WindowConstant.PORT;

/**
 * Desc:
 *  操作：在终端执行 nc -l 9000 ，然后输入 long text 类型的数据
 * Created by zhisheng on 2019-08-06
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class CustomTriggerMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //如果不指定时间的话，默认是 ProcessingTime，但是如果指定为事件事件的话，需要事件中带有时间或者添加时间水印
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        DataStreamSource<String> data = env.socketTextStream(parameterTool.get(HOST_NAME), parameterTool.getInt(PORT));

        data.keyBy(0)
                .timeWindow(Time.seconds(30));
//                .trigger(new CustomTrigger())

        env.execute("zhisheng  custom Trigger Window demo");
    }
}
