package com.zhisheng.examples.streaming.sideoutput;

import com.zhisheng.common.model.MetricEvent;
import com.zhisheng.common.utils.KafkaConfigUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Desc: 使用 filter 过滤数据
 * Created by zhisheng on 2019/10/1 下午4:54
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class FilterEvent {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        DataStreamSource<MetricEvent> data = KafkaConfigUtil.buildSource(env);  //从 Kafka 获取到所有的数据流
        SingleOutputStreamOperator<MetricEvent> machineData = data.filter(m -> "machine".equals(m.getTags().get("type")));  //过滤出机器的数据
        SingleOutputStreamOperator<MetricEvent> dockerData = data.filter(m -> "docker".equals(m.getTags().get("type")));    //过滤出容器的数据
        SingleOutputStreamOperator<MetricEvent> applicationData = data.filter(m -> "application".equals(m.getTags().get("type")));  //过滤出应用的数据
        SingleOutputStreamOperator<MetricEvent> middlewareData = data.filter(m -> "middleware".equals(m.getTags().get("type")));    //过滤出中间件的数据

    }
}
