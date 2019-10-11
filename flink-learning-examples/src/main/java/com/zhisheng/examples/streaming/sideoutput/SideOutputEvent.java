package com.zhisheng.examples.streaming.sideoutput;

import com.zhisheng.common.model.MetricEvent;
import com.zhisheng.common.utils.KafkaConfigUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Desc: 使用 side output 过滤数据
 * Created by zhisheng on 2019/10/1 下午5:15
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class SideOutputEvent {
    private static final OutputTag<MetricEvent> machineTag = new OutputTag<MetricEvent>("machine") {
    };
    private static final OutputTag<MetricEvent> dockerTag = new OutputTag<MetricEvent>("docker") {
    };
    private static final OutputTag<MetricEvent> applicationTag = new OutputTag<MetricEvent>("application") {
    };
    private static final OutputTag<MetricEvent> middlewareTag = new OutputTag<MetricEvent>("middleware") {
    };
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        DataStreamSource<MetricEvent> data = KafkaConfigUtil.buildSource(env);  //从 Kafka 获取到所有的数据流
        SingleOutputStreamOperator<MetricEvent> sideOutputData = data.process(new ProcessFunction<MetricEvent, MetricEvent>() {
            @Override
            public void processElement(MetricEvent metricEvent, Context context, Collector<MetricEvent> collector) throws Exception {
                String type = metricEvent.getTags().get("type");
                switch (type) {
                    case "machine":
                        context.output(machineTag, metricEvent);
                    case "docker":
                        context.output(dockerTag, metricEvent);
                    case "application":
                        context.output(applicationTag, metricEvent);
                    case "middleware":
                        context.output(middlewareTag, metricEvent);
                    default:
                        collector.collect(metricEvent);
                }
            }
        });
        DataStream<MetricEvent> machine = sideOutputData.getSideOutput(machineTag);
        DataStream<MetricEvent> docker = sideOutputData.getSideOutput(dockerTag);
        DataStream<MetricEvent> application = sideOutputData.getSideOutput(applicationTag);
        DataStream<MetricEvent> middleware = sideOutputData.getSideOutput(middlewareTag);
    }
}
