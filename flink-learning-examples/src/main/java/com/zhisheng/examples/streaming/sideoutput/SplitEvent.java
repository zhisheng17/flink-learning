package com.zhisheng.examples.streaming.sideoutput;

import com.zhisheng.common.model.MetricEvent;
import com.zhisheng.common.utils.KafkaConfigUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Desc: 使用 split 过滤数据
 * Created by zhisheng on 2019/10/1 下午4:54
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class SplitEvent {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        DataStreamSource<MetricEvent> data = KafkaConfigUtil.buildSource(env);  //从 Kafka 获取到所有的数据流
        SplitStream<MetricEvent> splitData = data.split(new OutputSelector<MetricEvent>() {
            @Override
            public Iterable<String> select(MetricEvent metricEvent) {
                List<String> tags = new ArrayList<>();
                String type = metricEvent.getTags().get("type");
                switch (type) {
                    case "machine":
                        tags.add("machine");
                        break;
                    case "docker":
                        tags.add("docker");
                        break;
                    case "application":
                        tags.add("application");
                        break;
                    case "middleware":
                        tags.add("middleware");
                        break;
                    default:
                        break;
                }
                return tags;
            }
        });

        DataStream<MetricEvent> machine = splitData.select("machine");
        DataStream<MetricEvent> docker = splitData.select("docker");
        DataStream<MetricEvent> application = splitData.select("application");
        DataStream<MetricEvent> middleware = splitData.select("middleware");

    }
}
