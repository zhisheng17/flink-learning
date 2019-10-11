package com.zhisheng.examples.streaming.checkpoint;

import com.zhisheng.common.model.MetricEvent;
import com.zhisheng.common.utils.CheckPointUtil;
import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.common.utils.KafkaConfigUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

import static com.zhisheng.common.constant.PropertiesConstants.METRICS_TOPIC;

/**
 * Desc: checkpoint 的配置
 * Created by zhisheng on 2019-04-17
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class Main {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        Properties props = KafkaConfigUtil.buildKafkaProps(parameterTool);

        SingleOutputStreamOperator<MetricEvent> metricData = env.addSource(new FlinkKafkaConsumer011<>(
                parameterTool.get(METRICS_TOPIC),
                new SimpleStringSchema(),
                props)).setParallelism(1)
                .map(string -> GsonUtil.fromJson(string, MetricEvent.class));

        metricData.print();

        CheckPointUtil.setCheckpointConfig(env, parameterTool)
                .execute("zhisheng --- checkpoint config example");
    }
}
