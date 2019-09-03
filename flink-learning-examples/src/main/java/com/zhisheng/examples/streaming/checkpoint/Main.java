package com.zhisheng.examples.streaming.checkpoint;

import com.zhisheng.common.model.MetricEvent;
import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.common.utils.KafkaConfigUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.net.URI;
import java.util.Properties;

import static com.zhisheng.common.constant.PropertiesConstants.*;

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
                parameterTool.get(METRICS_TOPIC),   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                props)).setParallelism(1)
                .map(string -> GsonUtil.fromJson(string, MetricEvent.class)); //博客里面用的是 fastjson，这里用的是gson解析，解析字符串成 student 对象

        metricData.print();

        if (parameterTool.getBoolean(STREAM_CHECKPOINT_ENABLE, false)) {
            //1、state 存放在内存中，默认是 5M
            StateBackend stateBackend = new MemoryStateBackend(5 * 1024 * 1024 * 100);
            env.enableCheckpointing(parameterTool.getInt(STREAM_CHECKPOINT_INTERVAL, 60000));
            env.setStateBackend(stateBackend);
        }

        if (parameterTool.getBoolean(STREAM_CHECKPOINT_ENABLE)) {
            StateBackend stateBackend = new FsStateBackend(new URI("file:///usr/local/state/"), 0);
            env.enableCheckpointing(parameterTool.getInt(STREAM_CHECKPOINT_INTERVAL, 60000));
            env.setStateBackend(stateBackend);
        }

        env.execute("zhisheng --- checkpoint config example");
    }
}
