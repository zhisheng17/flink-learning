package com.zhisheng.alert.alert;

import com.zhisheng.common.model.LogEvent;
import com.zhisheng.common.model.MetricEvent;
import com.zhisheng.common.schemas.LogSchema;
import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.common.utils.KafkaConfigUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * Desc: 异常日志告警
 * Created by zhisheng on 2019/10/13 下午12:27
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class LogEventAlert {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);

        Properties properties = KafkaConfigUtil.buildKafkaProps(parameterTool);
        FlinkKafkaConsumer011<LogEvent> consumer = new FlinkKafkaConsumer011<>(
                parameterTool.get("log.topic"),
                new LogSchema(),
                properties);
        env.addSource(consumer)
                .filter(logEvent -> "error".equals(logEvent.getLevel()))
                .print();
        env.execute("log event alert");
    }
}