package com.zhisheng.alert.alert;

import com.zhisheng.alert.function.AlertRuleAsyncIOFunction;
import com.zhisheng.alert.model.AlertEvent;
import com.zhisheng.common.model.MetricEvent;
import com.zhisheng.common.schemas.MetricSchema;
import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.common.utils.KafkaConfigUtil;
import com.zhisheng.common.watermarks.MetricWatermark;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Desc: 使用 AsynIO 读取告警规则并判断监控数据是否告警
 * Created by zhisheng on 2019/10/16 下午5:10
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class AsyncIOAlert {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);

        Properties properties = KafkaConfigUtil.buildKafkaProps(parameterTool);
        FlinkKafkaConsumer011<MetricEvent> consumer = new FlinkKafkaConsumer011<>(
                parameterTool.get("metrics.topic"),
                new MetricSchema(),
                properties);
        SingleOutputStreamOperator<MetricEvent> machineData = env.addSource(consumer)
                .assignTimestampsAndWatermarks(new MetricWatermark());

        AsyncDataStream.unorderedWait(machineData, new AlertRuleAsyncIOFunction(), 10000, TimeUnit.MICROSECONDS, 100)
                .map(metricEvent -> {
                    List<String> ma = (List<String>) metricEvent.getFields().get("xx");
                    AlertEvent alertEvent = new AlertEvent();
                    alertEvent.setType(metricEvent.getName());
                    alertEvent.setTrigerTime(metricEvent.getTimestamp());
                    alertEvent.setMetricEvent(metricEvent);
                    if (metricEvent.getTags().get("recover") != null && Boolean.valueOf(metricEvent.getTags().get("recover"))) {
                        alertEvent.setRecover(true);
                        alertEvent.setRecoverTime(metricEvent.getTimestamp());
                    } else {
                        alertEvent.setRecover(false);
                    }
                    return alertEvent;
                })
                .print();

        env.execute("Async IO get MySQL data");
    }
}