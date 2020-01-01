package com.zhisheng.alert.alert;

import com.google.common.collect.Maps;
import com.zhisheng.alert.function.OutageProcessFunction;
import com.zhisheng.alert.model.OutageMetricEvent;
import com.zhisheng.alert.model.AlertEvent;
import com.zhisheng.alert.watermark.OutageMetricWaterMark;
import com.zhisheng.common.model.MetricEvent;
import com.zhisheng.common.schemas.MetricSchema;
import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.common.utils.KafkaConfigUtil;
import com.zhisheng.common.watermarks.MetricWatermark;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.zhisheng.common.constant.MachineConstant.*;

/**
 * Desc: machine outage alert
 * Created by zhisheng on 2019/10/15 上午12:03
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class OutageAlert {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);

        Properties properties = KafkaConfigUtil.buildKafkaProps(parameterTool);
        FlinkKafkaConsumer011<MetricEvent> consumer = new FlinkKafkaConsumer011<>(
                parameterTool.get("metrics.topic"),
                new MetricSchema(),
                properties);
        env.addSource(consumer)
                .assignTimestampsAndWatermarks(new MetricWatermark())
                .flatMap(new FlatMapFunction<MetricEvent, OutageMetricEvent>() {
                    @Override
                    public void flatMap(MetricEvent metricEvent, Collector<OutageMetricEvent> collector) throws Exception {
                        Map<String, String> tags = metricEvent.getTags();
                        if (tags.containsKey(CLUSTER_NAME) && tags.containsKey(HOST_IP)) {
                            OutageMetricEvent outageMetricEvent = OutageMetricEvent.buildFromEvent(metricEvent);
                            if (outageMetricEvent != null) {
                                collector.collect(outageMetricEvent);
                            }
                        }
                    }
                })
                .assignTimestampsAndWatermarks(new OutageMetricWaterMark())
                .keyBy(outageMetricEvent -> outageMetricEvent.getKey())
                .process(new OutageProcessFunction(1000 * 10, 60))
//                .assignTimestampsAndWatermarks(new OutageMetricWaterMark())
                .map(new MapFunction<OutageMetricEvent, AlertEvent>() {
                    @Override
                    public AlertEvent map(OutageMetricEvent value) throws Exception {
                        AlertEvent alertEvent = new AlertEvent();
                        alertEvent.setType("outage");
                        alertEvent.setRecover(value.getRecover());
                        alertEvent.setTrigerTime(value.getTimestamp());
                        if (value.getRecover()) {
                            alertEvent.setRecoverTime(value.getRecoverTime());
                        }

                        MetricEvent metricEvent = new MetricEvent();
                        metricEvent.setTimestamp(value.getTimestamp());
                        metricEvent.setName("outage");
                        HashMap<String, Object> fields = Maps.newHashMap();
                        if (value.getMemUsedPercent() != null) {
                            fields.put(MEM + "_" + USED_PERCENT, value.getMemUsedPercent());
                        }
                        if (value.getLoad5() != null) {
                            fields.put(LOAD5, value.getLoad5());
                        }
                        if (value.getSwapUsedPercent() != null) {
                            fields.put(SWAP + "_" + USED_PERCENT, value.getSwapUsedPercent());
                        }
                        if (value.getCpuUsePercent() != null) {
                            fields.put(CPU + "_" + USED_PERCENT, value.getCpuUsePercent());
                        }
                        metricEvent.setFields(fields);
                        HashMap<String, String> tags = Maps.newHashMap();
                        tags.put(CLUSTER_NAME, value.getClusterName());
                        tags.put(HOST_IP, value.getHostIp());
                        metricEvent.setTags(tags);

                        alertEvent.setMetricEvent(metricEvent);

                        return alertEvent;
                    }
                })
                .print();

        env.execute("machine outage alert");
    }
}