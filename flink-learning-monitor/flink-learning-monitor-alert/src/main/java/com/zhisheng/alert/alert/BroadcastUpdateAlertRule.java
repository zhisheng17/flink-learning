package com.zhisheng.alert.alert;

import com.zhisheng.alert.function.GetAlertRuleSourceFunction;
import com.zhisheng.alert.model.AlertRule;
import com.zhisheng.common.model.MetricEvent;
import com.zhisheng.common.schemas.MetricSchema;
import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.common.utils.KafkaConfigUtil;
import com.zhisheng.common.watermarks.MetricWatermark;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Properties;

/**
 * Desc: 利用广播变量动态更新告警规则中的数据
 * Created by zhisheng on 2019/10/17 下午4:28
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class BroadcastUpdateAlertRule {
    private final static MapStateDescriptor<String, AlertRule> ALERT_RULE = new MapStateDescriptor<>(
            "alert_rule",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(AlertRule.class));

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

        DataStreamSource<List<AlertRule>> alarmDataStream = env.addSource(new GetAlertRuleSourceFunction()).setParallelism(1);//定时从数据库中查出告警规则数据
        machineData.connect(alarmDataStream.broadcast(ALERT_RULE))
                .process(new BroadcastProcessFunction<MetricEvent, List<AlertRule>, MetricEvent>() {
                    @Override
                    public void processElement(MetricEvent value, ReadOnlyContext ctx, Collector<MetricEvent> out) throws Exception {
                        ReadOnlyBroadcastState<String, AlertRule> broadcastState = ctx.getBroadcastState(ALERT_RULE);
                        if (broadcastState.contains(value.getName())) {
                            AlertRule alertRule = broadcastState.get(value.getName());
                            double used = (double) value.getFields().get(alertRule.getMeasurement());
                            if (used > Double.valueOf(alertRule.getThresholds())) {
                                log.info("AlertRule = {}, MetricEvent = {}", alertRule, value);
                                out.collect(value);
                            }
                        }
                    }

                    @Override
                    public void processBroadcastElement(List<AlertRule> value, Context ctx, Collector<MetricEvent> out) throws Exception {
                        if (value == null || value.size() == 0) {
                            return;
                        }
                        BroadcastState<String, AlertRule> alertRuleBroadcastState = ctx.getBroadcastState(ALERT_RULE);
                        for (AlertRule aValue : value) {
                            alertRuleBroadcastState.put(aValue.getName(), aValue);
                        }
                    }
                }).print();

        env.execute();
    }
}
