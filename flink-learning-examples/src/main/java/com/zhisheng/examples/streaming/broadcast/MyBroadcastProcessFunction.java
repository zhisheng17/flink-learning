package com.zhisheng.examples.streaming.broadcast;

import com.zhisheng.common.model.MetricEvent;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * Desc:
 * Created by zhisheng on 2020-04-28 15:46
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class MyBroadcastProcessFunction extends BroadcastProcessFunction<MetricEvent, Map<String, String>, MetricEvent> {

    private MapStateDescriptor<String, String> alarmRulesMapStateDescriptor;

    public MyBroadcastProcessFunction(MapStateDescriptor<String, String> alarmRulesMapStateDescriptor) {
        this.alarmRulesMapStateDescriptor = alarmRulesMapStateDescriptor;
    }

    @Override
    public void processElement(MetricEvent metricEvent, ReadOnlyContext readOnlyContext, Collector<MetricEvent> collector) throws Exception {
        ReadOnlyBroadcastState<String, String> broadcastState = readOnlyContext.getBroadcastState(alarmRulesMapStateDescriptor);
        Map<String, String> tags = metricEvent.getTags();
        if (!tags.containsKey("type") && !tags.containsKey("type_id")) {
            return;
        }
        String targetId = broadcastState.get(tags.get("type") + tags.containsKey("type_id"));
        if (targetId != null) {
            metricEvent.getTags().put("target_id", targetId); //将通知方式的 hook 放在 tag 里面，在下游要告警的时候通过该字段获取到对应的 hook 地址
            collector.collect(metricEvent);
        }
    }

    @Override
    public void processBroadcastElement(Map<String, String> value, Context context, Collector<MetricEvent> collector) throws Exception {
        if (value != null) {
            BroadcastState<String, String> broadcastState = context.getBroadcastState(alarmRulesMapStateDescriptor);
            for (Map.Entry<String, String> entry : value.entrySet()) {
                broadcastState.put(entry.getKey(), entry.getValue());
            }
        }
    }
}
