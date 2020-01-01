package com.zhisheng.log.function;

import com.zhisheng.common.model.LogEvent;
import com.zhisheng.common.utils.DateUtil;
import com.zhisheng.log.model.OriginalLogEvent;
import com.zhisheng.log.utils.GrokUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

import static com.zhisheng.common.utils.DateUtil.YYYY_MM_DD_HH_MM_SS;

/**
 * Desc:
 * Created by zhisheng on 2019/10/27 下午1:59
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class OriLog2LogEventFlatMapFunction extends RichFlatMapFunction<OriginalLogEvent, LogEvent> {
    @Override
    public void flatMap(OriginalLogEvent originalLogEvent, Collector<LogEvent> collector) throws Exception {
        if (originalLogEvent == null) {
            return;
        }
        LogEvent logEvent = new LogEvent();
        String source = originalLogEvent.getSource();
        if (source.contains("middleware")) {
            logEvent.setType("MIDDLEWARE");
        } else if (source.contains("app")){
            logEvent.setType("APP");
        } else if (source.contains("docker")) {
            logEvent.setType("DOCKER");
        } else {
            logEvent.setType("MACHINE");
        }
        logEvent.setMessage(originalLogEvent.getMessage());

        Map<String, Object> messageMap = GrokUtil.toMap("%{KAFKALOG}", originalLogEvent.getMessage());

        logEvent.setTimestamp(DateUtil.format(messageMap.get("timestamp").toString(), YYYY_MM_DD_HH_MM_SS));

        logEvent.setLevel(messageMap.get("level").toString());
        Map<String, String> tags = new HashMap<>();
        tags.put("host_name", originalLogEvent.getHost().get("name"));
        tags.put("kafka_tpoic", originalLogEvent.getMetadata().get("topic"));
        tags.put("source", originalLogEvent.getSource());
        //可以添加更多 message 解析出来的字段放在该 tags 里面

        logEvent.setTags(tags);
        collector.collect(logEvent);
    }
}
