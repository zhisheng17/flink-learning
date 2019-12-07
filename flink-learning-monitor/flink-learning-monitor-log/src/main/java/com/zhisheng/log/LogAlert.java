package com.zhisheng.log;

import com.zhisheng.common.model.LogEvent;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Desc: log alert
 * Created by zhisheng on 2019/10/26 下午7:23
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class LogAlert {
    public static void alert(DataStream<LogEvent> logDataStream, ParameterTool parameterTool) {
        //异常日志事件
        logDataStream.filter(logEvent -> "ERROR".equals(logEvent.getLevel().toUpperCase()))
                .print();

        //告警事件与应用通知方式和收敛方式的策略数据关联


        //sink 调用发送告警消息的接口
    }
}
