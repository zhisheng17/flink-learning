package com.zhisheng.examples.streaming.broadcast;

import com.zhisheng.common.model.MetricEvent;
import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.common.utils.KafkaConfigUtil;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * Desc: 广播变量，定时从数据库读取告警规则数据
 * Created by zhisheng on 2019-05-30
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class Main {

    final static MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
            "alarm_rules",
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO);

    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);

        DataStreamSource<Map<String, String>> alarmDataStream = env.addSource(new GetAlarmNotifyData()).setParallelism(1);//数据流定时从数据库中查出来数据

        //test for get data from MySQL
//        alarmDataStream.print();

        DataStreamSource<MetricEvent> metricEventDataStream = KafkaConfigUtil.buildSource(env);
        SingleOutputStreamOperator<MetricEvent> alert = metricEventDataStream.connect(alarmDataStream.broadcast(ALARM_RULES))
                .process(new MyBroadcastProcessFunction(ALARM_RULES));

        //其他的业务逻辑
        //alert.

        //然后在下游的算子中有使用到 alarmNotifyMap 中的配置信息

        env.execute("zhisheng broadcast demo");
    }
}
