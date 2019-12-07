package com.zhisheng.log;

import com.zhisheng.common.model.LogEvent;
import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.log.function.OriLog2LogEventFlatMapFunction;
import com.zhisheng.log.schema.OriginalLogEventSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import static com.zhisheng.common.utils.KafkaConfigUtil.buildKafkaProps;

/**
 * process the log data stream
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class LogMain {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        SingleOutputStreamOperator<LogEvent> logDataStream = env.addSource(new FlinkKafkaConsumer011<>("zhisheng_log",
                new OriginalLogEventSchema(),
                buildKafkaProps(parameterTool)))
                .flatMap(new OriLog2LogEventFlatMapFunction());
        //alert
        LogAlert.alert(logDataStream, parameterTool);


        // test
//        DataStreamSource<LogEvent> logDataStream = env.addSource(new SourceFunction<LogEvent>() {
//            @Override
//            public void run(SourceContext<LogEvent> ctx) throws Exception {
//                Map<String, String> tags = new HashMap<>();
//                tags.put("host_name", "zhisheng");
//                tags.put("kafka_tpoic", "zhisheng_log");
//                tags.put("source", "/var/logs/middleware/kafka.log");
//                while (true) {
//                    LogEvent logEvent = LogEvent.builder().timestamp(System.currentTimeMillis())
//                            .level("INFO")
//                            .type("APP")
//                            .message("2019-10-26 19:53:05 INFO [GroupMetadataManager brokerId=0] Removed 0 expired offsets in 0 milliseconds. (kafka.coordinator.group.GroupMetadataManager)")
//                            .tags(tags)
//                            .build();
//                    ctx.collect(logEvent);
//                }
//            }
//
//            @Override
//            public void cancel() {
//
//            }
//        });


        //sink to es
        LogSink2ES.sink2es(logDataStream, parameterTool);

        env.execute("flink learning monitor log");
    }
}