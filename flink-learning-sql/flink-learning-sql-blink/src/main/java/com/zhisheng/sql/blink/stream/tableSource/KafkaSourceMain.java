package com.zhisheng.sql.blink.stream.tableSource;

import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.common.utils.KafkaConfigUtil;
import com.zhisheng.sql.blink.stream.tableSink.MyRetractStreamTableSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * Desc:
 * Created by zhisheng on 2020-01-13 00:09
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class KafkaSourceMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment blinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        blinkStreamEnv.setParallelism(1);
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(blinkStreamEnv, blinkStreamSettings);

        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        Properties properties = KafkaConfigUtil.buildKafkaProps(parameterTool);
        DataStream<String> dataStream = blinkStreamEnv.addSource(new FlinkKafkaConsumer011<>(parameterTool.get("kafka.topic"), new SimpleStringSchema(), properties));
        Table table = blinkStreamTableEnv.fromDataStream(dataStream, "word");
        blinkStreamTableEnv.registerTable("kafkaDataStream", table);

        RetractStreamTableSink<Row> retractStreamTableSink = new MyRetractStreamTableSink(new String[]{"_count", "word"}, new DataType[]{DataTypes.BIGINT(), DataTypes.STRING()});
        blinkStreamTableEnv.registerTableSink("sinkTable", retractStreamTableSink);

        Table wordCount = blinkStreamTableEnv.sqlQuery("SELECT count(word) AS _count,word FROM kafkaDataStream GROUP BY word");

        wordCount.insertInto("sinkTable");

        blinkStreamTableEnv.execute("Blink Kafka Table Source");
    }
}
