package com.zhisheng.connectors.influxdb;


import com.zhisheng.common.model.MetricEvent;
import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.common.utils.KafkaConfigUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        DataStreamSource<MetricEvent> data = KafkaConfigUtil.buildSource(env);

        //请将下面的这些字段弄成常量
        InfluxDBConfig config = InfluxDBConfig.builder()
                .url(parameterTool.get("influxdb.url"))
                .username(parameterTool.get("influxdb.username"))
                .password(parameterTool.get("influxdb.password"))
                .database(parameterTool.get("influxdb.database"))
                .batchActions(parameterTool.getInt("influxdb.batchActions"))
                .flushDuration(parameterTool.getInt("influxdb.flushDuration"))
                .enableGzip(parameterTool.getBoolean("influxdb.enableGzip"))
                .createDatabase(parameterTool.getBoolean("influxdb.createDatabase"))
                .build();

        data.addSink(new InfluxDBSink(config));

        env.execute("flink InfluxDB connector");
    }
}
