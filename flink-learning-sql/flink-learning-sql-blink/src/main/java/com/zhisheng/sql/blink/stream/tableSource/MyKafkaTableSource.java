package com.zhisheng.sql.blink.stream.tableSource;

import com.zhisheng.common.utils.KafkaConfigUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;

import java.util.Properties;

/**
 * Desc: custom the table source
 * Created by zhisheng on 2020-01-14 08:58
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class MyKafkaTableSource implements StreamTableSource<String> {

    ParameterTool parameterTool;

    public MyKafkaTableSource(ParameterTool parameterTool) {
        this.parameterTool = parameterTool;
    }

    @Override
    public DataType getProducedDataType() {
        return DataTypes.STRING();  //不能少，否则报错
    }

    @Override
    public DataStream<String> getDataStream(StreamExecutionEnvironment env) {
        Properties properties = KafkaConfigUtil.buildKafkaProps(parameterTool);
        return env.addSource(new FlinkKafkaConsumer011<>(parameterTool.get("kafka.topic"), new SimpleStringSchema(), properties));
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder().fields(new String[]{"word"}, new DataType[]{DataTypes.STRING()}).build();
    }
}
