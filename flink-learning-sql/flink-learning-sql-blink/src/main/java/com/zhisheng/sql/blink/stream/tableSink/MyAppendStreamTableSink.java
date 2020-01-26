package com.zhisheng.sql.blink.stream.tableSink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

/**
 * Desc: custom AppendStreamTableSink，可以参考 KafkaTableSinkBase 等的实现
 * Created by zhisheng on 2020-01-12 23:45
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class MyAppendStreamTableSink implements AppendStreamTableSink<Row> {


    @Override
    public void emitDataStream(DataStream<Row> dataStream) {

    }

    @Override
    public TableSink<Row> configure(String[] strings, TypeInformation<?>[] typeInformations) {
        return null;
    }
}
