package com.zhisheng.sql.blink.stream.tableSink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * Desc:
 * Created by zhisheng on 2020-01-10 20:45
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class MyRetractStreamTableSink implements RetractStreamTableSink<Row> {

    private TableSchema tableSchema;

    public MyRetractStreamTableSink(String[] fieldNames, TypeInformation[] typeInformations) {
        this.tableSchema = new TableSchema(fieldNames, typeInformations);
    }

    public MyRetractStreamTableSink(String[] fieldNames, DataType[] dataTypes) {
        this.tableSchema = TableSchema.builder().fields(fieldNames, dataTypes).build();
    }

    @Override
    public TableSchema getTableSchema() {
        return tableSchema;
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        return dataStream.addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
            @Override
            public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                //自定义Sink
                // f0==true :插入新数据
                // f0==false:删除旧数据
                if (value.f0) {
                    //可以写入MySQL、Kafka或者发HttpPost...根据具体情况开发
                    System.out.println(value.f1);
                }
            }
        });
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(tableSchema.getFieldTypes(), tableSchema.getFieldNames());
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] strings, TypeInformation<?>[] typeInformations) {
        return null;
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {

    }
}
