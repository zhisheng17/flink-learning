package com.zhisheng.sql.blink.stream.tableSink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.types.Row;

/**
 * Desc: custom UpsertStreamTableSink，可以参考 JDBCUpsertTableSink 的实现
 * Created by zhisheng on 2020-01-12 23:45
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class MyUpsertStreamTableSink implements UpsertStreamTableSink<Row> {

    @Override
    public void setKeyFields(String[] strings) {

    }

    @Override
    public void setIsAppendOnly(Boolean aBoolean) {

    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return null;
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {

    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] strings, TypeInformation<?>[] typeInformations) {
        return null;
    }
}
