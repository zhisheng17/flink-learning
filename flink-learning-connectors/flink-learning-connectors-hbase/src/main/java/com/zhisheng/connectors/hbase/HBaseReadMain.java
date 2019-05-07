package com.zhisheng.connectors.hbase;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Desc: 读取 HBase 数据
 * Created by zhisheng on 2019-05-04
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class HBaseReadMain {
    //表名
    public static final String HBASE_TABLE_NAME = "zhisheng";
    // 列族
    static final byte[] INFO = "info".getBytes(ConfigConstants.DEFAULT_CHARSET);
    //列名
    static final byte[] BAR = "bar".getBytes(ConfigConstants.DEFAULT_CHARSET);


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.createInput(new TableInputFormat<Tuple2<String, String>>() {

            private Tuple2<String, String> reuse = new Tuple2<String, String>();

            @Override
            protected Scan getScanner() {
                Scan scan = new Scan();
                scan.addColumn(INFO, BAR);
                return scan;
            }

            @Override
            protected String getTableName() {
                return HBASE_TABLE_NAME;
            }

            @Override
            protected Tuple2<String, String> mapResultToTuple(Result result) {
                String key = Bytes.toString(result.getRow());
                String val = Bytes.toString(result.getValue(INFO, BAR));
                reuse.setField(key, 0);
                reuse.setField(val, 1);
                return reuse;
            }
        }).filter(new FilterFunction<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> value) throws Exception {
                return value.f1.startsWith("zhisheng");
            }
        }).print();
    }
}
