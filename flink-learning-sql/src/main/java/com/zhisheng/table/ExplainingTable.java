package com.zhisheng.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * Desc: explain table
 * Created by zhisheng on 2019-06-13
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class ExplainingTable {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Tuple2<Integer, String>> stream1 = env.fromElements(new Tuple2<>(1, "hello"));
        DataStream<Tuple2<Integer, String>> stream2 = env.fromElements(new Tuple2<>(1, "hello"));

        Table table1 = tEnv.fromDataStream(stream1, "count, word");
        Table table2 = tEnv.fromDataStream(stream2, "count, word");
        Table table = table1
                .where("LIKE(word, 'F%')")
                .unionAll(table2);

        String explanation = tEnv.explain(table);
        System.out.println(explanation);
    }
}
