package com.zhisheng.sql.blink.stream.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Desc: flink sql distinct & count demo
 * Created by zhisheng on 2020-03-18 23:41
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class FlinkSQLDistinctExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment blinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        blinkStreamEnv.setParallelism(1);
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(blinkStreamEnv, blinkStreamSettings);

        String ddlSource = "CREATE TABLE user_behavior (\n" +
                "    user_id BIGINT,\n" +
                "    item_id BIGINT,\n" +
                "    category_id BIGINT,\n" +
                "    behavior STRING,\n" +
                "    ts TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka',\n" +
                "    'connector.version' = '0.11',\n" +
                "    'connector.topic' = 'user_behavior',\n" +
                "    'connector.startup-mode' = 'latest-offset',\n" +
                "    'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
                "    'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'format.type' = 'json'\n" +
                ")";

        String countSql = "select user_id, count(user_id) from user_behavior group by user_id";

        blinkStreamTableEnv.sqlUpdate(ddlSource);
        Table countTable = blinkStreamTableEnv.sqlQuery(countSql);
        blinkStreamTableEnv.toRetractStream(countTable, Row.class).print();

        String distinctSql = "select distinct(user_id) from user_behavior";
        Table distinctTable = blinkStreamTableEnv.sqlQuery(distinctSql);
        blinkStreamTableEnv.toRetractStream(distinctTable, Row.class).print("==");

        blinkStreamTableEnv.execute("Blink Stream SQL count/distinct demo");
    }
}
