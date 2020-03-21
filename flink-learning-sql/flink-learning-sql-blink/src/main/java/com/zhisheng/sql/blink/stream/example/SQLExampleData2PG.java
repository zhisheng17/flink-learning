package com.zhisheng.sql.blink.stream.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * Desc: sink in PG
 * Created by zhisheng on 2020-03-19 08:36
 */
public class SQLExampleData2PG {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment blinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        blinkStreamEnv.setParallelism(1);
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(blinkStreamEnv, blinkStreamSettings);

        String ddlSource = "CREATE TABLE user_behavior (\n" +
                "    score numeric(38, 18)\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka',\n" +
                "    'connector.version' = '0.11',\n" +
                "    'connector.topic' = 'user_behavior',\n" +
                "    'connector.startup-mode' = 'latest-offset',\n" +
                "    'connector.properties.zookeeper.connect' = 'localhost:2181',\n" +
                "    'connector.properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'format.type' = 'json'\n" +
                ")";

        String ddlSink = "CREATE TABLE user_behavior_aggregate (\n" +
                "    score numeric(38, 18)\n" +
                ") WITH (\n" +
                "    'connector.type' = 'jdbc',\n" +
                "    'connector.driver' = 'org.postgresql.Driver',\n" +
                "    'connector.url' = 'jdbc:postgresql://localhost:3600/hello_hitch_user',\n" +
                "    'connector.table' = 't_hitch_user_ltv_aggregate', \n" +
                "    'connector.username' = 'hello_hitch_user', \n" +
                "    'connector.password' = 'hello_hitch_user',\n" +
                "    'connector.write.flush.max-rows' = '1' \n" +
                ")";

        String sql = "insert into user_behavior_aggregate select yidun_score from user_behavior";

        blinkStreamTableEnv.sqlUpdate(ddlSource);
        blinkStreamTableEnv.sqlUpdate(ddlSink);
        blinkStreamTableEnv.sqlUpdate(sql);

        blinkStreamTableEnv.execute("Blink Stream SQL demo PG");
    }
}
