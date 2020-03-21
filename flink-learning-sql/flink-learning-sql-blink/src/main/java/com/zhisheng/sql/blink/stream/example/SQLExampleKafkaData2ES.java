package com.zhisheng.sql.blink.stream.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;


/**
 * Desc: Blink Stream SQL Job, 读取 Kafka 数据，然后写入到 ES 6  和 ES 7
 * Created by zhisheng on 2019/11/3 下午1:14
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class SQLExampleKafkaData2ES {
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

        String ddlSink = "CREATE TABLE user_behavior_es (\n" +
                "    user_id BIGINT,\n" +
                "    item_id BIGINT\n" +
                ") WITH (\n" +
                "    'connector.type' = 'elasticsearch',\n" +
                "    'connector.version' = '6',\n" +
                "    'connector.hosts' = 'http://localhost:9200',\n" +
                "    'connector.index' = 'user_behavior_es',\n" +
                "    'connector.document-type' = 'user_behavior_es',\n" +
                "    'format.type' = 'json',\n" +
                "    'update-mode' = 'append',\n" +
                "    'connector.bulk-flush.max-actions' = '10'\n" +
                ")";

        //提取读取到的数据，然后只要两个字段，写入到 ES
        String sql = "insert into user_behavior_es select user_id, item_id from user_behavior";

        System.out.println(ddlSource);
        System.out.println(ddlSink);
        blinkStreamTableEnv.sqlUpdate(ddlSource);
        blinkStreamTableEnv.sqlUpdate(ddlSink);
        blinkStreamTableEnv.sqlUpdate(sql);

        blinkStreamTableEnv.execute("Blink Stream SQL Job2 —— read data from kafka，sink to es");
    }
}
