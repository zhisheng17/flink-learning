package com.zhisheng.datalake.paimon;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Desc: Flink Paimon Streaming Write Example
 * 通过 Flink SQL 实现从 Kafka 实时写入 Paimon 表
 *
 * <p>本示例演示：
 * 1. 创建 Paimon Catalog 和数据库
 * 2. 在默认 Catalog 中创建 Kafka 源表
 * 3. 创建 Paimon 主键表
 * 4. 使用 INSERT INTO 将 Kafka 数据实时写入 Paimon 表
 *
 * <p>使用前需要：
 * 1. 启动 Kafka 并创建 user_behavior 主题
 * 2. 向 Kafka 主题发送 JSON 格式的数据
 *
 * <p>Paimon 的流式写入会自动合并数据，支持 partial-update 等高级合并模式
 *
 * Created by zhisheng
 */
public class PaimonStreamingWriteExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 创建 Kafka 源表（在默认 Catalog 中）
        String kafkaSourceDDL = "CREATE TABLE kafka_source (\n" +
                "    user_id INT,\n" +
                "    item_id INT,\n" +
                "    behavior STRING,\n" +
                "    ts TIMESTAMP(3),\n" +
                "    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'user_behavior',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'properties.group.id' = 'paimon-group',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json'\n" +
                ")";

        tEnv.executeSql(kafkaSourceDDL);

        // 创建 Paimon Catalog
        tEnv.executeSql("CREATE CATALOG paimon_catalog WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'warehouse' = '/tmp/paimon_warehouse'\n" +
                ")");

        tEnv.executeSql("USE CATALOG paimon_catalog");
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS paimon_db");
        tEnv.executeSql("USE paimon_db");

        // 创建 Paimon 主键表
        tEnv.executeSql("CREATE TABLE IF NOT EXISTS paimon_user_behavior (\n" +
                "    user_id INT,\n" +
                "    item_id INT,\n" +
                "    behavior STRING,\n" +
                "    ts TIMESTAMP(3),\n" +
                "    dt STRING,\n" +
                "    PRIMARY KEY (user_id, dt) NOT ENFORCED\n" +
                ") PARTITIONED BY (dt)");

        // 将 Kafka 数据实时写入 Paimon 表
        tEnv.executeSql("INSERT INTO paimon_user_behavior " +
                "SELECT user_id, item_id, behavior, ts, DATE_FORMAT(ts, 'yyyy-MM-dd') " +
                "FROM default_catalog.default_database.kafka_source");
    }
}
