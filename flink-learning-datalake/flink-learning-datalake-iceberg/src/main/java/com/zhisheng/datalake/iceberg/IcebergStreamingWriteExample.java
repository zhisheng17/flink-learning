package com.zhisheng.datalake.iceberg;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Desc: Flink Iceberg Streaming Write Example
 * 通过 Flink SQL 实现从 Kafka 实时写入 Iceberg 表
 *
 * <p>本示例演示：
 * 1. 创建 Iceberg Catalog 和数据库
 * 2. 在默认 Catalog 中创建 Kafka 源表
 * 3. 创建 Iceberg 目标表
 * 4. 使用 INSERT INTO 将 Kafka 数据实时写入 Iceberg 表
 *
 * <p>使用前需要：
 * 1. 启动 Kafka 并创建 user_behavior 主题
 * 2. 向 Kafka 主题发送 JSON 格式的数据
 *
 * Created by zhisheng
 */
public class IcebergStreamingWriteExample {
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
                "    'properties.group.id' = 'iceberg-group',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json'\n" +
                ")";

        tEnv.executeSql(kafkaSourceDDL);

        // 创建 Iceberg Catalog
        tEnv.executeSql("CREATE CATALOG iceberg_catalog WITH (\n" +
                "    'type' = 'iceberg',\n" +
                "    'catalog-type' = 'hadoop',\n" +
                "    'warehouse' = '/tmp/iceberg_warehouse'\n" +
                ")");

        tEnv.executeSql("USE CATALOG iceberg_catalog");
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS iceberg_db");
        tEnv.executeSql("USE iceberg_db");

        // 创建 Iceberg 目标表
        tEnv.executeSql("CREATE TABLE IF NOT EXISTS iceberg_user_behavior (\n" +
                "    user_id INT,\n" +
                "    item_id INT,\n" +
                "    behavior STRING,\n" +
                "    ts TIMESTAMP(3),\n" +
                "    dt STRING\n" +
                ") PARTITIONED BY (dt)");

        // 将 Kafka 数据实时写入 Iceberg 表
        tEnv.executeSql("INSERT INTO iceberg_user_behavior " +
                "SELECT user_id, item_id, behavior, ts, DATE_FORMAT(ts, 'yyyy-MM-dd') " +
                "FROM default_catalog.default_database.kafka_source");
    }
}
