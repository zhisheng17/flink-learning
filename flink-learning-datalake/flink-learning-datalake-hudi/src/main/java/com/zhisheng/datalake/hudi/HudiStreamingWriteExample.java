package com.zhisheng.datalake.hudi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Desc: Flink Hudi Streaming Write Example
 * 通过 Flink SQL 实现从 Kafka 实时写入 Hudi 表
 *
 * <p>本示例演示：
 * 1. 创建 Kafka 源表（模拟实时数据流）
 * 2. 创建 Hudi 目标表
 * 3. 使用 INSERT INTO 将 Kafka 数据实时写入 Hudi 表
 *
 * <p>使用前需要：
 * 1. 启动 Kafka 并创建 user_behavior 主题
 * 2. 向 Kafka 主题发送 JSON 格式的数据
 *
 * Created by zhisheng
 */
public class HudiStreamingWriteExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 创建 Kafka 源表
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
                "    'properties.group.id' = 'hudi-group',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json'\n" +
                ")";

        tEnv.executeSql(kafkaSourceDDL);

        // 创建 Hudi 目标表
        String hudiSinkDDL = "CREATE TABLE hudi_user_behavior (\n" +
                "    user_id INT PRIMARY KEY NOT ENFORCED,\n" +
                "    item_id INT,\n" +
                "    behavior STRING,\n" +
                "    ts TIMESTAMP(3),\n" +
                "    dt STRING\n" +
                ") PARTITIONED BY (dt)\n" +
                "WITH (\n" +
                "    'connector' = 'hudi',\n" +
                "    'path' = '/tmp/hudi_user_behavior',\n" +
                "    'table.type' = 'MERGE_ON_READ',\n" +
                "    'hoodie.datasource.write.recordkey.field' = 'user_id',\n" +
                "    'hoodie.datasource.write.precombine.field' = 'ts',\n" +
                "    'write.tasks' = '1',\n" +
                "    'compaction.tasks' = '1',\n" +
                "    'compaction.async.enabled' = 'true',\n" +
                "    'compaction.trigger.strategy' = 'num_commits',\n" +
                "    'compaction.delta_commits' = '5'\n" +
                ")";

        tEnv.executeSql(hudiSinkDDL);

        // 将 Kafka 数据实时写入 Hudi 表
        tEnv.executeSql("INSERT INTO hudi_user_behavior " +
                "SELECT user_id, item_id, behavior, ts, DATE_FORMAT(ts, 'yyyy-MM-dd') " +
                "FROM kafka_source");
    }
}
