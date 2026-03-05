package com.zhisheng.datalake.paimon;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Desc: Flink Paimon (原 Flink Table Store) Data Lake Example
 * 通过 Flink SQL 实现 Paimon 表的创建、写入和查询
 *
 * <p>本示例演示：
 * 1. 创建 Paimon Catalog（基于文件系统）
 * 2. 在 Catalog 中创建数据库和主键表
 * 3. 向 Paimon 表插入数据
 * 4. 查询 Paimon 表中的数据
 *
 * <p>使用前需要：
 * 1. 确保 Paimon Flink 依赖已正确引入
 * 2. 确保本地 /tmp/paimon_warehouse 路径可写
 *
 * <p>Paimon 支持主键表（Primary Key Table）和仅追加表（Append Only Table）
 * 本示例使用主键表，支持数据的更新和删除操作
 *
 * Created by zhisheng
 */
public class PaimonDataLakeExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 创建 Paimon Catalog（基于文件系统）
        tEnv.executeSql("CREATE CATALOG paimon_catalog WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'warehouse' = '/tmp/paimon_warehouse'\n" +
                ")");

        tEnv.executeSql("USE CATALOG paimon_catalog");
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS paimon_db");
        tEnv.executeSql("USE paimon_db");

        // 创建 Paimon 主键表（Primary Key Table）
        tEnv.executeSql("CREATE TABLE IF NOT EXISTS paimon_users (\n" +
                "    id INT,\n" +
                "    name STRING,\n" +
                "    age INT,\n" +
                "    dt STRING,\n" +
                "    PRIMARY KEY (id, dt) NOT ENFORCED\n" +
                ") PARTITIONED BY (dt)");

        // 向 Paimon 表插入数据
        tEnv.executeSql("INSERT INTO paimon_users VALUES\n" +
                "    (1, 'Alice', 30, '2024-01-01'),\n" +
                "    (2, 'Bob', 25, '2024-01-01'),\n" +
                "    (3, 'Charlie', 35, '2024-01-02')");

        // 查询 Paimon 表中的数据
        tEnv.executeSql("SELECT * FROM paimon_users").print();
    }
}
