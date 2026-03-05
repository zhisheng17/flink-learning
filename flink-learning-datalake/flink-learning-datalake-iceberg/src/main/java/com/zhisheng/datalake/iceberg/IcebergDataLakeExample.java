package com.zhisheng.datalake.iceberg;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Desc: Flink Iceberg Data Lake Example
 * 通过 Flink SQL 实现 Iceberg 表的创建、写入和查询
 *
 * <p>本示例演示：
 * 1. 创建 Iceberg Hadoop Catalog
 * 2. 在 Catalog 中创建数据库和表
 * 3. 向 Iceberg 表插入数据
 * 4. 查询 Iceberg 表中的数据
 *
 * <p>使用前需要：
 * 1. 确保 Iceberg Flink Runtime 依赖已正确引入
 * 2. 确保本地 /tmp/iceberg_warehouse 路径可写
 *
 * Created by zhisheng
 */
public class IcebergDataLakeExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 创建 Iceberg Hadoop Catalog
        tEnv.executeSql("CREATE CATALOG iceberg_catalog WITH (\n" +
                "    'type' = 'iceberg',\n" +
                "    'catalog-type' = 'hadoop',\n" +
                "    'warehouse' = '/tmp/iceberg_warehouse'\n" +
                ")");

        tEnv.executeSql("USE CATALOG iceberg_catalog");
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS iceberg_db");
        tEnv.executeSql("USE iceberg_db");

        // 创建 Iceberg 分区表
        tEnv.executeSql("CREATE TABLE IF NOT EXISTS iceberg_users (\n" +
                "    id INT,\n" +
                "    name STRING,\n" +
                "    age INT,\n" +
                "    dt STRING\n" +
                ") PARTITIONED BY (dt)");

        // 向 Iceberg 表插入数据
        tEnv.executeSql("INSERT INTO iceberg_users VALUES\n" +
                "    (1, 'Alice', 30, '2024-01-01'),\n" +
                "    (2, 'Bob', 25, '2024-01-01'),\n" +
                "    (3, 'Charlie', 35, '2024-01-02')");

        // 查询 Iceberg 表中的数据
        tEnv.executeSql("SELECT * FROM iceberg_users").print();
    }
}
