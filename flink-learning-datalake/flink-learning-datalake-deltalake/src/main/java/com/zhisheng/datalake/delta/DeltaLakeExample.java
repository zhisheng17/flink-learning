package com.zhisheng.datalake.delta;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Desc: Flink Delta Lake Example
 * 通过 Flink SQL 实现 Delta Lake 表的创建、写入和查询
 *
 * <p>本示例演示：
 * 1. 使用 Flink SQL 创建 Delta Lake 表
 * 2. 向 Delta Lake 表插入批量数据
 * 3. 查询 Delta Lake 表中的数据
 *
 * <p>使用前需要：
 * 1. 确保 Delta Flink Connector 依赖已正确引入
 * 2. 确保本地 /tmp/delta_table 路径可写
 * 3. 确保 Hadoop 依赖已正确配置
 *
 * <p>注意：Delta Lake Flink Connector 的版本兼容性，
 * 请参考 Delta Lake 官方文档确认与当前 Flink 版本的兼容关系
 *
 * Created by zhisheng
 */
public class DeltaLakeExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 创建 Delta Lake Catalog
        tEnv.executeSql("CREATE CATALOG delta_catalog WITH (\n" +
                "    'type' = 'delta-catalog',\n" +
                "    'catalog-type' = 'in-memory'\n" +
                ")");

        tEnv.executeSql("USE CATALOG delta_catalog");
        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS delta_db");
        tEnv.executeSql("USE delta_db");

        // 创建 Delta Lake 表
        tEnv.executeSql("CREATE TABLE IF NOT EXISTS delta_users (\n" +
                "    id INT,\n" +
                "    name STRING,\n" +
                "    age INT,\n" +
                "    dt STRING\n" +
                ") PARTITIONED BY (dt)\n" +
                "WITH (\n" +
                "    'connector' = 'delta',\n" +
                "    'table-path' = '/tmp/delta_table'\n" +
                ")");

        // 向 Delta Lake 表插入数据
        tEnv.executeSql("INSERT INTO delta_users VALUES\n" +
                "    (1, 'Alice', 30, '2024-01-01'),\n" +
                "    (2, 'Bob', 25, '2024-01-01'),\n" +
                "    (3, 'Charlie', 35, '2024-01-02')");

        // 查询 Delta Lake 表中的数据
        tEnv.executeSql("SELECT * FROM delta_users").print();
    }
}
