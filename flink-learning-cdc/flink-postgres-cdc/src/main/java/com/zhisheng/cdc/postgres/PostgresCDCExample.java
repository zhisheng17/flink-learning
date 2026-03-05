package com.zhisheng.cdc.postgres;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Desc: Flink PostgreSQL CDC Example
 * 通过 Flink CDC 实时捕获 PostgreSQL 数据库的变更数据，并将结果打印到控制台
 *
 * <p>使用前需要：
 * 1. PostgreSQL 设置 wal_level = logical
 * 2. 创建 products 表并插入测试数据
 *
 * <pre>
 * CREATE TABLE products (
 *     id INTEGER PRIMARY KEY,
 *     name VARCHAR(255),
 *     description VARCHAR(512),
 *     weight NUMERIC(10, 2)
 * );
 * </pre>
 *
 * Created by zhisheng
 */
public class PostgresCDCExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 创建 PostgreSQL CDC 源表
        String sourceDDL = "CREATE TABLE postgres_products (\n" +
                "    id INT NOT NULL,\n" +
                "    name STRING,\n" +
                "    description STRING,\n" +
                "    weight DECIMAL(10, 2),\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'postgres-cdc',\n" +
                "    'hostname' = 'localhost',\n" +
                "    'port' = '5432',\n" +
                "    'username' = 'postgres',\n" +
                "    'password' = '123456',\n" +
                "    'database-name' = 'postgres',\n" +
                "    'schema-name' = 'public',\n" +
                "    'table-name' = 'products',\n" +
                "    'slot.name' = 'flink'\n" +
                ")";

        // 创建 print 结果表
        String sinkDDL = "CREATE TABLE print_sink (\n" +
                "    id INT NOT NULL,\n" +
                "    name STRING,\n" +
                "    description STRING,\n" +
                "    weight DECIMAL(10, 2),\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'print'\n" +
                ")";

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // 将 CDC 数据写入到 print sink
        tEnv.executeSql("INSERT INTO print_sink SELECT * FROM postgres_products");
    }
}
