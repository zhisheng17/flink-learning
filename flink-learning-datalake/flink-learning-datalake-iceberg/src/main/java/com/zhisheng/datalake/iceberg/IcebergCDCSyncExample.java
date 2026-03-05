package com.zhisheng.datalake.iceberg;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Desc: Flink CDC to Iceberg Example
 * 通过 Flink CDC 实时同步 MySQL 数据到 Iceberg 数据湖
 *
 * <p>本示例演示湖仓一体的核心场景：
 * 1. 使用 Flink CDC 捕获 MySQL 数据变更
 * 2. 创建 Iceberg Catalog 和数据湖表
 * 3. 实现 MySQL -> Iceberg 的实时数据同步
 *
 * <p>使用前需要：
 * 1. 启动 MySQL 并开启 binlog
 * 2. 创建源表并插入测试数据
 * 3. 引入 flink-sql-connector-mysql-cdc 依赖
 *
 * Created by zhisheng
 */
public class IcebergCDCSyncExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 创建 MySQL CDC 源表（在默认 Catalog 中）
        String mysqlSourceDDL = "CREATE TABLE mysql_orders (\n" +
                "    order_id INT NOT NULL,\n" +
                "    order_date TIMESTAMP(3),\n" +
                "    customer_name STRING,\n" +
                "    product_name STRING,\n" +
                "    price DECIMAL(10, 2),\n" +
                "    order_status STRING,\n" +
                "    PRIMARY KEY (order_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'hostname' = 'localhost',\n" +
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456',\n" +
                "    'database-name' = 'mydb',\n" +
                "    'table-name' = 'orders'\n" +
                ")";

        tEnv.executeSql(mysqlSourceDDL);

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
        tEnv.executeSql("CREATE TABLE IF NOT EXISTS iceberg_orders (\n" +
                "    order_id INT,\n" +
                "    order_date TIMESTAMP(3),\n" +
                "    customer_name STRING,\n" +
                "    product_name STRING,\n" +
                "    price DECIMAL(10, 2),\n" +
                "    order_status STRING,\n" +
                "    dt STRING\n" +
                ") PARTITIONED BY (dt)");

        // 将 MySQL CDC 数据实时同步到 Iceberg 数据湖
        tEnv.executeSql("INSERT INTO iceberg_orders " +
                "SELECT order_id, order_date, customer_name, product_name, price, order_status, " +
                "DATE_FORMAT(order_date, 'yyyy-MM-dd') " +
                "FROM default_catalog.default_database.mysql_orders");
    }
}
