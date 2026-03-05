package com.zhisheng.datalake.hudi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Desc: Flink CDC to Hudi Example
 * 通过 Flink CDC 实时同步 MySQL 数据到 Hudi 数据湖
 *
 * <p>本示例演示湖仓一体的核心场景：
 * 1. 使用 Flink CDC 捕获 MySQL 数据变更
 * 2. 创建 Hudi 数据湖表作为目标
 * 3. 实现 MySQL -> Hudi 的实时数据同步
 *
 * <p>使用前需要：
 * 1. 启动 MySQL 并开启 binlog
 * 2. 创建源表并插入测试数据
 * 3. 引入 flink-sql-connector-mysql-cdc 依赖
 *
 * Created by zhisheng
 */
public class HudiCDCSyncExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 创建 MySQL CDC 源表
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

        // 创建 Hudi 目标表
        String hudiSinkDDL = "CREATE TABLE hudi_orders (\n" +
                "    order_id INT PRIMARY KEY NOT ENFORCED,\n" +
                "    order_date TIMESTAMP(3),\n" +
                "    customer_name STRING,\n" +
                "    product_name STRING,\n" +
                "    price DECIMAL(10, 2),\n" +
                "    order_status STRING,\n" +
                "    dt STRING\n" +
                ") PARTITIONED BY (dt)\n" +
                "WITH (\n" +
                "    'connector' = 'hudi',\n" +
                "    'path' = '/tmp/hudi_orders',\n" +
                "    'table.type' = 'COPY_ON_WRITE',\n" +
                "    'hoodie.datasource.write.recordkey.field' = 'order_id',\n" +
                "    'hoodie.datasource.write.precombine.field' = 'order_date',\n" +
                "    'write.tasks' = '1',\n" +
                "    'compaction.tasks' = '1'\n" +
                ")";

        tEnv.executeSql(hudiSinkDDL);

        // 将 MySQL CDC 数据实时同步到 Hudi 数据湖
        tEnv.executeSql("INSERT INTO hudi_orders " +
                "SELECT order_id, order_date, customer_name, product_name, price, order_status, " +
                "DATE_FORMAT(order_date, 'yyyy-MM-dd') " +
                "FROM mysql_orders");
    }
}
