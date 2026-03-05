package com.zhisheng.cdc.db2;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Desc: Flink DB2 CDC Example
 * 通过 Flink CDC 实时捕获 IBM DB2 数据库的变更数据，并将结果打印到控制台
 *
 * <p>使用前需要：
 * 1. 开启 DB2 的 CDC 功能
 * 2. 创建 PRODUCTS 表并插入测试数据
 *
 * <pre>
 * CREATE TABLE DB2INST1.PRODUCTS (
 *     ID INTEGER NOT NULL PRIMARY KEY,
 *     NAME VARCHAR(255),
 *     DESCRIPTION VARCHAR(512),
 *     WEIGHT DECIMAL(10, 2)
 * );
 * </pre>
 *
 * Created by zhisheng
 */
public class Db2CDCExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 创建 DB2 CDC 源表
        String sourceDDL = "CREATE TABLE db2_products (\n" +
                "    id INT NOT NULL,\n" +
                "    name STRING,\n" +
                "    description STRING,\n" +
                "    weight DECIMAL(10, 2),\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'db2-cdc',\n" +
                "    'hostname' = 'localhost',\n" +
                "    'port' = '50000',\n" +
                "    'username' = 'db2inst1',\n" +
                "    'password' = '123456',\n" +
                "    'database-name' = 'mydb',\n" +
                "    'schema-name' = 'DB2INST1',\n" +
                "    'table-name' = 'PRODUCTS'\n" +
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
        tEnv.executeSql("INSERT INTO print_sink SELECT * FROM db2_products");
    }
}
