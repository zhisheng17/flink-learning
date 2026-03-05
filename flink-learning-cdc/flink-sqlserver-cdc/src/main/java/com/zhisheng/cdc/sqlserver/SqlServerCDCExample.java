package com.zhisheng.cdc.sqlserver;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Desc: Flink SQL Server CDC Example
 * 通过 Flink CDC 实时捕获 SQL Server 数据库的变更数据，并将结果打印到控制台
 *
 * <p>使用前需要：
 * 1. 开启 SQL Server 的 CDC 功能
 * 2. 创建 products 表并插入测试数据
 *
 * <pre>
 * CREATE TABLE products (
 *     id INT PRIMARY KEY,
 *     name VARCHAR(255),
 *     description VARCHAR(512),
 *     weight DECIMAL(10, 2)
 * );
 * EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'products', @role_name = NULL;
 * </pre>
 *
 * Created by zhisheng
 */
public class SqlServerCDCExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 创建 SQL Server CDC 源表
        String sourceDDL = "CREATE TABLE sqlserver_products (\n" +
                "    id INT NOT NULL,\n" +
                "    name STRING,\n" +
                "    description STRING,\n" +
                "    weight DECIMAL(10, 2),\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'sqlserver-cdc',\n" +
                "    'hostname' = 'localhost',\n" +
                "    'port' = '1433',\n" +
                "    'username' = 'sa',\n" +
                "    'password' = 'Password!',\n" +
                "    'database-name' = 'inventory',\n" +
                "    'schema-name' = 'dbo',\n" +
                "    'table-name' = 'products'\n" +
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
        tEnv.executeSql("INSERT INTO print_sink SELECT * FROM sqlserver_products");
    }
}
