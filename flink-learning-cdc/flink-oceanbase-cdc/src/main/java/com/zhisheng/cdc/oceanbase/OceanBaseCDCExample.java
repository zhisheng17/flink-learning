package com.zhisheng.cdc.oceanbase;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Desc: Flink OceanBase CDC Example
 * 通过 Flink CDC 实时捕获 OceanBase 数据库的变更数据，并将结果打印到控制台
 *
 * <p>使用前需要：
 * 1. 部署 OceanBase 集群和 oblogproxy 日志代理服务
 * 2. 创建 products 表并插入测试数据
 *
 * <pre>
 * CREATE TABLE products (
 *     id INT PRIMARY KEY,
 *     name VARCHAR(255),
 *     description VARCHAR(512),
 *     weight DECIMAL(10, 2)
 * );
 * </pre>
 *
 * Created by zhisheng
 */
public class OceanBaseCDCExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 创建 OceanBase CDC 源表
        String sourceDDL = "CREATE TABLE oceanbase_products (\n" +
                "    id INT NOT NULL,\n" +
                "    name STRING,\n" +
                "    description STRING,\n" +
                "    weight DECIMAL(10, 2),\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'oceanbase-cdc',\n" +
                "    'scan.startup.mode' = 'initial',\n" +
                "    'username' = 'user@obtenant',\n" +
                "    'password' = 'pswd',\n" +
                "    'tenant-name' = 'obtenant',\n" +
                "    'database-name' = 'mydb',\n" +
                "    'table-name' = 'products',\n" +
                "    'hostname' = 'localhost',\n" +
                "    'port' = '2881',\n" +
                "    'logproxy.host' = 'localhost',\n" +
                "    'logproxy.port' = '2983',\n" +
                "    'rootserver-list' = '127.0.0.1:2882:2881'\n" +
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
        tEnv.executeSql("INSERT INTO print_sink SELECT * FROM oceanbase_products");
    }
}
