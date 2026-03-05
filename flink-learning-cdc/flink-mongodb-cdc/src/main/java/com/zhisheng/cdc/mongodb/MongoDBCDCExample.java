package com.zhisheng.cdc.mongodb;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Desc: Flink MongoDB CDC Example
 * 通过 Flink CDC 实时捕获 MongoDB 数据库的变更数据，并将结果打印到控制台
 *
 * <p>使用前需要：
 * 1. 开启 MongoDB 的 Replica Set（副本集）模式
 * 2. 创建 products 集合并插入测试数据
 *
 * <pre>
 * db.products.insertMany([
 *     { _id: "1", name: "scooter", description: "Big 2-wheel scooter", weight: 5.18 },
 *     { _id: "2", name: "car battery", description: "12V car battery", weight: 8.1 }
 * ]);
 * </pre>
 *
 * Created by zhisheng
 */
public class MongoDBCDCExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 创建 MongoDB CDC 源表
        String sourceDDL = "CREATE TABLE mongodb_products (\n" +
                "    _id STRING NOT NULL,\n" +
                "    name STRING,\n" +
                "    description STRING,\n" +
                "    weight DECIMAL(10, 2),\n" +
                "    PRIMARY KEY (_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'mongodb-cdc',\n" +
                "    'hosts' = 'localhost:27017',\n" +
                "    'username' = 'flinkuser',\n" +
                "    'password' = 'flinkpw',\n" +
                "    'database' = 'mydb',\n" +
                "    'collection' = 'products'\n" +
                ")";

        // 创建 print 结果表
        String sinkDDL = "CREATE TABLE print_sink (\n" +
                "    _id STRING NOT NULL,\n" +
                "    name STRING,\n" +
                "    description STRING,\n" +
                "    weight DECIMAL(10, 2),\n" +
                "    PRIMARY KEY (_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'print'\n" +
                ")";

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        // 将 CDC 数据写入到 print sink
        tEnv.executeSql("INSERT INTO print_sink SELECT * FROM mongodb_products");
    }
}
