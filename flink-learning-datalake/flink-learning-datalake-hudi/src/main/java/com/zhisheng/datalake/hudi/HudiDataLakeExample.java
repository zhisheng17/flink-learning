package com.zhisheng.datalake.hudi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Desc: Flink Hudi Data Lake Example
 * 通过 Flink SQL 实现 Hudi 表的创建、写入和查询
 *
 * <p>本示例演示：
 * 1. 使用 Flink SQL 创建 Hudi MOR（Merge On Read）表
 * 2. 向 Hudi 表插入批量数据
 * 3. 查询 Hudi 表中的数据
 *
 * <p>使用前需要：
 * 1. 确保 Hudi Flink Bundle 依赖已正确引入
 * 2. 确保本地 /tmp/hudi_table 路径可写
 *
 * Created by zhisheng
 */
public class HudiDataLakeExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 创建 Hudi 表（Merge On Read 类型）
        String createTableSQL = "CREATE TABLE hudi_users (\n" +
                "    id INT PRIMARY KEY NOT ENFORCED,\n" +
                "    name STRING,\n" +
                "    age INT,\n" +
                "    ts TIMESTAMP(3),\n" +
                "    `partition` STRING\n" +
                ") PARTITIONED BY (`partition`)\n" +
                "WITH (\n" +
                "    'connector' = 'hudi',\n" +
                "    'path' = '/tmp/hudi_table',\n" +
                "    'table.type' = 'MERGE_ON_READ',\n" +
                "    'hoodie.datasource.write.recordkey.field' = 'id',\n" +
                "    'hoodie.datasource.write.precombine.field' = 'ts'\n" +
                ")";

        tEnv.executeSql(createTableSQL);

        // 创建 print 结果表
        String sinkDDL = "CREATE TABLE print_sink (\n" +
                "    id INT,\n" +
                "    name STRING,\n" +
                "    age INT,\n" +
                "    ts TIMESTAMP(3),\n" +
                "    `partition` STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'print'\n" +
                ")";

        tEnv.executeSql(sinkDDL);

        // 向 Hudi 表插入数据
        String insertSQL = "INSERT INTO hudi_users VALUES\n" +
                "    (1, 'Alice', 30, TIMESTAMP '2024-01-01 00:00:00', '2024-01-01'),\n" +
                "    (2, 'Bob', 25, TIMESTAMP '2024-01-01 00:00:00', '2024-01-01'),\n" +
                "    (3, 'Charlie', 35, TIMESTAMP '2024-01-02 00:00:00', '2024-01-02')";

        tEnv.executeSql(insertSQL);

        // 查询 Hudi 表中的数据并输出到 print sink
        tEnv.executeSql("INSERT INTO print_sink SELECT * FROM hudi_users");
    }
}
