package com.zhisheng.project.platform;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 Flink SQL 的实时计算平台示例
 *
 * <p>功能描述：
 * 本作业演示如何使用 Flink SQL / Table API 构建实时计算任务，包含：
 * <ul>
 *     <li>使用 DDL 创建 Kafka Source 表</li>
 *     <li>使用 DDL 创建 Kafka Sink 表</li>
 *     <li>使用 SQL 查询进行实时数据处理</li>
 *     <li>Tumble 窗口聚合查询</li>
 *     <li>动态表与流的转换</li>
 * </ul>
 *
 * <p>核心知识点：
 * <ul>
 *     <li>StreamTableEnvironment：流和表的桥接环境</li>
 *     <li>Flink SQL DDL：用 SQL 定义数据源和数据汇</li>
 *     <li>Connector DDL：Kafka connector 的 SQL 配置方式</li>
 *     <li>TUMBLE 窗口函数：SQL 中的滚动窗口</li>
 *     <li>Watermark 在 DDL 中的定义</li>
 *     <li>Table API：编程式查询</li>
 * </ul>
 *
 * <p>适用场景：
 * 在实时计算平台中，用户通过 SQL 方式提交实时计算任务，平台负责解析和执行
 *
 * @author zhisheng
 */
public class FlinkSqlPlatformJob {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSqlPlatformJob.class);

    public static void main(String[] args) throws Exception {
        // ========== 1. 创建 StreamTableEnvironment ==========
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // ========== 2. 使用 DDL 创建 Kafka Source 表（订单数据） ==========
        // 知识点：通过 SQL DDL 定义表结构、连接器配置、Watermark 策略
        String orderSourceDDL = "CREATE TABLE ods_order (\n"
                + "    order_id STRING,\n"
                + "    user_id STRING,\n"
                + "    product_name STRING,\n"
                + "    category STRING,\n"
                + "    price DOUBLE,\n"
                + "    quantity INT,\n"
                + "    total_amount DOUBLE,\n"
                + "    province STRING,\n"
                + "    create_time BIGINT,\n"
                + "    -- 从 BIGINT 时间戳生成事件时间属性\n"
                + "    ts AS TO_TIMESTAMP_LTZ(create_time, 3),\n"
                + "    -- 定义 Watermark：允许 5 秒乱序\n"
                + "    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n"
                + ") WITH (\n"
                + "    'connector' = 'kafka',\n"
                + "    'topic' = 'project-order-topic',\n"
                + "    'properties.bootstrap.servers' = 'localhost:9092',\n"
                + "    'properties.group.id' = 'sql-platform-group',\n"
                + "    'scan.startup.mode' = 'latest-offset',\n"
                + "    'format' = 'json',\n"
                + "    'json.fail-on-missing-field' = 'false',\n"
                + "    'json.ignore-parse-errors' = 'true'\n"
                + ")";
        tableEnv.executeSql(orderSourceDDL);
        LOG.info("创建 ODS 订单源表成功");

        // ========== 3. 使用 DDL 创建 Print Sink 表（用于调试输出） ==========
        String printSinkDDL = "CREATE TABLE print_sink (\n"
                + "    category STRING,\n"
                + "    window_start TIMESTAMP(3),\n"
                + "    window_end TIMESTAMP(3),\n"
                + "    order_count BIGINT,\n"
                + "    total_amount DOUBLE,\n"
                + "    unique_users BIGINT\n"
                + ") WITH (\n"
                + "    'connector' = 'print'\n"
                + ")";
        tableEnv.executeSql(printSinkDDL);
        LOG.info("创建 Print Sink 表成功");

        // ========== 4. 执行 SQL 查询：按类别每 5 分钟统计订单 ==========
        // 知识点：
        //   - TUMBLE 窗口函数：SQL 中的滚动窗口
        //   - GROUP BY 窗口 + 维度：多维窗口聚合
        //   - COUNT(DISTINCT)：SQL 中的 UV 去重
        String aggregationSQL = "INSERT INTO print_sink\n"
                + "SELECT\n"
                + "    category,\n"
                + "    TUMBLE_START(ts, INTERVAL '5' MINUTE) AS window_start,\n"
                + "    TUMBLE_END(ts, INTERVAL '5' MINUTE) AS window_end,\n"
                + "    COUNT(*) AS order_count,\n"
                + "    SUM(total_amount) AS total_amount,\n"
                + "    COUNT(DISTINCT user_id) AS unique_users\n"
                + "FROM ods_order\n"
                + "WHERE total_amount > 0\n"
                + "GROUP BY category, TUMBLE(ts, INTERVAL '5' MINUTE)";

        LOG.info("执行聚合 SQL：\n{}", aggregationSQL);
        TableResult result = tableEnv.executeSql(aggregationSQL);
        // 注意：INSERT INTO 是异步执行的，会返回一个 TableResult
        // 在流模式下，作业会持续运行直到被取消
        result.print();
    }
}
