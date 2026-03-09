package com.zhisheng.project.platform;

import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.project.common.constant.ProjectConstants;
import com.zhisheng.project.common.model.PageAccessEvent;
import com.zhisheng.project.common.utils.ProjectKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * Flink Table API 编程示例
 *
 * <p>功能描述：
 * 展示如何使用 Table API（编程方式）进行实时数据处理，与 SQL 方式形成对比
 *
 * <p>核心知识点：
 * <ul>
 *     <li>DataStream 与 Table 的互相转换</li>
 *     <li>Table API 的 select、filter、groupBy、window 操作</li>
 *     <li>Table API 的 Tumble 窗口</li>
 *     <li>Table 转回 DataStream 进行后续处理</li>
 *     <li>Expression API：$("field") 引用字段</li>
 * </ul>
 *
 * @author zhisheng
 */
public class TableApiExampleJob {

    private static final Logger LOG = LoggerFactory.getLogger(TableApiExampleJob.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ========== 1. 从 Kafka 消费数据并创建 DataStream ==========
        KafkaSource<String> kafkaSource = ProjectKafkaUtil.buildKafkaStringSource(
                ProjectConstants.TOPIC_PAGE_ACCESS, "table-api-example-group");

        WatermarkStrategy<PageAccessEvent> watermarkStrategy = WatermarkStrategy
                .<PageAccessEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<PageAccessEvent>) (event, ts) ->
                                event.getTimestamp());

        DataStream<PageAccessEvent> accessStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Page Access Source")
                .map(json -> GsonUtil.fromJson(json, PageAccessEvent.class))
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // ========== 2. DataStream → Table 转换 ==========
        // 知识点：fromDataStream 将 DataStream 转换为 Table
        Table accessTable = tableEnv.fromDataStream(accessStream,
                $("eventId"),
                $("userId"),
                $("pageUrl"),
                $("pageTitle"),
                $("pageCategory"),
                $("channel"),
                $("stayDuration"),
                $("timestamp").as("ts"),
                $("deviceType"),
                $("region"));

        // ========== 3. Table API 查询 ==========
        // 3.1 过滤 + 选择
        Table filteredTable = accessTable
                .filter($("channel").isNotNull())
                .select($("pageCategory"), $("channel"), $("userId"), $("stayDuration"), $("region"));

        // 3.2 分组聚合：按渠道统计
        Table channelStats = filteredTable
                .groupBy($("channel"))
                .select(
                        $("channel"),
                        $("userId").count().as("pv"),
                        $("stayDuration").avg().as("avg_stay")
                );

        // ========== 4. Table → DataStream 转换 ==========
        // 知识点：toChangelogStream 将动态表（有更新）转回 DataStream
        DataStream<Row> resultStream = tableEnv.toChangelogStream(channelStats);
        resultStream.print("channel-stats");

        env.execute("Table API 编程示例");
    }
}
