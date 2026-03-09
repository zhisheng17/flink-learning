package com.zhisheng.project.common.constant;

/**
 * 项目模块共享常量
 *
 * @author zhisheng
 */
public class ProjectConstants {

    private ProjectConstants() {
    }

    // Kafka topic 常量
    public static final String TOPIC_LOG = "project-log-topic";
    public static final String TOPIC_METRIC = "project-metric-topic";
    public static final String TOPIC_ALERT = "project-alert-topic";
    public static final String TOPIC_TRANSACTION = "project-transaction-topic";
    public static final String TOPIC_PAGE_ACCESS = "project-page-access-topic";
    public static final String TOPIC_ORDER = "project-order-topic";
    public static final String TOPIC_ORDER_DETAIL = "project-order-detail-topic";

    // DWD 层 topic
    public static final String TOPIC_DWD_ORDER_DETAIL = "dwd-order-detail-topic";
    // DWS 层 topic
    public static final String TOPIC_DWS_ORDER_STATS = "dws-order-stats-topic";

    // Kafka 默认配置
    public static final String DEFAULT_BROKER_LIST = "localhost:9092";
    public static final String DEFAULT_GROUP_ID = "flink-learning-project";

    // 告警级别
    public static final String ALERT_LEVEL_INFO = "INFO";
    public static final String ALERT_LEVEL_WARNING = "WARNING";
    public static final String ALERT_LEVEL_CRITICAL = "CRITICAL";
}
