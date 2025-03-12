CREATE TEMPORARY FUNCTION json_value_udf AS 'com.zhisheng.udf.JsonValueUdf';

CREATE TABLE metrics_flink_jobs (
    name STRING,
    fields Map<STRING, STRING>,
    tags Row<app_id STRING, platform_task_id STRING, platform_task_name STRING>
) WITH (
      'connector' = 'kafka',
      'topic' = 'metrics-flink-jobs',
      'properties.bootstrap.servers' = 'logs-kafka1.xxx:9092,logs-kafka2.xxx:9092,logs-kafka3.xxx:9092',
      'properties.group.id' = 'test',
      'format' = 'json'
);


CREATE TABLE flink_jobs_metrics (
    name STRING,
    fields Map<STRING, STRING>,
    tags Row<app_id STRING, platform_task_id STRING, platform_task_name STRING>
) WITH (
      'connector' = 'print'
);

insert into
    flink_jobs_metrics
select
    name,
    fields,
    tags
from
    metrics_flink_jobs ;