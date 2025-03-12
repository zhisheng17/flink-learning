-- 打印 flink 1.16 任务的消费延迟 metrics 监控数据


CREATE TABLE metrics_yarn_flink_jobs (
     name STRING,
     fields Map<STRING, STRING>,
     tags Row<app_id STRING, dataman_task_id STRING, dataman_task_name STRING>
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
    tags Row<app_id STRING, dataman_task_id STRING, dataman_task_name STRING>
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
    metrics_yarn_flink_jobs ;