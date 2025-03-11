CREATE TABLE yarn_flink_warn_logs (
      `source` STRING,
      `id` STRING,
      `timestamp` BIGINT,
      `content` STRING,
      `tags` ROW<host_ip STRING, method_name STRING, `level` STRING, `file_name` STRING, line_number STRING, thread_name STRING, container_type STRING, logger_name STRING, class_name STRING, app_id STRING, `host_name` STRING, container_id STRING, `task_id` STRING, `task_name` STRING>
) WITH (
      'connector' = 'kafka',
      'topic' = 'yarn_flink_log',
      -- 'scan.startup.mode' = 'latest-offset',
      'properties.bootstrap.servers' = 'logs-kafka1.xxx:9092,logs-kafka2.xxx:9092,logs-kafka3.xxx:9092',
      'properties.group.id' = 'flink_warn_logs',
      'scan.topic-partition-discovery.interval' = '10000 ms',
      'format' = 'json',
      'json.fail-on-missing-field' = 'false',
      'json.ignore-parse-errors' = 'true'
      );


CREATE TABLE yarn_flink_warn_logs_es (
     `source` STRING,
     `logTime` BIGINT,
     `log_time` TIMESTAMP(3),
     `content` STRING,
     `tags` ROW<host_ip STRING, method_name STRING, `level` STRING, `file_name` STRING, line_number STRING, thread_name STRING, container_type STRING, logger_name STRING, class_name STRING, app_id STRING, `host_name` STRING, container_id STRING, `task_id` STRING, `task_name` STRING>
) WITH (
      'connector' = 'elasticsearch-universal',
      'hosts' = 'http://log-xxx-xxx.cn:9200',
      'index' = 'yarn_flink_warn_logs-{log_time|yyyy.MM.dd}',
      --   'socket.timeout' = '120',
      --   'connection.timeout' = '120',
      --   'connection.request-timeout' = '120',
      'sink.parallelism' = '10',
      'sink.bulk-flush.max-actions' = '2000',
      'sink.bulk-flush.max-size' = '5MB',
      'sink.bulk-flush.interval' = '10'
      );


insert into yarn_flink_warn_logs_es
select
    `source`,
    `timestamp` as logTime,
    TO_TIMESTAMP(FROM_UNIXTIME(`timestamp` / 1000,'yyyy-MM-dd HH:mm:ss')) AS log_time,
    content,
    tags
from
    yarn_flink_warn_logs
where
    (
                tags.level = 'WARN' or tags.level = 'ERROR' or tags.container_type = 'jobmanager'
        )
  and
    (
        REGEXP(tags.class_name, 'xxx') is false
        )
  and
    (REGEXP(content, 'rror') is true
        or
     REGEXP(content, 'xception') is true
        or
     REGEXP(content, 'OOMKilled') is true)
  and
    (
        REGEXP(content, 'No JAAS configuration') is false
        )
  and
    (
        REGEXP(content, 'error while sniffing nodes') is false
        )
  and
    (
        REGEXP(content, 'document_missing_exception') is false
        )
  and
    (
        REGEXP(content, 'pipeline.classpaths') is false
        )
  and
    (
        REGEXP(content, 'InstanceAlreadyExistsException') is false
        )
  and
    (
        REGEXP(content, 'metrics-yarn-flink-jobs') is false
        )
  and
    (
        REGEXP(content, 'exceeded the 80 characters length limit and was truncated') is false
        )
  and
    (
        REGEXP(content, 'Error while reporting metrics') is false
        )
  and
    (
        REGEXP(content, 'flink-conf.yaml') is false
        )
  and
    (
        REGEXP(content, 'Loading configuration') is false
        )
  and
    (
        REGEXP(content, 'PushGateway') is false
        )
;
