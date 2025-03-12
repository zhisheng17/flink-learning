

k8s application 任务提交命令

```shell
/app/flink-1.16.1-sql/bin/flink run-application -t kubernetes-application -Dkubernetes.cluster-id=batch-flink-373362-1741687273399 -Dexecution.job-listeners=com.zhisheng.plugins.jobStatusListener -Djob.alert.ownerList=xxx -Denv.java.opts.taskmanager="-DtaskName=1618789_1739795455429 -DtaskId=373362" -Denv.java.opts.jobmanager="-DtaskName=1618789_1739795455429 -DtaskId=373362" -Denv.java.opts.client="-DtaskId=373362" -Dkubernetes.container.image=harbor.xxx/bigdata/links_sql_116_common_image_pro:2024122414 -Dkubernetes.config.file=/app/zhisheng/k8s/config_idc2 -Dkubernetes.namespace=dataxxx -Dcontainerized.master.env.LINKS_JOB_FILE=oss://xxx/release/373362.sql -Dcontainerized.master.env.relayJarsOssPath=oss:///functions/FlinkUDF-flink-dataman-udf.jar,oss://prod/external/iceberg_flink1.16/iceberg-flink-runtime-1.16-1.4.0-tag-hellobike-20231027.jar,oss://functions/celeborn-client-flink-1.16-shaded_2.12-0.5.0.jar -Dcontainerized.master.env.LINKS_USER_NAME=xxx_deploy -Dcontainerized.taskmanager.env.LINKS_JOB_FILE=oss://sql/xxx_deploy/release/373362.sql -Dcontainerized.taskmanager.env.relayJarsOssPath=oss://functions/FlinkUDF-flink-dataman-udf.jar,oss://external/iceberg_flink1.16/iceberg-flink-runtime-1.16-1.4.0-tag-hellobike-20231027.jar,oss://functions/celeborn-client-flink-1.16-shaded_2.12-0.5.0.jar -Dcontainerized.taskmanager.env.LINKS_USER_NAME=xxx_deploy -Dtaskmanager.numberOfTaskSlots=1 -Djobmanager.memory.process.size=2048m -Dtaskmanager.memory.process.size=3072m -Dparallelism.default=1 local:///opt/flink/usrlib/links-sql.jar -w /opt/flink/usrlib -f 373362.sql -t false -k8d batch-flink-373362-1741687273399 -b true
```

注意：

`-w /opt/flink/usrlib -f 373362.sql -t false -k8d batch-flink-373362-1741687273399 -b true`

这段内容是 Flink SQL 命令，其中 `-f 373362.sql` 是指 SQL 文件的路径，`-t false` 是指是否是临时任务，`-k8d batch-flink-373362-1741687273399` 是指 Flink 的 Appliation ID，`-b true` 是指是否是 Batch 任务。