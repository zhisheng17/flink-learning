![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/2022-12-12-115151.jpg)


![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/2022-12-12-115208.jpg)

### OOM

在 K8s 集群下很常见的异常就是因内存使用超过配置的 limit 值而触发 OOMKilled 异常

```shell
2022-05-19 17:46:40,102 WARN  akka.remote.ReliableDeliverySupervisor                       [] - Association with remote system [akka.tcp://flink@10.73.131.241:6122] has failed, address is now gated for [50] ms. Reason: [Association failed with [akka.tcp://flink@10.73.131.241:6122]] Caused by: [java.net.ConnectException: Connection refused: /10.73.131.241:6122]
2022-05-19 17:46:40,261 WARN  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Worker flink-4760-1652949469041-taskmanager-1-3 is terminated. Diagnostics: Pod terminated, container termination statuses: [flink-task-manager(exitCode=137, reason=OOMKilled, message=null)], pod status: Failed(reason=null, message=null)
2022-05-19 17:46:40,262 WARN  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Closing TaskExecutor connection flink-4760-1652949469041-taskmanager-1-3 because: Pod terminated, container termination statuses: [flink-task-manager(exitCode=137, reason=OOMKilled, message=null)], pod status: Failed(reason=null, message=null)
```

![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/2022-12-12-115252.jpg)