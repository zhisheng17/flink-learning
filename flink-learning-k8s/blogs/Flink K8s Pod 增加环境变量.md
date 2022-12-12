### 问题

为啥要增加环境变量？---> 默认已有的环境变量不能满足需求，我们会从作业的 env 中获取到作业的 POD IP/物理机器 IP/Cluster.id/实时平台注入的 taskid/taskname，把这些 label 打进收集到的 metrics 和 log 里面，方便后期查询和告警。

![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/2022-12-12-115936.jpg)

![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/2022-12-12-115951.jpg)

### 解决办法

注入需要的 env 到 pod 里面去，代码如下：

JM 

```java
.addNewEnv()
    .withName(ENV_FLINK_HOST_IP_ADDRESS)
    .withValueFrom(new EnvVarSourceBuilder()
        .withNewFieldRef(API_VERSION, HOST_IP_FIELD_PATH)
        .build())
    .endEnv()
.addNewEnv()
    .withName("CLUSTER_ID")
    .withValue(kubernetesJobManagerParameters.getClusterId())
    .endEnv()
```

![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/2022-12-12-120030.jpg)


TM

```java
.addNewEnv()
    .withName(ENV_FLINK_HOST_IP_ADDRESS)
    .withValueFrom(new EnvVarSourceBuilder()
    .withNewFieldRef(API_VERSION, HOST_IP_FIELD_PATH)
    .build())
    .endEnv()
.addNewEnv()
    .withName(ENV_FLINK_POD_IP_ADDRESS)
    .withValueFrom(new EnvVarSourceBuilder()
    .withNewFieldRef(API_VERSION, POD_IP_FIELD_PATH)
    .build())
    .endEnv()
.addNewEnv()
    .withName("CLUSTER_ID")
    .withValue(kubernetesTaskManagerParameters.getClusterId())
    .endEnv()
```

![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/2022-12-12-120058.jpg)


### 最终结果

可以看到加到 env 里面的环境变量已经 OK 了


![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/2022-12-12-120127.jpg)