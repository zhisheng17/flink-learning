## 作业 HA

### ZK

```shell
./bin/flink run-application -p 1 -t kubernetes-application \
  -Dkubernetes.cluster-id=state-machine-cluster \
  -Dtaskmanager.memory.process.size=1024m \
  -Dkubernetes.taskmanager.cpu=0.5 \
  -Dtaskmanager.numberOfTaskSlots=1 \
  -Dkubernetes.container.image=harbor.xxx.cn/flink/statemachine:v0.0.6 \
  -Dkubernetes.namespace=hke-flink \
  -Dkubernetes.jobmanager.service-account=flink \
  -Dkubernetes.container.image.pull-secrets=docker-registry-test \
  -Dkubernetes.jobmanager.node-selector=kubernetes.io/role:flink-node \
  -Dkubernetes.taskmanager.node-selector=kubernetes.io/role:flink-node \
  -Dkubernetes.rest-service.exposed.type=NodePort \
  -Dhigh-availability=zookeeper \
  -Dhigh-availability.storageDir=hdfs:///flink/ha/k8s \
  local:///opt/flink/usrlib/StateMachineExample.jar
```

使用高可用后作业的 job id 变成了 00000000000000000000000000000000


![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/2022-12-12-114349.jpg)

在 ZK 生成的文件目录为下：

![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/2022-12-12-114408.jpg)

在 HDFS 生成的文件目录为下：

![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/2022-12-12-114426.jpg)

因为 flink checkpoint 的状态生成路径规则是：

```shell
hdfs:/flink/checkpoints/{jobid}/chk-xxx 
```

那么 application mode 下所有作业的 id 都是 00000000000000000000000000000000，那么这些相同的作业 id 会将 checkpoint 数据都放在同一个路径下，这样会让作业状态文件看起来很混乱。

```shell
hdfs:/flink/checkpoints/00000000000000000000000000000000/chk-84225 
```

![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/2022-12-12-114516.jpg)

**解决办法**：

平台提交作业的时候为每个作业单独设置一个 checkpoint 路径当作启动参数，规则如下：

```shell
-Dstate.checkpoints.dir=hdfs:///flink/checkpoints/{kubernetes.cluster-id}
```

这样每个作业的 checkpoint 路径可以保持独立的

![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/2022-12-12-114606.jpg)


## Kubernetes

https://flink.apache.org/2021/02/10/native-k8s-with-ha.html

https://cwiki.apache.org/confluence/display/FLINK/FLIP-144%3A+Native+Kubernetes+HA+for+Flink

![](https://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/2022-12-12-114723.jpg)

```shell

./bin/flink run-application -p 1 -t kubernetes-application \
  -Dkubernetes.cluster-id=state-machine-cluster-test14 \
  -Dtaskmanager.memory.process.size=1024m \
  -Dkubernetes.taskmanager.cpu=0.5 \
  -Dtaskmanager.numberOfTaskSlots=1 \
  -Dkubernetes.container.image=harbor.xxx.cn/flink/statemachine:v0.0.6 \
  -Dkubernetes.namespace=hke-flink \
  -Dkubernetes.jobmanager.service-account=flink \
  -Dkubernetes.container.image.pull-secrets=docker-registry-test \
  -Dkubernetes.jobmanager.node-selector=kubernetes.io/role:flink-node \
  -Dkubernetes.taskmanager.node-selector=kubernetes.io/role:flink-node \
  -Dkubernetes.rest-service.exposed.type=NodePort \
  -Dhigh-availability=org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory \
  -Dhigh-availability.storageDir=hdfs:///flink/ha/k8s \
  -Dstate.checkpoints.dir=hdfs:///flink/checkpoints/state-machine-cluster-test14 \
  local:///opt/flink/usrlib/StateMachineExample.jar
```