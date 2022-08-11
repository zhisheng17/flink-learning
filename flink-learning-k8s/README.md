### flink-learning-k8s

1、自定义构建 Flink 镜像

```shell
./build_flink_docker_images.sh flink $imageTag

eg: ./build_flink_docker_images.sh flink 1.12.0-jar-pro-20220727
```

2、Flink 任务提交到 K8s 集群，不同的运行模式：

+ Session mode
+ Native Application mode
+ Flink K8s Operator
+ Standalone mode

3、Ingress

```shell
./build_ingress.sh $cluster.id $namespace

eg: ./build_ingress.sh statemachine-test1 namespace-flink
```