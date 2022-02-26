---
toc: true
title: 《Flink 实战与性能优化》—— Flink 作业如何在 Standalone、YARN、Mesos、K8S 上部署运行？
date: 2021-08-04
tags:
- Flink
- 大数据
- 流式计算
---


## 7.2 Flink 作业如何在 Standalone、YARN、Mesos、K8S 上部署运行？

前面章节已经有很多学习案列带大家使用 Flink，不仅有讲将 Flink 应用程序在 IDEA 中运行，也有讲将 Flink Job 编译打包上传到 Flink UI 上运行，在这 UI 背后可能是通过 Standalone、YARN、Mesos、Kubernetes 等运行启动的 Flink。那么这节就系统讲下如何部署和运行我们的 Flink Job，大家可以根据自己公司的场景进行选择使用哪种方式进行部署 Flink 作业！


<!--more-->

### 7.2.1 Standalone

第一种方式就是 Standalone 模式，这种模式笔者在前面 2.2 节里面演示的就是这种，我们通过执行命令：`./bin/start-cluster.sh` 启动一个 Flink Standalone 集群。

```
zhisheng@zhisheng  /usr/local/flink-1.9.0  ./bin/start-cluster.sh
Starting cluster.
Starting standalonesession daemon on host zhisheng.
Starting taskexecutor daemon on host zhisheng.
```

默认的话是启动一个 JobManager 和一个 TaskManager，我们可以通过 `jps` 查看进程有：

```
65425 Jps
51572 TaskManagerRunner
51142 StandaloneSessionClusterEntrypoint
```

其中上面的 TaskManagerRunner 代表的是 TaskManager 进程，StandaloneSessionClusterEntrypoint 代表的是 JobManager 进程。上面运行产生的只有一个 JobManager 和一个 TaskManager，如果是生产环境的话，这样的配置肯定是不够运行多个 Job 的，那么我们该如何在生产环境中配置 standalone 模式的集群呢？我们就需要修改 Flink 安装目录下面的 conf 文件夹里面配置：

```
flink-conf.yaml
masters
slaves
```

将 slaves 中再添加一个 `localhost`，这样就可以启动两个 TaskManager 了。接着启动脚本 `start-cluster.sh`，启动日志显示如下图所示。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-23-161333.png)

可以看见有两个 TaskManager 启动了，再看下 UI 显示的也是有两个 TaskManager，如下图所示。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-23-161431.png)

那么如果还想要添加一个 JobManager 或者 TaskManager 怎么办？总不能再次重启修改配置文件后然后再重启吧！这里你可以这样操作。

**增加一个 JobManager**：

```
bin/jobmanager.sh ((start|start-foreground) [host] [webui-port])|stop|stop-all
```

但是注意 Standalone 下一台机器最多只能运行一个 JobManager。

**增加一个 TaskManager**：

```
bin/taskmanager.sh start|start-foreground|stop|stop-all
```

比如我执行了 `./bin/taskmanager.sh start` 命令后，运行结果如下图所示。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-23-161657.png)

Standalone 模式下可以先对 Flink Job 通过 `mvn clean package` 编译打包，得到 Jar 包后，可以在 UI 上直接上传 Jar 包，然后点击 Submit 就可以运行了。


### 7.2.2 YARN


### 7.2.3 Mesos

#### Session 集群

#### Per Job 集群


### 7.3.4 Kubernetes


### 7.2.5 小结与反思


加入知识星球可以看到上面文章：https://t.zsxq.com/f66iAMz

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-09-25-zsxq.jpg)



本章讲解了 Flink 中所有的配置文件，每个配置文件中的配置有啥作用，并且如果在不同环境下配置 JobManager 的高可用，另外还介绍了 Flink 的部署问题，因为 Flink 本身是支持在不同的环境下部署的，比如 Standalone、K8S、YARN、Mesos 等，其中在调度平台上又有 Session 模式和 Per Job 模式，每种模式都有自己的特点，所以你可能需要根据公司的情况来做一定的选型，每种的部署也可能会有点不一样，遇到问题的化还得根据特殊情况进行特殊处理，希望你可以在公司灵活的处理这种问题。