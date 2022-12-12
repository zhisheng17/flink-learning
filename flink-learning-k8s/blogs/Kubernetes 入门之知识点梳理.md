本篇文章是第一篇，从介绍 k8s 知识点和常见名词开始。
## Master
k8s 中作为大脑存在的是 Master 节点，是集群的控制节点，负责整个集群的管理和控制。所有的其他 Node 都会向 Master 注册自己，并定期上报自身的所有信息。在 Master 节点上运行着下面 4 种进程：

- Api Server ：提供 HTTP Rest 接口的服务进程；所有资源的增删改查操作的唯一入口；集群控制的入口， kubectl 就是直接对 Api Server 负责；
- Controller Manager ：所有资源对象的自动化控制中心；
- Scheduler ：负责资源的调度，主要将 Pod 调度到指定的 Node 上；
- etcd Server ：所有资源对象的数据保存在 etcd 中。
## Node
除了 Master 节点外，其他的节点都称为 Node，即工作节点，且接受 Master 的控制。Node 超过指定时间不上报信息时，会被 Master 判定为失联，则该 Node 的状态被标记为不可用，随后 Master 会触发"工作负载大转移"的自动流程。而 Node 上运行着下面 3 种主要的进程：

- kubelet ：负责 Pod 对应的容器的创建、启停等任务；与 Master 节点密切合作，实现集群管理的基本面功能，Node 向 Master 上报信息，就是通过 kubelet 实现的；
- kube-proxy ：实现 Service 的通信与负载均衡机制；
- Docker Engine ：负责本机容器的创建与管理。
## Namespace
用于实现多租户的资源隔离。通过将集群内部的资源对象分配到不同的 Namespace 中。创建资源对象时可以指定属于哪个 Namespace。
## Pod
Pod 是 k8s 的最小调度单元。

![](https://tva1.sinaimg.cn/large/008vxvgGly1h919917hccj30dp0bgmxa.jpg)

如上图所示，而 Pod 是由一个个容器组成的，称为容器组。而组成 Pod 的容器分为 Pause 容器和一个个业务容器。其中以 Pause 容器的状态代表整个 Pod 的状态，由于 Pause 容器不易死亡，这样就能保证对 Pod 这个整体的状态的判断；而所有的业务容器共享 Pause 的 IP 和 Volume，这样就解决了联系紧密的业务容器之间的通信和共享资源的问题。Pod 在哪个 Node 上工作是由 kubelet 调度的。
Pod 拥有唯一 IP ，k8s 以 Endpoint (pod_ip + containerPort) 作为 Pod 中一个服务进程的对外通信地址。而任意两个 Pod 之间直接 TCP/IP 通信，采用虚拟二层网络技术实现，如 Flannel、Openvswitch。而 Pod 的 Endpoint 与 Pod 同生命周期，当 Pod 被销毁，对应的 Endpoint 也随之被销毁。
Pod 有两种类型：

- 普通 Pod ，存放在 etcd 中，被调度到 Node 中进行绑定，调度后被 Node 中的 kubelet 实例化成一组容器并启动；
- 静态 Pod ，存放在某个具体的 Node 上的一个具体文件中，只在此 Node 中启动运行。
## Volume
定义在 Pod 上，被一个 Pod 里的多个容器挂载到具体文件目录下。需要注意的是 Volume 与 Pod 的生命周期相同。
作用：Pod 中多个容器共享文件；让容器的数据写到宿主机的磁盘上；写文件到网络存储中；容器配置文件集中化定义与管理。
### 类型

- emptyDir：Pod 分配到 Node 上时创建，无需指定宿主机上的目录文件。Pod 被移除时，emptyDir 上的数据被永久删除。
- hostPath：在 Pod 上挂载宿主机上的文件或目录。在不同 Node 上具有相同配置的 Pod 可能会因为宿主机上的目录和文件不同而导致对 Volume 上目录和文件的访问结果不一致；若使用了资源配额管理， k8s 无法将 hostPath 在宿主机上使用的资源纳入管理。
    - 用途：容器生成的日志文件需要永久保存；需要访问宿主机上 Docker 引擎，将 hostPath 定义为宿主机 /var/lib/docker 目录。
- 其他：如 gcePersistentDisk 、 awsElasticBlockStore 等，都是由特定的云服务提供的永久磁盘。Pod 结束时不会被删除，只会被卸载。使用时需要按照要求安装特定虚拟机和永久磁盘。
## Deployment
用于更好地解决 Pod 的编排问题，其内部使用 ReplicaSet 来实现目的。
使用场景：

- 生成 RS 并完成 Pod 副本的创建过程；
- 检查部署动作是否完成；
- 更新 Deployment 以创建新的 Pod；
- 回滚；
- 挂起或恢复。

Pod 数量的描述

- DESIRED：Pod 副本数量的期望值
- CURRENT：当前的副本数
- UP_TO_DATE：最新版本的 Pod 副本数
- AVAILABLE：当前集群中可用 Pod 副本数
## Label
定义形式：key=value。我们主要使用 Label Selector 来查询和筛选某些 Label 的资源对象。
使用场景：

- kube-controller 筛选要监控的 Pod
- kube-proxy 进程建立 Service 对 Pod 的请求转发路由表
- kube-scheduler 进程实现 Pod 定向调度
## Annotation
定义形式：key=value
与 Label 的区别：

- Label 有严格命名规则
- Label 定义的是 metadata ，且用于 Label Selector；Annotation 是用户任意定义的附加信息

使用场景：

- build 信息、 release 信息、 Docker 镜像信息等
- 日志库、监控库、分析库等资源库的地址信息
- 程序调试工具信息
- 团队的联系信息
## Replica Set
Replica Set(RS) 是 Replication Controller(RC) 的升级版本。两者的唯一区别是对选择器的支持。ReplicaSet 支持 labels user guide 中描述的 set-based 选择器要求， 而 Replication Controller 仅支持 equality-based 的选择器要求。
我们一般用 Deployment 来定义 RS，很少直接创建 RS，从而形成一套完整的 Pod 的创建、删除、更新的编排机制。
RS 中可以定义的是：Pod 期待的副本数（Replicas）；用于筛选目标 Pod 的 Label Selector；当 Pod 副本数小于预期数量时，用于创建新 Pod 的模板。
Master 的 Controller Manager 定期巡检系统中当前存活的目标 Pod，确保目标 Pod 实例数等于期望值。删除 RS 不会影响 Pod ，支持基于集合的 Label Selector；通过改变 RS 中 Pod 副本数量，实现 Pod 扩容和缩容；通过改变 RS 中 Pod 模板中的镜像版本，实现 Pod 的滚动升级。
## Service
定义：微服务架构中的微服务。

![](https://tva1.sinaimg.cn/large/008vxvgGly1h9199olemuj30ij0l9wf3.jpg)

Service 定义了一个服务的访问入口地址，客户端通过该入口地址访问背后的集群实例。Service 通过 Label Selector 与后端 Pod 副本集群之间实现对接。Service 之间通过 TCP/IP 通信。
### 负载均衡
kube-proxy 进程是一个智能的负载均衡器，负责将对 Service 的请求转发到后端的 Pod。Pod 的所有副本为一组，提供一个对外的服务端口，将这些 Pod 的 Endpoint 列表加入该端口的转发列表。客户端通过负载均衡的对外 IP + 服务端口来访问此服务。
### ClusterIP
Service 拥有全局唯一虚拟 IP，称为 ClusterIP，每个 Service 变成了具备全局唯一 IP 的通信节点。与 Pod 不同的是，Pod 的 Endpoint 会随 Pod 的销毁而发生改变，但 ClusterIP 在 Service 的生命周期中不会发生改变。并且只要用 Service 的 Name 与 Service 的 ClusterIP 做一个 DNS 域名映射，即可实现服务发现。
Service 中一般会定义一个 targetPort，即提供该服务的容器暴露的端口，具体业务进程在容器内的 targetPort 上提供 TCP/IP 接入；而 Service 的 port 属性定义了 Service 的虚接口。
### 服务发现
before：每个 Service 生成一些对应的 Linux 环境变量，Pod 容器启动时自动注入。
now：通过 Add-On 增值包的方式引入 DNS 系统，将服务名作为 DNS 域名即可实现。
### 外部系统访问 Service
k8s 中有三种类型的 IP：

- Node IP集群中每个节点的物理网卡的 IP 地址；所有属于这个网络的服务器之间都通过这个网络直接通信；集群之外的节点访问该集群时，必须通过 Node IP 通信。
- Pod IPDocker Engine 根据 docker0 网桥的 IP 地址段进行分配的；虚拟的二层网络；不同 Pod 的容器之间互相访问时，通过 Pod IP 所在的虚拟二层网络进行通信。
- Cluster IP
    - 仅仅作用于 Service ：由 kuber 管理和分配 IP 地址；
    - 无法被 ping：因为没有一个实体网络对象来响应；
    - Cluster IP 只能结合 Service Port 组成一个具体的通信端口：单独的 Cluster IP 不具备 TCP/IP 通信基础；属于 kuber 集群的封闭空间；集群外的节点若需要访问，需要一些额外的操作；
    - Node IP 网、Pod IP 网和 Cluster IP 网之间的通信是 kuber 自制的一种编程方式的路由规则；

k8s 中实现外部系统访问 Service 的方法，主要是通过 NodePort，其实现方式是在每个 Node 上为需要提供外部访问的 Service 开启一个对应的 TCP 监听端口。此时，外部系统只要用任意一个 Node 的 IP + NodePort 即可访问此服务。
