---
toc: true
title: 《Flink 实战与性能优化》—— Flink 环境搭建
date: 2021-07-08
tags:
- Flink
- 大数据
- 流式计算
---

## 2.2 Flink 环境搭建

<!--more-->

在 2.1 节中已经将 Flink 的准备环境已经讲完了，本章节将带大家正式开始接触 Flink，那么我们得先安装一下 Flink。Flink 是可以在多个平台（Windows、Linux、Mac）上安装的。在开始写本书的时候最新版本是 1.8 版本，但是写到一半后更新到 1.9 了（合并了大量 Blink 的新特性），所以笔者又全部更新版本到 1.9，书籍后面也都是基于最新的版本讲解与演示。

Flink 的官网地址是：[https://flink.apache.org/](https://flink.apache.org/)


### 2.2.1 Flink 下载与安装

Flink 在 Mac、Linux、Window 平台上的安装方式如下。

#### 在 Mac 和 Linux 下安装

你可以通过该地址 [https://flink.apache.org/downloads.html](https://flink.apache.org/downloads.html) 下载到最新版本的 Flink。这里我们选择 `Apache Flink 1.9.0 for Scala 2.11` 版本，点击跳转到了一个镜像下载选择的地址，如下图所示，随便选择哪个就行，只是下载速度不一致而已。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-12-15-080110.png)

下载完后，你就可以直接解压下载的 Flink 压缩包了。接下来我们可以启动一下 Flink，我们进入到 Flink 的安装目录下执行命令 `./bin/start-cluster.sh` 即可，产生的日志如下：

```
zhisheng@zhisheng /usr/local/flink-1.9.0  ./bin/start-cluster.sh
Starting cluster.
Starting standalonesession daemon on host zhisheng.
Starting taskexecutor daemon on host zhisheng.
```

如果你的电脑是 Mac 的话，那么你也可以通过 Homebrew 命令进行安装。先通过命令 `brew search flink` 查找一下包：

```
 zhisheng@zhisheng  ~  brew search flink
==> Formulae
apache-flink ✔       homebrew/linuxbrew-core/apache-flink
```

可以发现找得到 Flink 的安装包，但是这样安装的版本可能不是最新的，如果你要安装的话，则使用命令：

```
brew install apache-flink
```

那么它就会开始进行下载并安装好，安装后的目录应该是在 `/usr/local/Cellar/apache-flink` 下，如下图所示：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-23-030606.png)

你可以通过下面命令检查安装的 Flink 到底是什么版本的：

```
flink --version
```

结果如下：

```
Version: 1.9.0, Commit ID: ff472b4
```

这种的话运行是得进入 `/usr/local/Cellar/apache-flink/1.9.0/libexec/bin` 目录下执行命令 `./start-cluster.sh` 才可以启动 Flink 的。执行命令后的启动日志如下所示：

```
Starting cluster.
Starting standalonesession daemon on host zhisheng.
Starting taskexecutor daemon on host zhisheng.
```

#### 在 Windows 下安装

如果你的电脑系统是 Windows 的话，那么你就直接双击 Flink 安装目录下面 bin 文件夹里面的 `start-cluster.bat` 就行，同样可以将 Flink 起动成功。


### 2.2.2 Flink 启动与运行

启动成功后的话，我们可以通过访问地址`http://localhost:8081/` 查看 UI 长啥样了，如下图所示：


### 2.2.3 Flink 目录配置文件解读

### 2.2.4 Flink 源码下载


### 2.2.5 Flink 源码编译

### 2.2.6 将 Flink 源码导入到 IDE


加入知识星球可以看到上面文章：https://t.zsxq.com/JyRzVnU

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-09-25-zsxq.jpg)



### 2.2.7 小结与反思

本节主要讲了 FLink 在不同系统下的安装和运行方法，然后讲了下怎么去下载源码和将源码导入到 IDE 中。不知道你在将源码导入到 IDE 中是否有遇到什么问题呢？