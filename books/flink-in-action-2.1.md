---
toc: true
title: 《Flink 实战与性能优化》—— Flink 环境准备
date: 2021-07-07
tags:
- Flink
- 大数据
- 流式计算
---


# 第二章 —— Flink 入门

通过第一章对 Flink 的介绍，相信你对 Flink 的概念和特性有了一定的了解，接下来本章将开始正式进入 Flink 的学习之旅，笔者将带你搭建 Flink 的环境和编写两个案例（WordCount 程序、读取 Socket 数据）来入门 Flink。


## 2.1 Flink 环境准备

<!--more-->


通过前面的章节内容，相信你已经对 Flink 的基础概念等知识已经有一定了解，现在是不是迫切的想把 Flink 给用起来？先别急，我们先把电脑的准备环境给安装好，这样后面才能更愉快地玩耍。

废话不多说了，直奔主题。因为本书后面章节内容会使用 Kafka、MySQL、ElasticSearch 等组件，并且运行 Flink 程序是需要依赖 Java 的，另外就是我们需要使用 IDE 来开发 Flink 应用程序以及使用 Maven 来管理 Flink 应用程序的依赖，所以本节我们提前安装这个几个组件，搭建好本地的环境，后面如果还要安装其他的组件笔者会在对应的章节中补充，如果你的操作系统已经中已经安装过 JDK、Maven、MySQL、IDEA 等，那么你可以跳过对应的内容，直接看你未安装过的。

这里笔者再说下自己电脑的系统环境：macOS High Sierra 10.13.5，后面文章的演示环境不作特别说明的话就是都在这个系统环境中。


### 2.1.1 JDK 安装与配置

虽然现在 JDK 已经更新到 12 了，但是为了稳定我们还是安装 JDK 8，如果没有安装过的话，可以去[官网](https://www.oracle.com/technetwork/java/javase/downloads/index.html) 的[下载页面](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)下载对应自己操作系统的最新 JDK8 就行。

Mac 系统的是 `jdk-8u211-macosx-x64.dmg` 格式、Linux 系统的是 `jdk-8u211-linux-x64.tar.gz` 格式。Mac 系统安装的话直接双击然后一直按照提示就行了，最后 JDK 的安装目录在 `/Library/Java/JavaVirtualMachines/` ，然后在 `/etc/hosts` 中配置好环境变量（注意：替换你自己电脑本地的路径）。

```
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_152.jdk/Contents/Home
export CLASSPATH=$JAVA_HOME/lib/tools.jar:$JAVA_HOME/lib/dt.jar:
export PATH=$PATH:$JAVA_HOME/bin
```

Linux 系统的话就是在某个目录下直接解压就行了，然后在 `/etc/profile` 添加一下上面的环境变量（注意：替换你自己电脑的路径）。然后执行 `java -version` 命令可以查看是否安装成功！

```
 zhisheng@zhisheng ~  java -version
java version "1.8.0_152"
Java(TM) SE Runtime Environment (build 1.8.0_152-b16)
Java HotSpot(TM) 64-Bit Server VM (build 25.152-b16, mixed mode)
```


### 2.1.2 Maven 安装与配置

安装好 JDK 后我们就可以安装 Maven 了，我们在[官网](http://maven.apache.org/download.cgi)下载二进制包就行，然后在自己本地软件安装目录解压压缩包就行。接下来你需要配置一下环境变量：

```
export M2_HOME=/Users/zhisheng/Documents/maven-3.5.2
export PATH=$PATH:$M2_HOME/bin
```

然后执行命令 `mvn -v` 可以验证是否安装成功，结果如下：

```
zhisheng@zhisheng ~ /Users  mvn -v
Apache Maven 3.5.2 (138edd61fd100ec658bfa2d307c43b76940a5d7d; 2017-10-18T15:58:13+08:00)
Maven home: /Users/zhisheng/Documents/maven-3.5.2
Java version: 1.8.0_152, vendor: Oracle Corporation
Java home: /Library/Java/JavaVirtualMachines/jdk1.8.0_152.jdk/Contents/Home/jre
Default locale: zh_CN, platform encoding: UTF-8
OS name: "mac os x", version: "10.13.5", arch: "x86_64", family: "mac"
```


### 2.1.3 IDE 安装与配置

安装完 JDK 和 Maven 后，就可以安装 IDE 了，大家可以选择你熟练的 IDE 就行，笔者后面演示的代码都是在 IDEA 中运行的，如果想为了后面不出其他的问题的话，建议尽量和笔者的环境保持一致。

IDEA 官网下载地址：[下载页面的地址](https://www.jetbrains.com/idea/download/#section=mac)，下载后可以双击后然后按照提示一步步安装，安装完成后需要在 IDEA 中配置 JDK 路径和 Maven 的路径，后面我们开发也都是靠 Maven 来管理项目的依赖。


### 2.1.4 MySQL 安装与配置



### 2.1.5 Kafka 安装与配置



### 2.1.6 ElasticSearch 安装与配置


加入知识星球可以看到上面文章：https://t.zsxq.com/JyRzVnU

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-09-25-zsxq.jpg)




### 2.1.7 小结与反思

本节讲解了下 JDK、Maven、IDE、MySQL、Kafka、ElasticSearch 的安装与配置，因为这些都是后面要用的，所以这里单独抽一篇文章来讲解环境准备的安装步骤，当然这里还并不涉及全，因为后面我们还可能会涉及到 HBase、HDFS 等知识，后面我们用到再看，本书的内容主要讲解 Flink，所以更多的环境准备还是得靠大家自己独立完成。

这里笔者说下笔者自己一般安装环境的选择：

xxx

下面章节我们就正式进入 Flink 专题了！