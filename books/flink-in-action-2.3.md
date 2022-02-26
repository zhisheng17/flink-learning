---
toc: true
title: 《Flink 实战与性能优化》—— 案例1：WordCount 应用程序
date: 2021-07-09
tags:
- Flink
- 大数据
- 流式计算
---

## 2.3 案例1：WordCount 应用程序

在 2.2 节中带大家讲解了下 Flink 的环境安装，这篇文章就开始我们的第一个 Flink 案例实战，也方便大家快速开始自己的第一个 Flink 应用。大数据里学习一门技术一般都是从 WordCount 开始入门的，那么笔者还是不打破常规了，所以这篇文章笔者也将带大家通过 WordCount 程序来初步了解 Flink。

<!--more-->

### 2.3.1 使用 Maven 创建项目

Flink 支持 Maven 直接构建模版项目，你在终端使用该命令：

```
mvn archetype:generate                               \
      -DarchetypeGroupId=org.apache.flink              \
      -DarchetypeArtifactId=flink-quickstart-java      \
      -DarchetypeVersion=1.9.0
```

在执行的过程中它会提示你输入 groupId、artifactId、和 package 名，你按照要求输入就行，最后就可以成功创建一个项目，如下图所示。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-17-151203.png)

进入到目录你就可以看到已经创建了项目，里面结构如下：

```
 zhisheng@zhisheng  ~/IdeaProjects/github/Flink-WordCount  tree
.
├── pom.xml
└── src
    └── main
        ├── java
        │   └── com
        │       └── zhisheng
        │           ├── BatchJob.java
        │           └── StreamingJob.java
        └── resources
            └── log4j.properties

6 directories, 4 files
```

该项目中包含了两个类 BatchJob 和 StreamingJob，另外还有一个 log4j.properties 配置文件，然后你就可以将该项目导入到 IDEA 了。你可以在该目录下执行 `mvn clean package` 就可以编译该项目，编译成功后在 target 目录下会生成一个 Job 的 Jar 包，但是这个 Job 还不能执行，因为 StreamingJob 这个类中的 main 方法里面只是简单的创建了 StreamExecutionEnvironment 环境，然后就执行 execute 方法，这在 Flink 中是不算一个可执行的 Job 的，因此如果你提交到 Flink UI 上也是会报错的。

如下图所示，上传作业程序打包编译的 Jar 包：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-17-151434.png)

运行报错结果如下图所示：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-17-152026.png)

```
Server Response Message:
Internal server error.
```

我们查看 Flink JobManager 的日志，可以看见错误信息如下图所示：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-10-17-152954.png)

```
2019-04-26 17:27:33,706 ERROR org.apache.flink.runtime.webmonitor.handlers.JarRunHandler    - Unhandled exception.
org.apache.flink.client.program.ProgramInvocationException: The main method caused an error: No operators defined in streaming topology. Cannot execute.
```

因为 execute 方法之前我们是需要补充我们 Job 的一些算子操作的，所以报错还是很正常的，本节下面将会提供完整代码。


### 2.3.2 使用 IDEA 创建项目

一般我们项目可能是由多个 Job 组成，并且代码也都是在同一个工程下面进行管理，上面那种创建方式适合单个 Job 执行，但如果在公司多人合作的时候还是得在同一个工程下面创建项目，每个 Flink Job 对应着一个 module，该 module 负责独立的业务逻辑，比如笔者在 GitHub 的 [https://github.com/zhisheng17/flink-learning](https://github.com/zhisheng17/flink-learning) 项目，它的项目结构如下图所示：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-11-12-163154.png)

接下来我们需要在父工程的 pom.xml 中加入如下属性（含编码、Flink 版本、JDK 版本、Scala 版本、Maven 编译版本）：

```xml
<properties>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <!--Flink 版本-->
    <flink.version>1.9.0</flink.version>
    <!--JDK 版本-->
    <java.version>1.8</java.version>
    <!--Scala 2.11 版本-->
    <scala.binary.version>2.11</scala.binary.version>
    <maven.compiler.source>${java.version}</maven.compiler.source>
    <maven.compiler.target>${java.version}</maven.compiler.target>
</properties>
```

然后加入依赖：

```xml
<dependencies>
    <!-- Apache Flink dependencies -->
    <!-- These dependencies are provided, because they should not be packaged into the JAR file. -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-java</artifactId>
        <version>${flink.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-streaming-java_${scala.binary.version}</artifactId>
        <version>${flink.version}</version>
        <scope>provided</scope>
    </dependency>


    <!-- Add logging framework, to produce console output when running in the IDE. -->
    <!-- These dependencies are excluded from the application JAR by default. -->
    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-log4j12</artifactId>
        <version>1.7.7</version>
        <scope>runtime</scope>
    </dependency>
    <dependency>
        <groupId>log4j</groupId>
        <artifactId>log4j</artifactId>
        <version>1.2.17</version>
        <scope>runtime</scope>
    </dependency>
</dependencies>
```

上面依赖中 flink-java 和 flink-streaming-java 是我们 Flink 必备的核心依赖，为什么设置 scope 为 provided 呢（默认是 compile）？是因为 Flink 其实在自己的安装目录中 lib 文件夹里的 `lib/flink-dist_2.11-1.9.0.jar` 已经包含了这些必备的 Jar 了，所以我们在给自己的 Flink Job 添加依赖的时候最后打成的 Jar 包可不希望又将这些重复的依赖打进去。有两个好处：

+ 减小了我们打的 Flink Job Jar 包容量大小
+ 不会因为打入不同版本的 Flink 核心依赖而导致类加载冲突等问题

但是问题又来了，我们需要在 IDEA 中调试运行我们的 Job，如果将 scope 设置为 provided 的话，是会报错的：

```
Error: A JNI error has occurred, please check your installation and try again
Exception in thread "main" java.lang.NoClassDefFoundError: org/apache/flink/api/common/ExecutionConfig$GlobalJobParameters
	at java.lang.Class.getDeclaredMethods0(Native Method)
	at java.lang.Class.privateGetDeclaredMethods(Class.java:2701)
	at java.lang.Class.privateGetMethodRecursive(Class.java:3048)
	at java.lang.Class.getMethod0(Class.java:3018)
	at java.lang.Class.getMethod(Class.java:1784)
	at sun.launcher.LauncherHelper.validateMainClass(LauncherHelper.java:544)
	at sun.launcher.LauncherHelper.checkAndLoadMain(LauncherHelper.java:526)
Caused by: java.lang.ClassNotFoundException: org.apache.flink.api.common.ExecutionConfig$GlobalJobParameters
	at java.net.URLClassLoader.findClass(URLClassLoader.java:381)
	at java.lang.ClassLoader.loadClass(ClassLoader.java:424)
	at sun.misc.Launcher$AppClassLoader.loadClass(Launcher.java:338)
	at java.lang.ClassLoader.loadClass(ClassLoader.java:357)
	... 7 more
```

默认 scope 为 compile 的话，本地调试的话就不会出错了。另外测试到底能够减小多少 Jar 包的大小呢？我这里先写了个 Job 测试。当 scope 为 compile 时，编译后的 target 目录：

```
zhisheng@zhisheng  ~/Flink-WordCount/target   master ●✚  ll
total 94384
-rw-r--r--  1 zhisheng  staff    45M  4 26 21:23 Flink-WordCount-1.0-SNAPSHOT.jar
drwxr-xr-x  4 zhisheng  staff   128B  4 26 21:23 classes
drwxr-xr-x  3 zhisheng  staff    96B  4 26 21:23 generated-sources
drwxr-xr-x  3 zhisheng  staff    96B  4 26 21:23 maven-archiver
drwxr-xr-x  3 zhisheng  staff    96B  4 26 21:23 maven-status
-rw-r--r--  1 zhisheng  staff   7.2K  4 26 21:23 original-Flink-WordCount-1.0-SNAPSHOT.jar
```

当 scope 为 provided 时，编译后的 target 目录：

```
zhisheng@zhisheng ~/Flink-WordCount/target   master ●✚  ll
total 32
-rw-r--r--  1 zhisheng  staff   7.5K  4 26 21:27 Flink-WordCount-1.0-SNAPSHOT.jar
drwxr-xr-x  4 zhisheng  staff   128B  4 26 21:27 classes
drwxr-xr-x  3 zhisheng  staff    96B  4 26 21:27 generated-sources
drwxr-xr-x  3 zhisheng  staff    96B  4 26 21:27 maven-archiver
drwxr-xr-x  3 zhisheng  staff    96B  4 26 21:27 maven-status
-rw-r--r--  1 zhisheng  staff   7.2K  4 26 21:27 original-Flink-WordCount-1.0-SNAPSHOT.jar
```

。。。


### 2.3.3 流计算 WordCount 应用程序代码实现



### 2.3.4 运行流计算 WordCount 应用程序


#### 本地 IDE 运行

#### UI 运行 Job


### 2.3.5 流计算 WordCount 应用程序代码分析


### 2.3.6 小结与反思


加入知识星球可以看到上面文章：https://t.zsxq.com/Z7EAmq3

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-09-25-zsxq.jpg)
