---
toc: true
title: 《Flink 实战与性能优化》—— Flink Connector —— HBase 的用法
date: 2021-07-21
tags:
- Flink
- 大数据
- 流式计算
---

## 3.10 Flink Connector —— HBase 的用法

HBase 是一个分布式的、面向列的开源数据库，同样，很多公司也有使用该技术存储数据的，本节将对 HBase 做些简单的介绍，以及利用 Flink HBase Connector 读取 HBase 中的数据和写入数据到 HBase 中。

<!--more-->

### 3.10.1 准备环境和依赖

下面分别讲解 HBase 的环境安装、配置、常用的命令操作以及添加项目需要的依赖。

#### HBase 安装

如果是苹果系统，可以使用 HomeBrew 命令安装：

```
brew install hbase
```

HBase 最终会安装在路径 `/usr/local/Cellar/hbase/` 下面，安装版本不同，文件名也不同。

#### 配置 HBase

打开 `libexec/conf/hbase-env.sh` 修改里面的 JAVA_HOME：

```
# The java implementation to use.  Java 1.7+ required.
export JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk1.8.0_152.jdk/Contents/Home"
```

根据你自己的 JAVA_HOME 来配置这个变量。

打开 `libexec/conf/hbase-site.xml` 配置 HBase 文件存储目录:

```xml
<configuration>
  <property>
    <name>hbase.rootdir</name>
    <!-- 配置HBase存储文件的目录 -->
    <value>file:///usr/local/var/hbase</value>
  </property>
  <property>
    <name>hbase.zookeeper.property.clientPort</name>
    <value>2181</value>
  </property>
  <property>
    <name>hbase.zookeeper.property.dataDir</name>
    <!-- 配置HBase存储内建zookeeper文件的目录 -->
    <value>/usr/local/var/zookeeper</value>
  </property>
  <property>
    <name>hbase.zookeeper.dns.interface</name>
    <value>lo0</value>
  </property>
  <property>
    <name>hbase.regionserver.dns.interface</name>
    <value>lo0</value>
  </property>
  <property>
    <name>hbase.master.dns.interface</name>
    <value>lo0</value>
  </property>

</configuration>
```

#### 运行 HBase

执行启动的命令：

```
./bin/start-hbase.sh
```

执行后打印出来的日志如：

```
starting master, logging to /usr/local/var/log/hbase/hbase-zhisheng-master-zhisheng.out
```

#### 验证是否安装成功

使用 jps 命令：

```
zhisheng@zhisheng  /usr/local/Cellar/hbase/1.2.9/libexec  jps
91302 HMaster
62535 RemoteMavenServer
1100
91471 Jps
```

出现 HMaster 说明安装运行成功。

#### 启动 HBase Shell

执行下面命令：

```
./bin/hbase shell
```

运行结果如下图所示：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-05-04-035328.jpg)

#### 停止 HBase

执行下面的命令：

```
./bin/stop-hbase.sh
```

运行结果如下图所示：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-05-04-035513.jpg)

#### HBase 常用命令

HBase 中常用的命令有：list（列出已存在的表）、create（创建表）、put（写数据）、get（读数据）、scan（读数据，读全表）、describe（显示表详情），如下图所示。

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-05-04-040821.jpg)

简单使用上诉命令的结果如下：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-05-04-040230.jpg)

#### 添加依赖

在 pom.xml 中添加 HBase 相关的依赖：

```xml
<dependency>
 <groupId>org.apache.flink</groupId>
 <artifactId>flink-hbase_${scala.binary.version}</artifactId>
 <version>${flink.version}</version>
</dependency>
<dependency>
 <groupId>org.apache.hadoop</groupId>
 <artifactId>hadoop-common</artifactId>
 <version>2.7.4</version>
</dependency>
```

Flink HBase Connector 中，HBase 不仅可以作为数据源，也还可以写入数据到 HBase 中去，我们先来看看如何从 HBase 中读取数据。


### 3.10.2 Flink 使用 TableInputFormat 读取 HBase 批量数据

这里我们使用 TableInputFormat 来读取 HBase 中的数据，首先准备数据。

#### 准备数据

先往 HBase 中插入五条数据如下：

```jshelllanguage
put 'zhisheng', 'first', 'info:bar', 'hello'
put 'zhisheng', 'second', 'info:bar', 'zhisheng001'
put 'zhisheng', 'third', 'info:bar', 'zhisheng002'
put 'zhisheng', 'four', 'info:bar', 'zhisheng003'
put 'zhisheng', 'five', 'info:bar', 'zhisheng004'
```

scan 整个 `zhisheng` 表的话，有五条数据，运行结果如下图所示：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-05-04-073344.jpg)

#### Flink Job 代码

Flink 读取 HBase 数据的程序代码如下所示：

```java
/**
 * Desc: 读取 HBase 数据
 */
public class HBaseReadMain {
    //表名
    public static final String HBASE_TABLE_NAME = "zhisheng";
    // 列族
    static final byte[] INFO = "info".getBytes(ConfigConstants.DEFAULT_CHARSET);
    //列名
    static final byte[] BAR = "bar".getBytes(ConfigConstants.DEFAULT_CHARSET);

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.createInput(new TableInputFormat<Tuple2<String, String>>() {
            private Tuple2<String, String> reuse = new Tuple2<String, String>();
            @Override
            protected Scan getScanner() {
                Scan scan = new Scan();
                scan.addColumn(INFO, BAR);
                return scan;
            }
            @Override
            protected String getTableName() {
                return HBASE_TABLE_NAME;
            }
            @Override
            protected Tuple2<String, String> mapResultToTuple(Result result) {
                String key = Bytes.toString(result.getRow());
                String val = Bytes.toString(result.getValue(INFO, BAR));
                reuse.setField(key, 0);
                reuse.setField(val, 1);
                return reuse;
            }
        }).filter(new FilterFunction<Tuple2<String, String>>() {
            @Override
            public boolean filter(Tuple2<String, String> value) throws Exception {
                return value.f1.startsWith("zhisheng");
            }
        }).print();
    }
}
```

上面代码中将 HBase 中的读取全部读取出来后然后过滤以 `zhisheng` 开头的 value 数据。读取结果如下图所示：

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-12-14-161125.png)

可以看到输出的结果中已经将以 `zhisheng` 开头的四条数据都打印出来了。


### 3.10.3 Flink 使用 TableOutputFormat 向 HBase 写入数据

#### 添加依赖


#### Flink Job 代码


### 3.10.4 Flink 使用 HBaseOutputFormat 向 HBase 实时写入数据


#### 读取数据


#### 写入数据

#### 配置文件



### 3.10.5 项目运行及验证


### 3.10.6 小结与反思


加入知识星球可以看到上面文章：https://t.zsxq.com/3bimqBM

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-09-25-zsxq.jpg)


