---
toc: true
title: 《Flink 实战与性能优化》—— 案例2：实时处理 Socket 数据
date: 2021-07-10
tags:
- Flink
- 大数据
- 流式计算
---

## 2.4 案例2：实时处理 Socket 数据

在 2.3 节中讲解了 Flink 最简单的 WordCount 程序的创建、运行结果查看和代码分析，本节将继续带大家来看一个入门上手的程序：Flink 处理 Socket 数据。

<!--more-->

### 2.4.1 使用 IDEA 创建项目

使用 IDEA 创建新的 module，结构如下：

```
├── pom.xml
└── src
    ├── main
    │   ├── java
    │   │   └── com
    │   │       └── zhisheng
    │   │           └── socket
    │   │               └── Main.java
    │   └── resources
    │       └── log4j.properties
    └── test
        └── java
```

项目创建好了后，我们下一步开始编写 Flink Socket Job 的代码。


### 2.4.2 实时处理 Socket 数据应用程序代码实现

程序代码如下所示：

```java
public class Main {
    public static void main(String[] args) throws Exception {
        //参数检查
        if (args.length != 2) {
            System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
            return;
        }
        String hostname = args[0];
        Integer port = Integer.parseInt(args[1]);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据
        DataStreamSource<String> stream = env.socketTextStream(hostname, port);
        //计数
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream.flatMap(new LineSplitter())
                .keyBy(0)
                .sum(1);
        sum.print();
        env.execute("Java WordCount from SocketText");
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
            String[] tokens = s.toLowerCase().split("\\W+");

            for (String token: tokens) {
                if (token.length() > 0) {
                    collector.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
```

**pom.xml** 添加 build：

```xml
<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-compiler-plugin</artifactId>
            <version>3.1</version>
            <configuration>
                <source>${java.version}</source>
                <target>${java.version}</target>
            </configuration>
        </plugin>

        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-shade-plugin</artifactId>
            <version>3.0.0</version>
            <executions>
                <execution>
                    <phase>package</phase>
                    <goals>
                        <goal>shade</goal>
                    </goals>
                    <configuration>
                        <artifactSet>
                            <excludes>
                                <exclude>org.apache.flink:force-shading</exclude>
                                <exclude>com.google.code.findbugs:jsr305</exclude>
                                <exclude>org.slf4j:*</exclude>
                                <exclude>log4j:*</exclude>
                            </excludes>
                        </artifactSet>
                        <filters>
                            <filter>
                                <artifact>*:*</artifact>
                                <excludes>
                                    <exclude>META-INF/*.SF</exclude>
                                    <exclude>META-INF/*.DSA</exclude>
                                    <exclude>META-INF/*.RSA</exclude>
                                </excludes>
                            </filter>
                        </filters>
                        <transformers>
                            <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                <!--注意：这里一定要换成你自己的 Job main 方法的启动类-->
                                <mainClass>com.zhisheng.socket.Main</mainClass>
                            </transformer>
                        </transformers>
                    </configuration>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```


### 2.4.3 运行实时处理 Socket 数据应用程序

下面分别讲解在 IDE 和 Flink UI 上运行作业。

#### 本地 IDE 运行


#### UI 运行 Job



### 2.4.4 实时处理 Socket 数据应用程序代码分析




加入知识星球可以看到上面文章：https://t.zsxq.com/VBEQv3F

![](http://zhisheng-blog.oss-cn-hangzhou.aliyuncs.com/img/2019-09-25-zsxq.jpg)






### 2.4.5 Flink 中使用 Lambda 表达式

因为 Lambda 表达式看起来简洁，所以有时候也是希望在这些 Flink 作业中也可以使用上它，虽然 Flink 中是支持 Lambda，但是个人感觉不太友好。比如上面的应用程序如果将 LineSplitter 该类之间用 Lambda 表达式完成的话则要像下面这样写：

```java
stream.flatMap((s, collector) -> {
    for (String token : s.toLowerCase().split("\\W+")) {
        if (token.length() > 0) {
            collector.collect(new Tuple2<String, Integer>(token, 1));
        }
    }
})
        .keyBy(0)
        .sum(1)
        .print();
```

但是这样写完后，运行作业报错如下：

```
Exception in thread "main" org.apache.flink.api.common.functions.InvalidTypesException: The return type of function 'main(LambdaMain.java:34)' could not be determined automatically, due to type erasure. You can give type information hints by using the returns(...) method on the result of the transformation call, or by letting your function implement the 'ResultTypeQueryable' interface.
	at org.apache.flink.api.dag.Transformation.getOutputType(Transformation.java:417)
	at org.apache.flink.streaming.api.datastream.DataStream.getType(DataStream.java:175)
	at org.apache.flink.streaming.api.datastream.DataStream.keyBy(DataStream.java:318)
	at com.zhisheng.examples.streaming.socket.LambdaMain.main(LambdaMain.java:41)
Caused by: org.apache.flink.api.common.functions.InvalidTypesException: The generic type parameters of 'Collector' are missing. In many cases lambda methods don't provide enough information for automatic type extraction when Java generics are involved. An easy workaround is to use an (anonymous) class instead that implements the 'org.apache.flink.api.common.functions.FlatMapFunction' interface. Otherwise the type has to be specified explicitly using type information.
	at org.apache.flink.api.java.typeutils.TypeExtractionUtils.validateLambdaType(TypeExtractionUtils.java:350)
	at org.apache.flink.api.java.typeutils.TypeExtractionUtils.extractTypeFromLambda(TypeExtractionUtils.java:176)
	at org.apache.flink.api.java.typeutils.TypeExtractor.getUnaryOperatorReturnType(TypeExtractor.java:571)
	at org.apache.flink.api.java.typeutils.TypeExtractor.getFlatMapReturnTypes(TypeExtractor.java:196)
	at org.apache.flink.streaming.api.datastream.DataStream.flatMap(DataStream.java:611)
	at com.zhisheng.examples.streaming.socket.LambdaMain.main(LambdaMain.java:34)
```

根据上面的报错信息其实可以知道要怎么解决了，该错误是因为 Flink 在用户自定义的函数中会使用泛型来创建 serializer，当使用匿名函数时，类型信息会被保留。但 Lambda 表达式并不是匿名函数，所以 javac 编译的时候并不会把泛型保存到 class 文件里。解决方法：使用 Flink 提供的 returns 方法来指定 flatMap 的返回类型

```java
//使用 TupleTypeInfo 来指定 Tuple 的参数类型
.returns((TypeInformation) TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class))
```

在 flatMap 后面加上上面这个 returns 就行了，但是如果算子多了的话，每个都去加一个 returns，其实会很痛苦的，所以通常使用匿名函数或者自定义函数居多。


### 2.4.5 小结与反思



在第一章中介绍了 Flink 的特性，本章主要是让大家能够快速入门，所以在第一节和第二节中分别讲解了 Flink 的环境准备和搭建，在第三节和第四节中通过两个入门的应用程序（WordCount 应用程序和读取 Socket 数据应用程序）让大家可以快速入门 Flink，两个程序都是需要自己动手实操，所以更能加深大家的印象。