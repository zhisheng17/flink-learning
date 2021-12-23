### flink-learning-libraries-machine-learning

TODO：

因在 Flink 1.9 版本中移除了 flink-ml 模块，所以暂不对这块做讲解，后面升级 Flink 版本时，该模块可以加入更多东西，doing！

//可以根据 1.8 版本的 Flink-ml 进行学习，引入依赖：

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-ml_${scala.binary.version}</artifactId>
    <version>1.8.0</version>
</dependency>
```

haberman.data 数据集是 '乳腺癌手术患者生存率的研究的病例'，数据以逗号分隔的，其中前三列是特征，最后一列是类别（患者存活 5 年或更长时间的是 1，在 5 年内死亡的是 2）。