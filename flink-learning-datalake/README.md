### Flink-learning-datalake

Flink Data Lake 项目里面包含了数据湖四大组件的基础、原理、实战、应用、源码相关内容

#### 模块说明

| 模块 | 技术 | 说明 |
|------|------|------|
| flink-learning-datalake-hudi | [Apache Hudi](https://github.com/apache/hudi) | Hudi 数据湖读写、流式写入、CDC 同步示例 |
| flink-learning-datalake-iceberg | [Apache Iceberg](https://github.com/apache/iceberg) | Iceberg Catalog 管理、数据读写、CDC 同步示例 |
| flink-learning-datalake-deltalake | [Delta Lake](https://github.com/delta-io/delta) | Delta Lake 表管理和数据读写示例 |
| flink-learning-table-store | [Apache Paimon](https://github.com/apache/paimon) (原 Flink Table Store) | Paimon 主键表、流式写入、CDC 同步示例 |

#### 示例列表

**Hudi**
- `HudiDataLakeExample` - Hudi 表的基本创建、写入和查询
- `HudiStreamingWriteExample` - 从 Kafka 实时写入 Hudi 表
- `HudiCDCSyncExample` - MySQL CDC 实时同步到 Hudi 数据湖

**Iceberg**
- `IcebergDataLakeExample` - Iceberg Catalog、表创建和查询
- `IcebergStreamingWriteExample` - 从 Kafka 实时写入 Iceberg 表
- `IcebergCDCSyncExample` - MySQL CDC 实时同步到 Iceberg 数据湖

**Delta Lake**
- `DeltaLakeExample` - Delta Lake 表的创建、写入和查询

**Paimon (原 Flink Table Store)**
- `PaimonDataLakeExample` - Paimon 主键表的创建、写入和查询
- `PaimonStreamingWriteExample` - 从 Kafka 实时写入 Paimon 表
- `PaimonCDCSyncExample` - MySQL CDC 实时同步到 Paimon 数据湖



### 数据湖资料

+ [delta lake 书籍](https://books.japila.pl/delta-lake-internals/overview/)


### 数据湖论文

+ [Lakehouse Architecture](http://cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf)