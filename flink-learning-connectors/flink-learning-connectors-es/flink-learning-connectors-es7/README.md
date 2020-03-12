## Flink connector ElasticSearch 7.x


遇到过的问题有：

1、ElasticSearch 的分片和副本的调优

2、ElasticSearch 的线程队列的调优

3、ElasticSearch 的刷新时间的调整

4、ElasticSearch 磁盘到 85% 后出现不写入

5、ElasticSearch 某个节点挂了导致写入的请求丢失不会重试

6、ElasticSearch bulk 写

7、权限认证的问题

8、Index template 初始化

9、使用 RestHighLevelClient

总之目的就是为了在能够高效的将数据写入进 ElasticSearch，还要保证 ElasticSearch 不挂


## TODO

1、X-Pack 权限认证

2、使用 RestHighLevelClient