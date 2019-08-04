package com.zhisheng.connectors.cassandra.batch;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.mapping.Mapper;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.batch.connectors.cassandra.CassandraPojoInputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraPojoOutputFormat;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Desc: CassandraPojoInputFormat/CassandraPojoOutputFormat batch api
 *
 * <p>The example assumes that a table exists in a local cassandra database, according to the following queries:
 * CREATE KEYSPACE IF NOT EXISTS flink WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
 * CREATE TABLE IF NOT EXISTS flink.batches (id text, counter int, batch_id int, PRIMARY KEY(id, counter, batchId));
 *
 * Created by zhisheng on 2019-08-04
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class BatchPojoExample {
    private static final String SELECT_QUERY = "SELECT id, counter, batch_id FROM flink.batches;";

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<CustomCassandraAnnotatedPojo> customCassandraAnnotatedPojos = IntStream.range(0, 20)
                .mapToObj(x -> new CustomCassandraAnnotatedPojo(UUID.randomUUID().toString(), x, 0))
                .collect(Collectors.toList());

        DataSet<CustomCassandraAnnotatedPojo> dataSet = env.fromCollection(customCassandraAnnotatedPojos);

        ClusterBuilder clusterBuilder = new ClusterBuilder() {
            private static final long serialVersionUID = -1754532803757154795L;

            @Override
            protected Cluster buildCluster(Cluster.Builder builder) {
                return builder.addContactPoints("127.0.0.1").build();
            }
        };

        dataSet.output(new CassandraPojoOutputFormat<>(clusterBuilder, CustomCassandraAnnotatedPojo.class, () -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)}));

        env.execute("zhisheng");

        /*
         *	This is for the purpose of showing an example of creating a DataSet using CassandraPojoInputFormat.
         */
        DataSet<CustomCassandraAnnotatedPojo> inputDS = env
                .createInput(new CassandraPojoInputFormat<>(
                        SELECT_QUERY,
                        clusterBuilder,
                        CustomCassandraAnnotatedPojo.class,
                        () -> new Mapper.Option[]{Mapper.Option.consistencyLevel(ConsistencyLevel.ANY)}
                ));

        inputDS.print();
    }
}
