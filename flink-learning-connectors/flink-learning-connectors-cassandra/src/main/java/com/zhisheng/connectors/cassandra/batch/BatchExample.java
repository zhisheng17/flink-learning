package com.zhisheng.connectors.cassandra.batch;

import com.datastax.driver.core.Cluster;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraTupleOutputFormat;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import java.util.ArrayList;

/**
 * Desc: Cassandra Input-/OutputFormats batch api
 *
 * <p>The example assumes that a table exists in a local cassandra database, according to the following queries:
 * CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
 * CREATE TABLE IF NOT EXISTS test.zhisheng (number int, strings text, PRIMARY KEY(number, strings));
 *
 * Created by zhisheng on 2019-08-04
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class BatchExample {
    private static final String INSERT_QUERY = "INSERT INTO test.zhisheng (number, strings) VALUES (?,?);";
    private static final String SELECT_QUERY = "SELECT number, strings FROM test.zhisheng;";

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ArrayList<Tuple2<Integer, String>> collection = new ArrayList<>(20);
        for (int i = 0; i < 20; i++) {
            collection.add(new Tuple2<>(i, "string " + i));
        }

        env.fromCollection(collection)
                .output(new CassandraTupleOutputFormat<Tuple2<Integer, String>>(INSERT_QUERY, new ClusterBuilder() {
                    @Override
                    protected Cluster buildCluster(Cluster.Builder builder) {
                        return builder.addContactPoints("127.0.0.1").build();
                    }
                }));

        env.execute("zhisheng");

        DataSet<Tuple2<Integer, String>> inputDS = env
                .createInput(new CassandraInputFormat<Tuple2<Integer, String>>(SELECT_QUERY, new ClusterBuilder() {
                    @Override
                    protected Cluster buildCluster(Cluster.Builder builder) {
                        return builder.addContactPoints("127.0.0.1").build();
                    }
                }), TupleTypeInfo.of(new TypeHint<Tuple2<Integer, String>>() {
                }));

        inputDS.print();
    }
}
