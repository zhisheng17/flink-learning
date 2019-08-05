package com.zhisheng.connectors.cassandra.streaming;

import com.datastax.driver.core.Cluster;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import java.util.ArrayList;

/**
 * Desc: Tuple Cassandra Sink in streaming api
 *
 * CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
 * CREATE TABLE IF NOT EXISTS zhisheng.writetuple(element1 text PRIMARY KEY, element2 int)
 *
 * Created by zhisheng on 2019-08-04
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class CassandraTupleSinkExample {
    private static final String INSERT = "INSERT INTO zhisheng.writetuple (element1, element2) VALUES (?, ?)";
    private static final ArrayList<Tuple2<String, Integer>> collection = new ArrayList<>(20);

    static {
        for (int i = 0; i < 20; i++) {
            collection.add(new Tuple2<>("cassandra-" + i, i));
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> source = env.fromCollection(collection);

        CassandraSink.addSink(source)
                .setQuery(INSERT)
                .setClusterBuilder(new ClusterBuilder() {
                    @Override
                    protected Cluster buildCluster(Cluster.Builder builder) {
                        return builder.addContactPoint("127.0.0.1").build();
                    }
                })
                .build();

        env.execute("WriteTupleIntoCassandra");
    }
}
