package com.zhisheng.connectors.cassandra.streaming;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.mapping.Mapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import java.util.ArrayList;

/**
 * Desc: Pojo Cassandra Sink in the Streaming API
 *
 * CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
 * CREATE TABLE IF NOT EXISTS test.message(body txt PRIMARY KEY)
 *
 * Created by zhisheng on 2019-08-04
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class CassandraPojoSinkExample {
    private static final ArrayList<Message> messages = new ArrayList<>(20);

    static {
        for (long i = 0; i < 20; i++) {
            messages.add(new Message("cassandra-" + i));
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Message> source = env.fromCollection(messages);

        CassandraSink.addSink(source)
                .setClusterBuilder(new ClusterBuilder() {
                    @Override
                    protected Cluster buildCluster(Cluster.Builder builder) {
                        return builder.addContactPoint("127.0.0.1").build();
                    }
                })
                .setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
                .build();

        env.execute("Cassandra Sink example");
    }
}
