package com.zhisheng.connectors.cassandra.streaming;

import com.datastax.driver.core.Cluster;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Desc: use Cassandra Sink in streaming api
 *
 * <p>The example assumes that a table exists in a local cassandra database, according to the following queries:
 * CREATE KEYSPACE IF NOT EXISTS example WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
 * CREATE TABLE zhisheng.values (id text, count int, PRIMARY KEY(id));
 *
 * <p>Important things to note are that checkpointing is enabled, a StateBackend is set and the enableWriteAheadLog() call
 * when creating the CassandraSink.
 *
 * Created by zhisheng on 2019-08-04
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class CassandraTupleWriteAheadSinkExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 1000));
        env.setStateBackend(new FsStateBackend("file:///" + System.getProperty("java.io.tmpdir") + "/flink/backend"));

        CassandraSink<Tuple2<String, Integer>> sink = CassandraSink.addSink(env.addSource(new MySource()))
                .setQuery("INSERT INTO zhisheng.values (id, counter) values (?, ?);")
                .enableWriteAheadLog()
                .setClusterBuilder(new ClusterBuilder() {

                    private static final long serialVersionUID = 2793938419775311824L;

                    @Override
                    public Cluster buildCluster(Cluster.Builder builder) {
                        return builder.addContactPoint("127.0.0.1").build();
                    }
                })
                .build();

        sink.name("Cassandra Sink").disableChaining().setParallelism(1).uid("hello");

        env.execute();
    }

    private static class MySource implements SourceFunction<Tuple2<String, Integer>>, ListCheckpointed<Integer> {
        private static final long serialVersionUID = 4022367939215095610L;

        private int counter = 0;
        private boolean stop = false;

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            while (!stop) {
                Thread.sleep(50);
                ctx.collect(new Tuple2<>("" + UUID.randomUUID(), 1));
                counter++;
                if (counter == 100) {
                    stop = true;
                }
            }
        }

        @Override
        public void cancel() {
            stop = true;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(this.counter);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            if (state.isEmpty() || state.size() > 1) {
                throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
            }
            this.counter = state.get(0);
        }
    }
}
