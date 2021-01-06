package com.zhisheng.libraries.stateProcessApi;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;

/**
 * Desc:
 * Created by zhisheng on 2019/10/19 下午9:50
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class Main {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment bEnv   = ExecutionEnvironment.getExecutionEnvironment();
        ExistingSavepoint savepoint = Savepoint.load(bEnv, "hdfs://path/", new RocksDBStateBackend(""));

        DataSet<Integer> listState  = savepoint.readListState("zhisheng-uid", "list-state", Types.INT);

        DataSet<Integer> unionState = savepoint.readUnionState("zhisheng-uid", "union-state", Types.INT);

        DataSet<Tuple2<Integer, Integer>> broadcastState = savepoint.readBroadcastState("zhisheng-uid", "broadcast-state", Types.INT, Types.INT);

//        DataSet<Integer> listState = savepoint.readListState(
//                "zhisheng-uid", "list-state",
//                Types.INT, new MyCustomIntSerializer());
    }
}
