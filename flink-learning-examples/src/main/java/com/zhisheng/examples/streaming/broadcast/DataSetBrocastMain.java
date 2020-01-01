package com.zhisheng.examples.streaming.broadcast;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import java.util.List;

/**
 * Desc:
 * Created by zhisheng on 2019/10/17 上午9:21
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class DataSetBrocastMain {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //1. 待广播的数据
        DataSet<Integer> toBroadcast = env.fromElements(1, 2, 3);

        env.fromElements("a", "b")
                .map(new RichMapFunction<String, String>() {
                    List<Integer> broadcastData;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 3. 获取广播的DataSet数据 作为一个Collection
                        broadcastData = getRuntimeContext().getBroadcastVariable("zhisheng");
                    }

                    @Override
                    public String map(String value) throws Exception {
                        return broadcastData.get(1) + value;
                    }
                }).withBroadcastSet(toBroadcast, "zhisheng")// 2. 广播DataSet
                .print();
    }
}
