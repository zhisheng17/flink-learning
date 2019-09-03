package com.zhisheng.state.queryablestate;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Desc: QuerybleStateStream
 * Created by zhisheng on 2019-07-05
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class QuerybleStateStream {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStreamSource<String> socket = env.socketTextStream("localhost", 9002);
        env.socketTextStream("localhost", 9002)
        .flatMap(new FlatMapFunction<String, ClimateLog>() {
            @Override
            public void flatMap(String value, Collector<ClimateLog> out) throws Exception {
                try {
                    String[] split = value.split(",");
                    out.collect(new ClimateLog(split[0], split[1], Float.parseFloat(split[2]), Float.parseFloat(split[3])));
                } catch (Exception e) {
                    //如果数据有问题的话，允许把这条有问题的数据丢掉
                    log.warn("解析 socket 数据异常, value = {}, err = {}", value, e.getMessage());
                }
            }
        });

    }
}
