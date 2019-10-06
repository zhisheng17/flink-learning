package com.zhisheng.examples.streaming.parallelism;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Desc: parallelism & slot
 * Created by zhisheng on 2019/10/6 下午1:43
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class Main {
    public static void main(String[] args) throws Exception {
        //创建流运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        env.setParallelism(1);

        env.addSource(new SourceFunction<Long>() {
            @Override
            public void run(SourceContext<Long> sourceContext) throws Exception {
                while (true) {
                    sourceContext.collect(System.currentTimeMillis());
                }
            }

            @Override
            public void cancel() {

            }
        })
                .map((MapFunction<Long, Long>) aLong -> aLong / 1000).setParallelism(3)
                .print();

        env.execute("zhisheng RestartStrategy example");
    }
}
