package com.zhisheng.examples.streaming.restartStrategy;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Desc: NullPointerException application，default RestartStrategy Test
 * Created by zhisheng on 2019/10/5 下午11:22
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class DefaultRestartStrategyMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        env.setParallelism(1);

        env.addSource(new SourceFunction<Long>() {
            @Override
            public void run(SourceContext<Long> sourceContext) throws Exception {
                while (true) {
                    sourceContext.collect(null);
                }
            }
            @Override
            public void cancel() {
            }
        })
                .map((MapFunction<Long, Long>) aLong -> aLong / 1)
                .print();

        env.execute("zhisheng default RestartStrategy example");
    }
}
