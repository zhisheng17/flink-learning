package com.zhisheng.examples.streaming.restartStrategy;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Desc: 测试重启代码出现异常，导致触发重启策略
 * Created by zhisheng on 2019-04-19
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class AEMain {
    public static void main(String[] args) throws Exception {
        //创建流运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        //每隔 5s 重启一次，尝试三次如果 Job 还没有起来则停止
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));

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
                .map((MapFunction<Long, Long>) aLong -> aLong / 0)
                .print();

        env.execute("zhisheng RestartStrategy example");
    }
}
