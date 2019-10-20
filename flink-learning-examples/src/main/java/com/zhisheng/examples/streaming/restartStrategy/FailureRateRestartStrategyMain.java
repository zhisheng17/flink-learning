package com.zhisheng.examples.streaming.restartStrategy;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Desc: NullPointerException application，failureRate Restart Strategy Test
 * Created by zhisheng on 2019/10/5 下午11:22
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class FailureRateRestartStrategyMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        //每隔 10s 重启一次，如果两分钟内重启过三次则停止 Job
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(2), Time.seconds(10)));

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

        env.execute("zhisheng failureRate Restart Strategy example");
    }
}
