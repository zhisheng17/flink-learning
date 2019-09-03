package com.zhisheng.examples.streaming.exception;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Desc: 测试重启代码出现异常，导致触发重启策略
 * Created by zhisheng on 2019-04-19
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class Main {
    public static void main(String[] args) {
        //创建流运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1));

    }
}
