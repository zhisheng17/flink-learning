package com.zhisheng.connectors.netty;

import com.zhisheng.common.utils.ExecutionEnvUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Desc: Netty connector
 * Created by zhisheng on 2019-05-04
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class Main {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);



        env.execute("flink netty connector");
    }
}
