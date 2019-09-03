package com.zhisheng.examples.streaming.remote;

import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * 向远程集群提交 job
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class Main {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment(
                "localhost",
                6123,
                1,
                "/usr/local/blink-1.5.1/examples/streaming/SessionWindowing.jar"
        );


    }
}
