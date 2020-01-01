package com.zhisheng.sql.blink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * Desc:
 * Created by zhisheng on 2019/11/3 下午1:14
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class Example1 {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnvironment = TableEnvironment.create(settings);
        StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());

    }
}
