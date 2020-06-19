package com.zhisheng.data.sinks.sinks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Desc:
 * Created by zhisheng on 2019-09-26
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class MySink extends RichSinkFunction<String> {
    private String tx;

    public MySink(String tx) {
        System.out.println("+++++++++++++" + tx);
        this.tx = tx;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        tx = "5";
        System.out.println("========");
        super.open(parameters);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        System.out.println(value + " " + tx);
    }
}
