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

    //这种情况下无问题
    public MySink(String xxxx) {
        System.out.println("+++++++++++++" + xxxx);
        tx = xxxx;
    }

    //这种情况下有问题，他会将 tx 识别错误，导致下面赋值失败
//    public MySink(String tx) {
//        System.out.println("+++++++++++++" + tx);
//        tx = tx;
//    }

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
