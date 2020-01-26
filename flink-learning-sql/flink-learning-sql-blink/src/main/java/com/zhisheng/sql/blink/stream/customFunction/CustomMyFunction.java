package com.zhisheng.sql.blink.stream.customFunction;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Desc: custom function
 * Created by zhisheng on 2020-01-26 19:34
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class CustomMyFunction extends ScalarFunction {

    //这个方法不能少
    public boolean eval(Integer count) {
        return count > 5;
    }
}
