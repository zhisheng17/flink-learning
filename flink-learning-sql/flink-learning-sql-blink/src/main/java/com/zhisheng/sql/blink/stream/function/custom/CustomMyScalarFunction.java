package com.zhisheng.sql.blink.stream.function.custom;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Desc: custom scalar function
 * https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/udfs.html#scalar-functions
 * <p>
 * Created by zhisheng on 2020-01-26 19:34
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class CustomMyScalarFunction extends ScalarFunction {

    //这个方法不能少
    public boolean eval(Integer count) {
        return count > 5;
    }
}
