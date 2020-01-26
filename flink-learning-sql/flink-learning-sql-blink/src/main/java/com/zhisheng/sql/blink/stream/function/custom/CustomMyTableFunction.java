package com.zhisheng.sql.blink.stream.function.custom;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableFunction;

/**
 * Desc: custom table function
 * https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/udfs.html#table-functions
 * <p>
 * Created by zhisheng on 2020-01-26 21:23
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class CustomMyTableFunction extends TableFunction<Tuple2<String, Integer>> {

    private String separator;

    public CustomMyTableFunction(String separator) {
        this.separator = separator == null || "".equals(separator) ? " " : separator;
    }

    public void eval(String str) {
        for (String s : str.split(separator)) {
            collect(new Tuple2<>(s, s.length()));
        }
    }
}
