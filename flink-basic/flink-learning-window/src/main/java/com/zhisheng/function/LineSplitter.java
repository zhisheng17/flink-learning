package com.zhisheng.function;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Desc:
 * Created by zhisheng on 2019-08-06
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class LineSplitter implements FlatMapFunction<String, Tuple2<Long, String>> {
    @Override
    public void flatMap(String s, Collector<Tuple2<Long, String>> collector) {
        String[] tokens = s.split(" ");

        if (tokens.length >= 2 && isValidLong(tokens[0])) {
            collector.collect(new Tuple2<>(Long.valueOf(tokens[0]), tokens[1]));
        }
    }

    private static boolean isValidLong(String str) {
        try {
            long _v = Long.parseLong(str);
            return true;
        } catch (NumberFormatException e) {
            log.info("the str = {} is not a number", str);
            return false;
        }
    }
}
