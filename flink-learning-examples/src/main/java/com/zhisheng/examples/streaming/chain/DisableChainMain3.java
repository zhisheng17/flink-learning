package com.zhisheng.examples.streaming.chain;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * Desc: operator chain —— disable chain
 * Created by zhisheng on 2019/10/6 下午7:42
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class DisableChainMain3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        //disable operator chain
        env.disableOperatorChaining();

        env.addSource(new SourceFunction<String>() {
            @Override
            public void cancel() {
            }

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int i = 0;
                while (true) {
                    ctx.collect("zhisheng" + i ++);
                }
            }
        })
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] splits = value.toLowerCase().split("\\W+");

                        for (String split : splits) {
                            if (split.length() > 0) {
                                out.collect(new Tuple2<>(split, 1));
                            }
                        }
                    }
                })
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .print();

        env.execute("zhisheng —— word count disable chain demo");
    }
}
