package com.zhisheng.examples.streaming.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * Desc:
 * Created by zhisheng on 2020-03-07 20:26
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class StateMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setStateBackend(new MemoryStateBackend());

        env.addSource(new RichParallelSourceFunction<Tuple2<String, Long>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Long>> sourceContext) throws Exception {
                while (true) {
                    sourceContext.collect(new Tuple2<>(String.valueOf(System.currentTimeMillis()), System.currentTimeMillis()));
                    Thread.sleep(10);
                }
            }

            @Override
            public void cancel() {

            }
        }).keyBy(0)
                .map(new RichMapFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {

                    private ValueState<Long> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        state = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("uvState",
                                        TypeInformation.of(new TypeHint<Long>() {
                                        })));
                    }

                    @Override
                    public Tuple2<String, Long> map(Tuple2<String, Long> tuple2) throws Exception {
                        state.update(tuple2.f1);
                        return tuple2;
                    }
                }).print();

        env.execute();
    }
}
