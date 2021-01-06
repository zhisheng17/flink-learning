package com.zhisheng.libraries.stateProcessApi;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Desc:
 * Created by zhisheng on 2020-01-06 17:33
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class StatefulFunctionWithTime extends KeyedProcessFunction<Integer, Integer, Void> {

    ValueState<Integer> state;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("state", Types.INT);
        state = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public void processElement(Integer value, Context ctx, Collector<Void> out) throws Exception {
        state.update(value + 1);
    }
}
