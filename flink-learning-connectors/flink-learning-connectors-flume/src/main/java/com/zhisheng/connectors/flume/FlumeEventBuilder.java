package com.zhisheng.connectors.flume;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flume.Event;

import java.io.Serializable;

/**
 * Desc: A function that can create a Event from an incoming instance of the given type.
 * Created by zhisheng on 2019-05-04
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public interface FlumeEventBuilder<IN> extends Function, Serializable {

    Event createFlumeEvent(IN value, RuntimeContext ctx);

}
