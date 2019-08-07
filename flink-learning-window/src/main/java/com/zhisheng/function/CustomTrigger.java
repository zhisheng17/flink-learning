package com.zhisheng.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Desc: https://stackoverrun.com/cn/q/10143011
 * https://www.jianshu.com/p/1dacf2f84325
 * Created by zhisheng on 2019-08-06
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class CustomTrigger extends Trigger<Tuple2<Long, String>, TimeWindow> {

    public CustomTrigger() {
        super();
    }

    //每个元素被添加到窗口时都会调用该方法
    @Override
    public TriggerResult onElement(Tuple2<Long, String> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        return null;
    }

    //当一个已注册的 ProcessingTime 计时器启动时调用
    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return null;
    }

    //当一个已注册的 EventTime 计时器启动时调用
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return null;
    }

    //与状态性触发器相关，当使用会话窗口时，两个触发器对应的窗口合并时，合并两个触发器的状态
    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
        super.onMerge(window, ctx);
    }

    //执行任何需要清除的相应窗口
    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

    }
}
