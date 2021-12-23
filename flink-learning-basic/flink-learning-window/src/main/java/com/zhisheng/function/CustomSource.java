package com.zhisheng.function;

import com.zhisheng.common.model.WordEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * Desc:
 * Created by zhisheng on 2019-08-07
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class CustomSource extends RichSourceFunction<WordEvent> {

    private volatile boolean isRunning = true;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void run(SourceContext<WordEvent> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(new WordEvent(word(), count(), System.currentTimeMillis()));
            Thread.sleep(1000);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        isRunning = false;
    }

    @Override
    public void cancel() {
        isRunning = false;
    }


    private String word() {
        String[] strs = new String[]{"A", "B", "C", "D", "E", "F"};
        int index = (int) (Math.random() * strs.length);
        return "zhisheng" + strs[index];
    }

    private int count() {
        int[] strs = new int[]{1, 2, 3, 4, 5, 6};
        int index = (int) (Math.random() * strs.length);
        return strs[index];
    }

}
