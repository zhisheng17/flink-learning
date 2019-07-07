package com.zhisheng.examples.streaming.watermark;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class WordWatermark implements AssignerWithPeriodicWatermarks<Word> {

    private long currentTimestamp = Long.MIN_VALUE;

    @Override
    public long extractTimestamp(Word word, long previousElementTimestamp) {
        if (word.getTimestamp() > currentTimestamp) {
            this.currentTimestamp = word.getTimestamp();
        }
        return currentTimestamp;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        long maxTimeLag = 5000;
        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag);

    }
}
