package com.zhisheng.common.watermarks;

import com.zhisheng.common.model.Metrics;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class MetricWatermark implements AssignerWithPeriodicWatermarks<Metrics> {

    private long currentTimestamp = Long.MIN_VALUE;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
    }

    @Override
    public long extractTimestamp(Metrics metrics, long l) {
        long timestamp = metrics.getTimestamp() / (1000 * 1000);
        this.currentTimestamp = timestamp;
        return timestamp;
    }
}
