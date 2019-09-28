package com.zhisheng.connectors.clickhouse.model;

import com.google.common.base.Preconditions;

import java.util.Map;

import static com.zhisheng.connectors.clickhouse.model.ClickhouseSinkConsts.*;

/**
 * Desc:
 * Created by zhisheng on 2019/9/28 上午10:05
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class ClickhouseSinkCommonParams {
    private final ClickhouseClusterSettings clickhouseClusterSettings;

    private final String failedRecordsPath;
    private final int numWriters;
    private final int queueMaxCapacity;

    private final int timeout;
    private final int maxRetries;

    public ClickhouseSinkCommonParams(Map<String, String> params) {
        this.clickhouseClusterSettings = new ClickhouseClusterSettings(params);
        this.numWriters = Integer.valueOf(params.get(NUM_WRITERS));
        this.queueMaxCapacity = Integer.valueOf(params.get(QUEUE_MAX_CAPACITY));
        this.maxRetries = Integer.valueOf(params.get(NUM_RETRIES));
        this.timeout = Integer.valueOf(params.get(TIMEOUT_SEC));
        this.failedRecordsPath = params.get(FAILED_RECORDS_PATH);

        Preconditions.checkNotNull(failedRecordsPath);
        Preconditions.checkArgument(queueMaxCapacity > 0);
        Preconditions.checkArgument(numWriters > 0);
        Preconditions.checkArgument(timeout > 0);
        Preconditions.checkArgument(maxRetries > 0);
    }

    public int getNumWriters() {
        return numWriters;
    }

    public int getQueueMaxCapacity() {
        return queueMaxCapacity;
    }

    public ClickhouseClusterSettings getClickhouseClusterSettings() {
        return clickhouseClusterSettings;
    }

    public int getTimeout() {
        return timeout;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public String getFailedRecordsPath() {
        return failedRecordsPath;
    }

    @Override
    public String toString() {
        return "ClickhouseSinkCommonParams{" +
                "clickhouseClusterSettings=" + clickhouseClusterSettings +
                ", failedRecordsPath='" + failedRecordsPath + '\'' +
                ", numWriters=" + numWriters +
                ", queueMaxCapacity=" + queueMaxCapacity +
                ", timeout=" + timeout +
                ", maxRetries=" + maxRetries +
                '}';
    }
}
