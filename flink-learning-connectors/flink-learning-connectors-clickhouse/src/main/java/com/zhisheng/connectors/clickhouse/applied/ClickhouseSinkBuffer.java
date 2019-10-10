package com.zhisheng.connectors.clickhouse.applied;

import com.google.common.base.Preconditions;
import com.zhisheng.connectors.clickhouse.model.ClickhouseRequestBlank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Desc:
 * Created by zhisheng on 2019/9/28 上午10:08
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class ClickhouseSinkBuffer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ClickhouseSinkBuffer.class);

    private final ClickhouseWriter writer;
    private final String targetTable;
    private final int maxFlushBufferSize;
    private final long timeoutMillis;
    private final List<String> localValues;

    private volatile long lastAddTimeMillis = 0L;

    private ClickhouseSinkBuffer(
            ClickhouseWriter chWriter,
            long timeout,
            int maxBuffer,
            String table
    ) {
        writer = chWriter;
        localValues = new ArrayList<>();
        timeoutMillis = timeout;
        maxFlushBufferSize = maxBuffer;
        targetTable = table;

        logger.info("Instance Clickhouse Sink, target table = {}, buffer size = {}", this.targetTable, this.maxFlushBufferSize);
    }

    String getTargetTable() {
        return targetTable;
    }

    public void put(String recordAsCSV) {
        tryAddToQueue();
        localValues.add(recordAsCSV);
        lastAddTimeMillis = System.currentTimeMillis();
    }

    synchronized void tryAddToQueue() {
        if (flushCondition()) {
            addToQueue();
        }
    }

    private void addToQueue() {
        List<String> deepCopy = buildDeepCopy(localValues);
        ClickhouseRequestBlank params = ClickhouseRequestBlank.Builder
                .aBuilder()
                .withValues(deepCopy)
                .withTargetTable(targetTable)
                .build();

        logger.debug("Build blank with params: buffer size = {}, target table  = {}", params.getValues().size(), params.getTargetTable());
        writer.put(params);

        localValues.clear();
    }

    private boolean flushCondition() {
        return localValues.size() > 0 && (checkSize() || checkTime());
    }

    private boolean checkSize() {
        return localValues.size() >= maxFlushBufferSize;
    }

    private boolean checkTime() {
        if (lastAddTimeMillis == 0) {
            return false;
        }

        long current = System.currentTimeMillis();
        return current - lastAddTimeMillis > timeoutMillis;
    }

    private static List<String> buildDeepCopy(List<String> original) {
        return Collections.unmodifiableList(new ArrayList<>(original));
    }

    @Override
    public void close() {
        if (localValues != null && localValues.size() > 0) {
            addToQueue();
        }
    }

    public static final class Builder {
        private String targetTable;
        private int maxFlushBufferSize;
        private int timeoutSec;

        private Builder() {
        }

        public static Builder aClickhouseSinkBuffer() {
            return new Builder();
        }

        public Builder withTargetTable(String targetTable) {
            this.targetTable = targetTable;
            return this;
        }

        public Builder withMaxFlushBufferSize(int maxFlushBufferSize) {
            this.maxFlushBufferSize = maxFlushBufferSize;
            return this;
        }

        public Builder withTimeoutSec(int timeoutSec) {
            this.timeoutSec = timeoutSec;
            return this;
        }

        public ClickhouseSinkBuffer build(ClickhouseWriter writer) {

            Preconditions.checkNotNull(targetTable);
            Preconditions.checkArgument(maxFlushBufferSize > 0);
            Preconditions.checkArgument(timeoutSec > 0);

            return new ClickhouseSinkBuffer(
                    writer,
                    TimeUnit.SECONDS.toMillis(this.timeoutSec),
                    this.maxFlushBufferSize,
                    this.targetTable
            );
        }
    }
}
