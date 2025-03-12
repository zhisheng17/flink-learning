package com.zhisheng.sql.utils;

import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

public class CloseableRowIteratorWrapper implements CloseableIterator<Row> {
    private final CloseableIterator<Row> iterator;
    private boolean isFirstRowReady = false;

    public CloseableRowIteratorWrapper(CloseableIterator<Row> iterator) {
        this.iterator = iterator;
    }

    @Override
    public void close() throws Exception {
        iterator.close();
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = iterator.hasNext();
        isFirstRowReady = isFirstRowReady || hasNext;
        return hasNext;
    }

    @Override
    public Row next() {
        Row next = iterator.next();
        isFirstRowReady = true;
        return next;
    }

    public boolean isFirstRowReady() {
        return isFirstRowReady || hasNext();
    }
}
