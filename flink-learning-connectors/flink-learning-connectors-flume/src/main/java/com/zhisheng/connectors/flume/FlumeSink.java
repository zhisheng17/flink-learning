package com.zhisheng.connectors.flume;

import com.zhisheng.connectors.flume.utils.FlumeUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;

import java.util.ArrayList;
import java.util.List;


/**
 * Desc: Flume sink
 * Created by zhisheng on 2019-05-04
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class FlumeSink<IN> extends RichSinkFunction<IN> {
    /**
     * 最大尝试次数
     */
    private static final int DEFAULT_MAX_RETRY_ATTEMPTS = 3;

    /**
     * 等待时间
     */
    private static final long DEFAULT_WAIT_TIMEMS = 1000L;

    private String clientType;

    private String hostname;

    private int port;

    private int batchSize;

    private int maxRetryAttempts;

    private long waitTimeMs;

    private List<IN> incomingList;

    private FlumeEventBuilder eventBuilder;

    private RpcClient client;

    public FlumeSink(String clientType, String hostname, int port, FlumeEventBuilder<IN> eventBuilder) {
        this(clientType, hostname, port, eventBuilder, RpcClientConfigurationConstants.DEFAULT_BATCH_SIZE, DEFAULT_MAX_RETRY_ATTEMPTS, DEFAULT_WAIT_TIMEMS);
    }

    public FlumeSink(String clientType, String hostname, int port, FlumeEventBuilder<IN> eventBuilder, int batchSize) {
        this(clientType, hostname, port, eventBuilder, batchSize, DEFAULT_MAX_RETRY_ATTEMPTS, DEFAULT_WAIT_TIMEMS);
    }


    public FlumeSink(String clientType, String hostname, int port, FlumeEventBuilder<IN> eventBuilder, int batchSize, int maxRetryAttempts, long waitTimeMs) {
        this.clientType = clientType;
        this.hostname = hostname;
        this.port = port;
        this.eventBuilder = eventBuilder;
        this.batchSize = batchSize;
        this.maxRetryAttempts = maxRetryAttempts;
        this.waitTimeMs = waitTimeMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        incomingList = new ArrayList();
        client = FlumeUtil.getRpcClient(clientType, hostname, port, batchSize);
    }

    @Override
    public void invoke(IN value) throws Exception {
        int number;
        synchronized (this) {
            if (null != value) {
                incomingList.add(value);
            }
            number = incomingList.size();
        }

        if (number == batchSize) {
            flush();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        FlumeUtil.destroy(client);
    }


    private void flush() throws IllegalStateException {
        List<Event> events = new ArrayList<>();
        List<IN> toFlushList;
        synchronized (this) {
            if (incomingList.isEmpty()) {
                return;
            }
            toFlushList = incomingList;
            incomingList = new ArrayList();
        }

        for (IN value : toFlushList) {
            Event event = this.eventBuilder.createFlumeEvent(value, getRuntimeContext());
            events.add(event);
        }

        int retries = 0;
        boolean flag = true;
        while (flag) {
            if (null != client || retries > maxRetryAttempts) {
                flag = false;
            }

            if (retries <= maxRetryAttempts && null == client) {
                log.info("Wait for {} ms before retry", waitTimeMs);
                try {
                    Thread.sleep(waitTimeMs);
                } catch (InterruptedException ignored) {
                    log.error("Interrupted while trying to connect {} on {}", hostname, port);
                }
                reconnect();
                log.info("Retry attempt number {}", retries);
                retries++;
            }
        }

        try {
            client.appendBatch(events);
        } catch (EventDeliveryException e) {
            log.info("Encountered exception while sending data to flume : {}", e.getMessage(), e);
        }

    }

    private void reconnect() {
        FlumeUtil.destroy(client);
        client = null;
        client = FlumeUtil.getRpcClient(clientType, hostname, port, batchSize);
    }

}
