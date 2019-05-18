package com.zhisheng.connectors.es6.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.util.ExceptionUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class ElasticSearchSinkUtil {

    /**
     * es sink
     *
     * @param hosts               es hosts
     * @param bulkFlushMaxActions bulk flush size
     * @param parallelism         并行数
     * @param data                数据
     * @param func
     * @param <T>
     */
    public static <T> void addSink(List<HttpHost> hosts, int bulkFlushMaxActions, int parallelism,
                                   SingleOutputStreamOperator<T> data, ElasticsearchSinkFunction<T> func) {
        ElasticsearchSink.Builder<T> esSinkBuilder = new ElasticsearchSink.Builder<>(hosts, func);
        esSinkBuilder.setBulkFlushMaxActions(bulkFlushMaxActions);
        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());
        data.addSink(esSinkBuilder.build()).setParallelism(parallelism);
    }

    /**
     * 解析配置文件的 es hosts
     *
     * @param hosts
     * @return
     * @throws MalformedURLException
     */
    public static List<HttpHost> getEsAddresses(String hosts) throws MalformedURLException {
        String[] hostList = hosts.split(",");
        List<HttpHost> addresses = new ArrayList<>();
        for (String host : hostList) {
            if (host.startsWith("http")) {
                URL url = new URL(host);
                addresses.add(new HttpHost(url.getHost(), url.getPort()));
            } else {
                String[] parts = host.split(":", 2);
                if (parts.length > 1) {
                    addresses.add(new HttpHost(parts[0], Integer.parseInt(parts[1])));
                } else {
                    throw new MalformedURLException("invalid elasticsearch hosts format");
                }
            }
        }
        return addresses;
    }

    public static class RetryRejectedExecutionFailureHandler implements ActionRequestFailureHandler {
        private static final long serialVersionUID = -7423562912824511906L;

        public RetryRejectedExecutionFailureHandler() {
        }

        @Override
        public void onFailure(ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) throws Throwable {
            if (ExceptionUtils.findThrowable(failure, EsRejectedExecutionException.class).isPresent()) {
                indexer.add(new ActionRequest[]{action});
            } else {
                if (ExceptionUtils.findThrowable(failure, SocketTimeoutException.class).isPresent()) {
                    // 忽略写入超时，因为ElasticSearchSink 内部会重试请求，不需要抛出来去重启 flink job
                    return;
                } else {
                    Optional<IOException> exp = ExceptionUtils.findThrowable(failure, IOException.class);
                    if (exp.isPresent()) {
                        IOException ioExp = exp.get();
                        if (ioExp != null && ioExp.getMessage() != null && ioExp.getMessage().contains("max retry timeout")) {
                            // request retries exceeded max retry timeout
                            // 经过多次不同的节点重试，还是写入失败的，则忽略这个错误，丢失数据。
                            log.error(ioExp.getMessage());
                            return;
                        }
                    }
                }
                throw failure;
            }
        }
    }
}
