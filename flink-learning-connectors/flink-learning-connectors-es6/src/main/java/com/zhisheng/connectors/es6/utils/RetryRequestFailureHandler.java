package com.zhisheng.connectors.es6.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.util.ExceptionUtils;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Optional;

/**
 * Desc: es sink Request Failure Handler
 * Created by zhisheng on 2019/10/21 下午3:07
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class RetryRequestFailureHandler implements ActionRequestFailureHandler {

    public RetryRequestFailureHandler() {
    }

    @Override
    public void onFailure(ActionRequest actionRequest, Throwable throwable, int i, RequestIndexer requestIndexer) throws Throwable {
        if (ExceptionUtils.findThrowable(throwable, EsRejectedExecutionException.class).isPresent()) {
            requestIndexer.add(new ActionRequest[]{actionRequest});
        } else {
            if (ExceptionUtils.findThrowable(throwable, SocketTimeoutException.class).isPresent()) {
                return;
            } else {
                Optional<IOException> exp = ExceptionUtils.findThrowable(throwable, IOException.class);
                if (exp.isPresent()) {
                    IOException ioExp = exp.get();
                    if (ioExp != null && ioExp.getMessage() != null && ioExp.getMessage().contains("max retry timeout")) {
                        log.error(ioExp.getMessage());
                        return;
                    }
                }
            }
            throw throwable;
        }
    }
}