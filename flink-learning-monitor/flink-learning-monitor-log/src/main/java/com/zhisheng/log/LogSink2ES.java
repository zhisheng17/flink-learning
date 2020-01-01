package com.zhisheng.log;

import com.zhisheng.common.model.LogEvent;
import com.zhisheng.common.model.MetricEvent;
import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.log.utils.ESSinkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.net.MalformedURLException;
import java.util.List;

import static com.zhisheng.common.constant.PropertiesConstants.*;
import static com.zhisheng.common.constant.PropertiesConstants.ZHISHENG;

/**
 * Desc: sink log to es
 * Created by zhisheng on 2019/10/26 下午7:23
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class LogSink2ES {
    public static void sink2es(SingleOutputStreamOperator<LogEvent> logDataStream, ParameterTool parameterTool) {
        List<HttpHost> esAddresses;
        try {
             esAddresses = ESSinkUtil.getEsAddresses(parameterTool.get(ELASTICSEARCH_HOSTS));
        } catch (MalformedURLException e) {
            log.error("get es address has an error", e);
            return;
        }
        int bulkSize = parameterTool.getInt(ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS, 40);
        int sinkParallelism = parameterTool.getInt(STREAM_SINK_PARALLELISM, 5);

        ESSinkUtil.addSink(esAddresses, bulkSize, sinkParallelism, logDataStream,
                (LogEvent logEvent, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
                    requestIndexer.add(Requests.indexRequest()
                            .index("zhisheng_log")
                            .type(ZHISHENG)
                            .source(GsonUtil.toJSONBytes(logEvent), XContentType.JSON));
                },
                parameterTool);
    }
}
