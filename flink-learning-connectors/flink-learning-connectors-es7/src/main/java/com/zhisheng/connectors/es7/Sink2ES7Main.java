package com.zhisheng.connectors.es7;

import com.zhisheng.common.model.MetricEvent;
import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.connectors.es7.util.ESSinkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.zhisheng.common.constant.PropertiesConstants.*;

/**
 * Desc: sink data to es7
 * Created by zhisheng on 2019/10/22 下午5:10
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class Sink2ES7Main {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);

        List<HttpHost> esAddresses = ESSinkUtil.getEsAddresses(parameterTool.get(ELASTICSEARCH_HOSTS));
        int bulkSize = parameterTool.getInt(ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS, 40);
        int sinkParallelism = parameterTool.getInt(STREAM_SINK_PARALLELISM, 1);

        log.info("-----esAddresses = {}, parameterTool = {}, ", esAddresses, parameterTool);


        DataStreamSource<MetricEvent> data = env.addSource(new ParallelSourceFunction<MetricEvent>() {
            @Override
            public void run(SourceContext<MetricEvent> context) throws Exception {
                while (true) {
                    //just for test

                    Map<String, Object> fields = new HashMap<>();
                    fields.put("system", 10);
                    fields.put("user", 20);
                    fields.put("idle", 70);

                    Map<String, String> tags = new HashMap<>();
                    tags.put("cluster_name", "zhisheng");
                    tags.put("host_ip", "11.0.11.0");

                    MetricEvent metricEvent = MetricEvent.builder()
                            .name("cpu")
                            .timestamp(System.currentTimeMillis())
                            .fields(fields)
                            .tags(tags)
                            .build();

                    context.collect(metricEvent);
                    Thread.sleep(200);
                }
            }

            @Override
            public void cancel() {
            }
        });

        ESSinkUtil.addSink(esAddresses, bulkSize, sinkParallelism, data,
                (MetricEvent metric, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
                    requestIndexer.add(Requests.indexRequest()
                            .index(ZHISHENG + "_" + metric.getName())
                            .source(GsonUtil.toJSONBytes(metric), XContentType.JSON));
                },
                parameterTool);

        env.execute("flink learning connectors es7");
    }
}
