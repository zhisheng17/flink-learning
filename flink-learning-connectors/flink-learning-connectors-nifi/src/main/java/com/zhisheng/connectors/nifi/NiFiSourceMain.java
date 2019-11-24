package com.zhisheng.connectors.nifi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.nifi.NiFiDataPacket;
import org.apache.flink.streaming.connectors.nifi.NiFiSource;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;

import java.nio.charset.Charset;

/**
 * Desc: nifi source
 * Created by zhisheng on 2019/11/24 上午11:06
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class NiFiSourceMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SiteToSiteClientConfig clientConfig = new SiteToSiteClient.Builder()
                .url("http://localhost:8080/nifi")
                .portName("Data for Flink")
                .requestBatchCount(5)
                .buildConfig();

        SourceFunction<NiFiDataPacket> nifiSource = new NiFiSource(clientConfig);
        DataStream<NiFiDataPacket> streamSource = env.addSource(nifiSource).setParallelism(2);

        DataStream<String> dataStream = streamSource.map(new MapFunction<NiFiDataPacket, String>() {
            @Override
            public String map(NiFiDataPacket value) throws Exception {
                return new String(value.getContent(), Charset.defaultCharset());
            }
        });

        dataStream.print();
        env.execute();
    }
}
