package com.zhisheng.connectors.nifi;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.nifi.NiFiDataPacket;
import org.apache.flink.streaming.connectors.nifi.NiFiDataPacketBuilder;
import org.apache.flink.streaming.connectors.nifi.NiFiSink;
import org.apache.flink.streaming.connectors.nifi.StandardNiFiDataPacket;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;

import java.util.HashMap;

/**
 * Desc: nifi sink
 * Created by zhisheng on 2019/11/24 上午11:06
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class NiFiSinkMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SiteToSiteClientConfig clientConfig = new SiteToSiteClient.Builder()
                .url("http://localhost:8080/nifi")
                .portName("Data from Flink")
                .buildConfig();

        DataStreamSink<String> dataStream = env.fromElements("one", "two", "three", "four", "five", "q")
                .addSink(new NiFiSink<>(clientConfig, new NiFiDataPacketBuilder<String>() {
                    @Override
                    public NiFiDataPacket createNiFiDataPacket(String s, RuntimeContext ctx) {
                        return new StandardNiFiDataPacket(s.getBytes(ConfigConstants.DEFAULT_CHARSET),
                                new HashMap<String, String>());
                    }
                }));

        env.execute();
    }
}
