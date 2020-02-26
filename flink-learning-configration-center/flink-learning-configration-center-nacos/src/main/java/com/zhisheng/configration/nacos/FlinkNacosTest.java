package com.zhisheng.configration.nacos;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Desc: flink nacos 整合测试
 * Created by zhisheng on 2020/2/16 下午5:45
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class FlinkNacosTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        env.setParallelism(1);

        String serverAddr = "localhost";
        String dataId = "test";
        String group = "DEFAULT_GROUP";
        Properties properties = new Properties();
        properties.put("serverAddr", serverAddr);
        ConfigService configService = NacosFactory.createConfigService(properties);
        String content = configService.getConfig(dataId, group, 5000);
        System.out.println("main " + content);

        env.addSource(new RichSourceFunction<String>() {
            ConfigService configService;
            String config;
            String dataId = "test";
            String group = "DEFAULT_GROUP";

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                String serverAddr = "localhost";
                Properties properties = new Properties();
                properties.put("serverAddr", serverAddr);
                configService = NacosFactory.createConfigService(properties);
                config = configService.getConfig(dataId, group, 5000);
                configService.addListener(dataId, group, new Listener() {
                    @Override
                    public Executor getExecutor() {
                        return null;
                    }

                    @Override
                    public void receiveConfigInfo(String configInfo) {
                        config = configInfo;
                        System.out.println("open Listener receive : " + configInfo);
                    }
                });
            }

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
//                config = configService.getConfig(dataId, group, 5000);
                while (true) {
                    Thread.sleep(3000);
                    System.out.println("run config = " + config);
                    ctx.collect(String.valueOf(System.currentTimeMillis()));
                }
            }

            @Override
            public void cancel() {

            }
        }).print();


        env.execute("zhisheng flink nacos");
    }
}
