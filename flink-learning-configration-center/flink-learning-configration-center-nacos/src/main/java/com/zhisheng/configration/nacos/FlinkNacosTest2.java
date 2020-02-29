package com.zhisheng.configration.nacos;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Desc: 测试 nacos 动态更改 Checkpoint 配置后，Flink 是否可以获取到更改后的值，并生效
 * Created by zhisheng on 2020/2/29 下午3:27
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class FlinkNacosTest2 {
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
        final String[] content = {configService.getConfig(dataId, group, 5000)};
        configService.addListener(dataId, group, new Listener() {
            @Override
            public Executor getExecutor() {
                return null;
            }

            @Override
            public void receiveConfigInfo(String configInfo) {
                System.out.println("===============");
                content[0] = configInfo;
                env.getCheckpointConfig().setCheckpointInterval(Long.valueOf(content[0]));
            }
        });
        System.out.println(content[0]);

        env.getCheckpointConfig().setCheckpointInterval(Long.valueOf(content[0]));
        env.getCheckpointConfig().setCheckpointTimeout(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        env.addSource(new SourceFunction<Tuple2<String, Long>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
                while (true) {
                    ctx.collect(new Tuple2<>("zhisheng", System.currentTimeMillis()));
                    Thread.sleep(800);
                }
            }

            @Override
            public void cancel() {

            }
        }).print();


        env.execute();
    }
}
