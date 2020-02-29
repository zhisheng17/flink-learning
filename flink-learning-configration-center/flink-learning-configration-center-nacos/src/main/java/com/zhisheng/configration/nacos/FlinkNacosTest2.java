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
 * Desc: 测试 nacos 动态更改 Checkpoint 配置后，Flink 是否可以获取到更改后的值，并生效?
 *
 * 结论是：不生效，因为 Flink 是 Lazy Evaluation（延迟执行），当程序的 main 方法执行时，数据源加载数据和数据转换等算子不会立马执行，
 * 这些操作会被创建并添加到程序的执行计划中去，只有当执行环境 env 的 execute 方法被显示地触发执行时，整个程序才开始执行实际的操作，所以
 * 在一开始初始化后等程序执行 execute 方法后再修改 env 的配置其实就不起作用了。
 *
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
                System.out.println("----------");
                System.out.println(env.getCheckpointConfig().getCheckpointInterval());
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
