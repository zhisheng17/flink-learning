package com.zhisheng.configration.apollo;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigChangeListener;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.model.ConfigChange;
import com.ctrip.framework.apollo.model.ConfigChangeEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * Desc: flink Apollo
 * Created by zhisheng on 2020/2/23 下午7:37
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class FlinkApolloTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        env.setParallelism(1);

        env.addSource(new RichSourceFunction<String>() {

            private Config config;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                config = ConfigService.getAppConfig();
                config.addChangeListener(new ConfigChangeListener() {
                    @Override
                    public void onChange(ConfigChangeEvent configChangeEvent) {
                        for (String key : configChangeEvent.changedKeys()) {
                            ConfigChange change = configChangeEvent.getChange(key);
                            log.info("Change - key: {}, oldValue: {}, newValue: {}, changeType: {}",
                                    change.getPropertyName(), change.getOldValue(), change.getNewValue(),
                                    change.getChangeType());
                        }
                    }
                });
            }

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (true) {
                    ctx.collect(config.getProperty("name", "zhisheng"));
                    Thread.sleep(3000);
                }
            }

            @Override
            public void cancel() {
            }
        }).print();

        env.execute("zhisheng flink Apollo");
    }
}
