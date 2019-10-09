package com.zhisheng.examples.streaming.config;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * Desc: ParameterTool get SystemProperties config
 * Created by zhisheng on 2019/10/9 下午8:50
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class ParameterToolGetPropertiesMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromPropertiesFile(ParameterToolGetPropertiesMain.class.getResourceAsStream("/application.properties")));
        env.addSource(new RichSourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (true) {
                    ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                    sourceContext.collect(System.currentTimeMillis() + parameterTool.get("metrics.topic"));
                }
            }

            @Override
            public void cancel() {
            }
        }).print();

        env.execute("ParameterTool Get config from SystemProperties");
    }
}
