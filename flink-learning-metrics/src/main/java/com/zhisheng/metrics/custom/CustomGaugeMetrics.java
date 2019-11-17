package com.zhisheng.metrics.custom;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Desc: custom Gauge
 * Created by zhisheng on 2019-11-16 19:08
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class CustomGaugeMetrics {
    public static void main(String[] args) throws Exception {
        //创建流运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));

        env.addSource(new SourceFunction<String>() {
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<String> out) throws Exception {
                while (isRunning) {
                    out.collect(String.valueOf(Math.round(Math.random() * 100)));
                    Thread.sleep(15000);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }).map(new RichMapFunction<String, String>() {
            private transient int value = 0;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().getMetricGroup()
                        .addGroup("flink-metrics-test")
                        .gauge("gaugeTest", new Gauge<Integer>() {
                            @Override
                            public Integer getValue() {
                                return value;
                            }
                        });
            }

            @Override
            public String map(String s) throws Exception {
                value++;
                return s;
            }
        }).print();

        env.execute("Flink custom Gauge Metrics");
    }
}
