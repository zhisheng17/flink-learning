package com.zhisheng.metrics.custom;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Desc: custom Counter
 * Created by zhisheng on 2019-11-16 19:08
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class CustomCounterMetrics {
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
                    Thread.sleep(10000);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }).map(new RichMapFunction<String, String>() {
            Counter counter1;
            Counter counter2;
            Counter counter3;
            int index;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                index = getRuntimeContext().getIndexOfThisSubtask();
                counter1 = getRuntimeContext().getMetricGroup()
                        .addGroup("flink-metrics-test")
                        .counter("mapTest" + index);
                counter2 = getRuntimeContext().getMetricGroup()
                        .addGroup("flink-metrics-test")
                        .counter("filterTest" + index);
                counter3 = getRuntimeContext().getMetricGroup()
                        .addGroup("flink-metrics-test")
                        .counter("mapCounter", new SimpleCounter());
            }

            @Override
            public String map(String s) throws Exception {
                System.out.println("index = " + (index + 1) + " counter1 = " + counter1.getCount() + " counter2 = " + counter2.getCount());
                counter1.inc();
                counter3.inc();
                if ("50".equals(s) || "20".equals(s)) {
                    counter2.inc();
                }
                return s;
            }
        }).print();


        env.execute("Flink custom Counter Metrics");
    }
}
