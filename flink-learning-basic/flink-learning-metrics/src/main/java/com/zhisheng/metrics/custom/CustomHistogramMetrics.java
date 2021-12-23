package com.zhisheng.metrics.custom;

import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Desc: custom Histogram
 * Created by zhisheng on 2019-11-16 19:08
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class CustomHistogramMetrics {
    public static void main(String[] args) throws Exception {
        //创建流运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        env.setParallelism(1);
        env.addSource(new SourceFunction<Long>() {
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<Long> out) throws Exception {
                while (isRunning) {
                    out.collect(Long.valueOf(Math.round(Math.random() * 100)));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        }).map(new RichMapFunction<Long, Long>() {
            Histogram histogram;
            int index;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                com.codahale.metrics.Histogram dropwizardHistogram =
                        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));
                index = getRuntimeContext().getIndexOfThisSubtask() + 1;
                histogram = getRuntimeContext().getMetricGroup()
                        .addGroup("flink-metrics-test")
                        .histogram("histogramTest", new DropwizardHistogramWrapper(dropwizardHistogram));
            }

            @Override
            public Long map(Long s) throws Exception {
                histogram.update(s);
                System.out.println("index = " + " count = " + histogram.getCount() + " max= " + histogram.getStatistics().getMax() + " min = " + histogram.getStatistics().getMin() + " mean = " + histogram.getStatistics().getMean() + " 75% = " + histogram.getStatistics().getQuantile(0.75));
                return s;
            }
        }).print();

        env.execute("Flink custom Histogram Metrics");
    }
}
