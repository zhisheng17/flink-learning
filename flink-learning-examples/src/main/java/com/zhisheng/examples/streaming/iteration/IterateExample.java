package com.zhisheng.examples.streaming.iteration;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Example illustrating iterations in Flink streaming.
 * <p> The program sums up random numbers and counts additions
 * it performs to reach a specific threshold in an iterative streaming fashion. </p>
 *
 * <p>
 * This example shows how to use:
 * <ul>
 * <li>streaming iterations,
 * <li>buffer timeout to enhance latency,
 * <li>directed outputs.
 * </ul>
 * </p>
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class IterateExample {

    private static final int BOUND = 100;

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setBufferTimeout(1);
        env.getConfig().setGlobalJobParameters(params);



        IterativeStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> it = env.addSource(new RandomFibonacciSource())
                .map(new InputMap())
                .iterate(5000);

        SplitStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> step = it.map(new Step())
                .split(new MySelector());

        it.closeWith(step.select("iterate"));

        step.select("output")
                .map(new OutputMap())
                .print();

        env.execute("Streaming Iteration Example");
    }

    /**
     * Generate BOUND number of random integer pairs from the range from 1 to BOUND/2.
     */
    private static class RandomFibonacciSource implements SourceFunction<Tuple2<Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        private Random rnd = new Random();

        private volatile boolean isRunning = true;
        private int counter = 0;

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {

            while (isRunning && counter < BOUND) {
                int first = rnd.nextInt(BOUND / 2 - 1) + 1;
                int second = rnd.nextInt(BOUND / 2 - 1) + 1;

                ctx.collect(new Tuple2<>(first, second));
                counter++;
                Thread.sleep(50L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    /**
     * Map the inputs so that the next Fibonacci numbers can be calculated while preserving the original input tuple.
     * A counter is attached to the tuple and incremented in every iteration step.
     */
    public static class InputMap implements MapFunction<Tuple2<Integer, Integer>, Tuple5<Integer, Integer, Integer, Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
            return new Tuple5<>(value.f0, value.f1, value.f0, value.f1, 0);
        }
    }

    /**
     * Iteration step function that calculates the next Fibonacci number.
     */
    public static class Step implements MapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple5<Integer, Integer, Integer, Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple5<Integer, Integer, Integer, Integer, Integer> value) throws Exception {
            return new Tuple5<>(value.f0, value.f1, value.f3, value.f2 + value.f3, ++value.f4);
        }
    }

    /**
     * OutputSelector testing which tuple needs to be iterated again.
     */
    public static class MySelector implements OutputSelector<Tuple5<Integer, Integer, Integer, Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Iterable<String> select(Tuple5<Integer, Integer, Integer, Integer, Integer> value) {
            List<String> output = new ArrayList<>();
            if (value.f2 < BOUND && value.f3 < BOUND) {
                output.add("iterate");
            } else {
                output.add("output");
            }
            return output;
        }
    }

    /**
     * Giving back the input pair and the counter.
     */
    public static class OutputMap implements MapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<Tuple2<Integer, Integer>, Integer> map(Tuple5<Integer, Integer, Integer, Integer, Integer> value) throws Exception {
            return new Tuple2<>(new Tuple2<>(value.f0, value.f1), value.f4);
        }
    }

}
