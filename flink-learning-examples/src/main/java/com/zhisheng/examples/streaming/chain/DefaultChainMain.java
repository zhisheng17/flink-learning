package com.zhisheng.examples.streaming.chain;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Desc: operator chain
 * Created by zhisheng on 2019/10/6 下午7:42
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class DefaultChainMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));

        env.fromElements(WORDS)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] splits = value.toLowerCase().split("\\W+");

                        for (String split : splits) {
                            if (split.length() > 0) {
                                out.collect(new Tuple2<>(split, 1));
                            }
                        }
                    }
                })
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .print();

        env.execute("zhisheng —— word count chain demo");
    }

    private static final String[] WORDS = new String[]{
            "To be, or not to be,--that is the question:--",
            "Whether 'tis nobler in the mind to suffer",
            "The slings and arrows of outrageous fortune",
            "Or to take arms against a sea of troubles,",
            "And by opposing end them?--To die,--to sleep,--",
            "No more; and by a sleep to say we end",
            "The heartache, and the thousand natural shocks",
            "That flesh is heir to,--'tis a consummation",
            "Devoutly to be wish'd. To die,--to sleep;--",
            "To sleep! perchance to dream:--ay, there's the rub;",
            "For in that sleep of death what dreams may come,",
            "When we have shuffled off this mortal coil,",
            "Must give us pause: there's the respect",
            "That makes calamity of so long life;",
            "For who would bear the whips and scorns of time,",
            "The oppressor's wrong, the proud man's contumely,",
            "The pangs of despis'd love, the law's delay,",
            "The insolence of office, and the spurns",
            "That patient merit of the unworthy takes,",
            "When he himself might his quietus make",
            "With a bare bodkin? who would these fardels bear,",
            "To grunt and sweat under a weary life,",
            "But that the dread of something after death,--",
            "The undiscover'd country, from whose bourn",
            "No traveller returns,--puzzles the will,",
            "And makes us rather bear those ills we have",
            "Than fly to others that we know not of?",
            "Thus conscience does make cowards of us all;",
            "And thus the native hue of resolution",
            "Is sicklied o'er with the pale cast of thought;",
            "And enterprises of great pith and moment,",
            "With this regard, their currents turn awry,",
            "And lose the name of action.--Soft you now!",
            "The fair Ophelia!--Nymph, in thy orisons",
            "Be all my sins remember'd."
    };
}
