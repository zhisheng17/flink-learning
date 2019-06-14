package com.zhisheng.examples.streaming.sideoutput;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Desc: SideOutput
 * Created by zhisheng on 2019-06-02
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class Main {

    private static final OutputTag<String> overFiveTag = new OutputTag<String>("overFive") {
    };
    private static final OutputTag<String> equalFiveTag = new OutputTag<String>("equalFive") {
    };

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.getConfig().setGlobalJobParameters(params);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tokenizer = env.fromElements(WORDS)
                .keyBy(new KeySelector<String, Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer getKey(String value) throws Exception {
                        return 0;
                    }
                })
                .process(new Tokenizer());

//        tokenizer.getSideOutput(overFiveTag).print();   //将字符串长度大于 5 的打印出来

//        tokenizer.getSideOutput(equalFiveTag).print();  //将字符串长度等于 5 的打印出来

//        tokenizer.print();  //这个打印出来的是字符串长度小于 5 的

        tokenizer.keyBy(0)
                .sum(1)
                .print();   //做 word count 后打印出来

        env.execute("Streaming WordCount SideOutput");
    }

    public static final class Tokenizer extends ProcessFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");

            for (String token : tokens) {
                if (token.length() > 5) {
                    ctx.output(overFiveTag, token); //将字符串长度大于 5 的 word 放到 overFiveTag 中去
                } else if (token.length() == 5) {
                    ctx.output(equalFiveTag, token); //将字符串长度等于 5 的 word 放到 equalFiveTag 中去
                } else if (token.length() < 5) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }

        }
    }

    public static final String[] WORDS = new String[]{
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
