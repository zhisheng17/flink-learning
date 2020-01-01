package com.zhisheng.libraries.cep;


import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.common.utils.StringUtil;
import com.zhisheng.libraries.cep.model.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class CEPMain {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> eventDataStream = env.socketTextStream("127.0.0.1", 9200)
                .flatMap(new FlatMapFunction<String, Event>() {
                    @Override
                    public void flatMap(String s, Collector<Event> collector) throws Exception {
                        if (StringUtil.isNotEmpty(s)) {
                            String[] split = s.split(",");
                            if (split.length == 2) {
                                collector.collect(new Event(Integer.valueOf(split[0]), split[1]));
                            }
                        }
                    }
                });

        //42,zhisheng
        //20,zhisheng

        Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        log.info("start {}", event.getId());
                        return event.getId() == 42;
                    }
                }
        ).next("middle").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        log.info("middle {}", event.getId());
                        return event.getId() >= 10;
                    }
                }
        );

        CEP.pattern(eventDataStream, pattern).select(new PatternSelectFunction<Event, String>() {
            @Override
            public String select(Map<String, List<Event>> p) throws Exception {
                StringBuilder builder = new StringBuilder();
                log.info("p = {}", p);
                builder.append(p.get("start").get(0).getId()).append(",").append(p.get("start").get(0).getName()).append("\n")
                        .append(p.get("middle").get(0).getId()).append(",").append(p.get("middle").get(0).getName());
                return builder.toString();
            }
        }).print();

        CEP.pattern(eventDataStream, pattern).flatSelect(new PatternFlatSelectFunction<Event, String>() {
            @Override
            public void flatSelect(Map<String, List<Event>> map, Collector<String> collector) throws Exception {
                for (Map.Entry<String, List<Event>> entry : map.entrySet()) {
                    collector.collect(entry.getKey() + " " + entry.getValue().get(0).getId() + "," + entry.getValue().get(0).getName());
                }
            }
        }).print();


        env.execute("flink learning cep");
    }
}