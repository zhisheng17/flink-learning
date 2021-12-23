package com.zhisheng.libraries.cep;

import com.zhisheng.common.utils.ExecutionEnvUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Map;

/**
 * Desc: 单个 pattern 数量
 * Created by zhisheng on 2019/10/30 上午12:48
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class IndividualPatternQuantifier {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setParallelism(1);

        DataStreamSource<String> data = env.socketTextStream("127.0.0.1", 9200);

        Pattern<String, String> pattern = Pattern.<String>begin("start")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return "a".equals(s);
                    }
                })
                .times(5).optional();

        CEP.pattern(data, pattern)
                .select(new PatternSelectFunction<String, String>() {
                    @Override
                    public String select(Map<String, List<String>> map) throws Exception {
                        log.info(map.toString());
                        return map.get("start").get(0);
                    }
                }).print();
        env.execute("flink learning cep Individual Pattern Quantifier");
    }
}
