package com.zhisheng.data.sources;

import com.zhisheng.common.model.MetricEvent;
import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.common.utils.KafkaConfigUtil;
import com.zhisheng.data.sources.model.Rule;
import com.zhisheng.data.sources.utils.MySQLUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Desc: 定时捞取告警规则
 * Created by zhisheng on 2019-05-24
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class ScheduleMain {

    public static List<Rule> rules;

    public static void main(String[] args) throws Exception {
        //定时捞取规则，每隔一分钟捞一次
        ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(1);
        threadPool.scheduleAtFixedRate(new GetRulesJob(), 0, 1, TimeUnit.MINUTES);

        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);

        DataStreamSource<MetricEvent> source = KafkaConfigUtil.buildSource(env);
        source.map(new MapFunction<MetricEvent, MetricEvent>() {
            @Override
            public MetricEvent map(MetricEvent value) throws Exception {
                if (rules.size() <= 2) {
                    System.out.println("===========2");
                } else {
                    System.out.println("===========3");
                }
                return value;
            }
        }).print();

        env.execute("schedule");
    }


    static class GetRulesJob implements Runnable {
        @Override
        public void run() {
            try {
                rules = getRules();
            } catch (SQLException e) {
                log.error("get rules from mysql has an error {}", e.getMessage());
            }
        }
    }


    private static List<Rule> getRules() throws SQLException {
        System.out.println("-----get rule");
        String sql = "select * from rule";

        Connection connection = MySQLUtil.getConnection("com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8",
                "root",
                "root123456");

        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();

        List<Rule> list = new ArrayList<>();
        while (resultSet.next()) {
            list.add(Rule.builder()
                    .id(resultSet.getString("id"))
                    .name(resultSet.getString("name"))
                    .type(resultSet.getString("type"))
                    .measurement(resultSet.getString("measurement"))
                    .threshold(resultSet.getString("threshold"))
                    .level(resultSet.getString("level"))
                    .targetType(resultSet.getString("target_type"))
                    .targetId(resultSet.getString("target_id"))
                    .webhook(resultSet.getString("webhook"))
                    .build()
            );
        }

        return list;
    }
}
