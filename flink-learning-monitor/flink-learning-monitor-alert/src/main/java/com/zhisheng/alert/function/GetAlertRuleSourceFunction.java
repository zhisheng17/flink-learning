package com.zhisheng.alert.function;

import com.zhisheng.alert.model.AlertRule;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Desc:
 * Created by zhisheng on 2019/10/17 下午4:47
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class GetAlertRuleSourceFunction extends RichSourceFunction<List<AlertRule>> {

    private PreparedStatement ps;
    private Connection connection;
    private volatile boolean isRunning = true;

    private ParameterTool parameterTool;

    @Override
    public void open(Configuration parameters) throws Exception {
        parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        connection = getConnection();
        String sql = "select * from alert_rule;";
        if (connection != null) {
            ps = this.connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<List<AlertRule>> ctx) throws Exception {
        List<AlertRule> list = new ArrayList<>();
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                AlertRule alertRule = new AlertRule().builder()
                        .id(resultSet.getInt("id"))
                        .name(resultSet.getString("name"))
                        .measurement(resultSet.getString("measurement"))
                        .thresholds(resultSet.getString("thresholds"))
                        .build();
                list.add(alertRule);
            }
            log.info("=======select alarm notify from mysql, size = {}, map = {}", list.size(), list);

            ctx.collect(list);
            list.clear();
            Thread.sleep(1000 * 60);
        }
    }

    @Override
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            log.error("runException:{}", e);
        }
        isRunning = false;
    }

    private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8", "root", "root123456");
        } catch (Exception e) {
            log.error("-----------mysql get connection has exception , msg = {}", e.getMessage());
        }
        return con;
    }
}