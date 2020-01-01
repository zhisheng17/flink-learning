package com.zhisheng.alert.function;

import com.zhisheng.common.model.MetricEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Desc:
 * Created by zhisheng on 2019/10/16 下午5:24
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class AlertRuleAsyncIOFunction extends RichAsyncFunction<MetricEvent, MetricEvent> {

    PreparedStatement ps;
    private Connection connection;

    private ParameterTool parameterTool;

    @Override
    public void open(Configuration parameters) throws Exception {
        parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        connection = getConnection();
        String sql = "select * from alert_rule where name = ?;";
        if (connection != null) {
            ps = this.connection.prepareStatement(sql);
        }
    }

    @Override
    public void timeout(MetricEvent metricEvent, ResultFuture<MetricEvent> resultFuture) throws Exception {
        log.info("=================timeout======{} ", metricEvent);
    }

    @Override
    public void asyncInvoke(MetricEvent metricEvent, ResultFuture<MetricEvent> resultFuture) throws Exception {
        ps.setString(1, metricEvent.getName());
        ResultSet resultSet = ps.executeQuery();
        Map<String, Object> fields = metricEvent.getFields();
        if (resultSet.next()) {
            String thresholds = resultSet.getString("thresholds");
            String measurement = resultSet.getString("measurement");
            if (fields.get(measurement) != null && (double) fields.get(measurement) > Double.valueOf(thresholds)) {
                List<MetricEvent> list = new ArrayList<>();
                list.add(metricEvent);
                resultFuture.complete(Collections.singletonList(metricEvent));
            }
        }
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
