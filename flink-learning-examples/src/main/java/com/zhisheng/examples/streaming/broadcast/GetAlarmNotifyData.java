package com.zhisheng.examples.streaming.broadcast;


import com.zhisheng.examples.util.MySQLUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import static com.zhisheng.common.constant.PropertiesConstants.*;

@Slf4j
public class GetAlarmNotifyData extends RichSourceFunction<Map<String, String>> {

    private Connection connection = null;
    private PreparedStatement ps = null;
    private volatile boolean isRunning = true;
    private ParameterTool parameterTool;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());

        String database = parameterTool.get(MYSQL_DATABASE);
        String host = parameterTool.get(MYSQL_HOST);
        String password = parameterTool.get(MYSQL_PASSWORD);
        String port = parameterTool.get(MYSQL_PORT);
        String username = parameterTool.get(MYSQL_USERNAME);

        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=UTF-8";
        connection = MySQLUtil.getConnection(driver, url, username, password);

        if (connection != null) {
            String sql = "select * from alarm_notify";
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<Map<String, String>> ctx) throws Exception {
        Map<String, String> map = new HashMap<>();
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                if (0 == resultSet.getInt("status")) {
                    map.put(resultSet.getString("type") + resultSet.getString("type_id"), resultSet.getString("target_id"));
                }
            }
            log.info("=======select alarm notify from mysql, size = {}, map = {}", map.size(), map);

            ctx.collect(map);
            map.clear();
            Thread.sleep(2000 * 60);
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
}
