package com.zhisheng.data.sinks.sinks;

import com.zhisheng.data.sinks.model.Student;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * Desc: sink 数据到 mysql
 * Created by yuanblog_tzs on 2019-02-17
 * Blog: http://www.54tianzhisheng.cn/tags/Flink/
 */
@Slf4j
public class SinkToMySQL extends RichSinkFunction<Student> {
    PreparedStatement ps;
    private Connection connection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "insert into Student(id, name, password, age) values(?, ?, ?, ?);";
        if (connection != null) {
            ps = this.connection.prepareStatement(sql);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(Student value, Context context) throws Exception {
        if (ps == null) {
            return;
        }
        //组装数据，执行插入操作
        ps.setInt(1, value.getId());
        ps.setString(2, value.getName());
        ps.setString(3, value.getPassword());
        ps.setInt(4, value.getAge());
        ps.executeUpdate();
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
