package com.zhisheng.connectors.mysql.sinks;

import com.zhisheng.connectors.mysql.model.Student;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * Desc: 数据批量 sink 数据到 mysql
 * Created by yuanblog_tzs on 2019-02-17
 * Blog: http://www.54tianzhisheng.cn/2019/01/09/Flink-MySQL-sink/
 */
@Slf4j
public class SinkToMySQL extends RichSinkFunction<List<Student>> {
    PreparedStatement ps;
    BasicDataSource dataSource;
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
        dataSource = new BasicDataSource();
        connection = getConnection(dataSource);
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
    public void invoke(List<Student> value, Context context) throws Exception {
        if (ps == null) {
            return;
        }
        //遍历数据集合
        for (Student student : value) {
            ps.setInt(1, student.getId());
            ps.setString(2, student.getName());
            ps.setString(3, student.getPassword());
            ps.setInt(4, student.getAge());
            ps.addBatch();
        }
        int[] count = ps.executeBatch();//批量后执行
        log.info("成功了插入了 {} 行数据", count.length);
    }


    private static Connection getConnection(BasicDataSource dataSource) {
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
        dataSource.setUrl("jdbc:mysql://localhost:3306/test");
        dataSource.setUsername("root");
        dataSource.setPassword("root123456");
        //设置连接池的一些参数
        dataSource.setInitialSize(10);
        dataSource.setMaxTotal(50);
        dataSource.setMinIdle(2);

        Connection con = null;
        try {
            con = dataSource.getConnection();
            log.info("创建连接池：{}", con);
        } catch (Exception e) {
            log.error("-----------mysql get connection has exception , msg = {}", e.getMessage());
        }
        return con;
    }
}
