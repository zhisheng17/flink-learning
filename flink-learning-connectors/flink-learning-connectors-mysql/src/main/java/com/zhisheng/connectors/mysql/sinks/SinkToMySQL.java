package com.zhisheng.connectors.mysql.sinks;

import com.zhisheng.connectors.mysql.model.Student;
import com.zhisheng.connectors.mysql.utils.JdbcPoolUtil;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * Desc: 数据批量 sink 数据到 mysql
 * Created by zhisheng_tian on 2019-02-17
 * Blog: http://www.54tianzhisheng.cn/2019/01/09/Flink-MySQL-sink/
 */
public class SinkToMySQL extends RichSinkFunction<List<Student>> {
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
        connection = JdbcPoolUtil.getConnect();
        String sql = "insert into Student(id, name, password, age) values(?, ?, ?, ?);";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        //连接池不需要关闭
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
        //遍历数据集合
        for (Student student : value) {
            ps.setInt(1, student.getId());
            ps.setString(2, student.getName());
            ps.setString(3, student.getPassword());
            ps.setInt(4, student.getAge());
            ps.addBatch();
        }
        int[] count = ps.executeBatch();//批量后执行
        System.out.println("成功了插入了" + count.length + "行数据");
    }
}
