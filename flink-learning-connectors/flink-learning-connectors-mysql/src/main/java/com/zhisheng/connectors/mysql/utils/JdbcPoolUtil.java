package com.zhisheng.connectors.mysql.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;

/**
 * @author chenmengjie@buaa.edu.cn
 * @create 2019/11/25
 */
@Slf4j
public class JdbcPoolUtil {
  private static BasicDataSource dataSource;

  static {
    dataSource = new BasicDataSource();
    dataSource.setDriverClassName("com.mysql.jdbc.Driver");
    // 注意，替换成自己本地的 mysql 数据库地址和用户名、密码
    dataSource.setUrl("jdbc:mysql://localhost:3306/test");
    dataSource.setUsername("root");
    dataSource.setPassword("root123456");
    // 设置连接池的一些参数
    dataSource.setInitialSize(10);
    dataSource.setMaxTotal(50);
    dataSource.setMinIdle(2);
  }

  public static Connection getConnect() {
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
