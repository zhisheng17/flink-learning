package com.zhisheng.examples.util;

import com.google.common.base.Throwables;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;

@Slf4j
public class MySQLUtil {

    public static Connection getConnection(String driver, String url, String user, String password) {
        Connection con = null;
        try {
            Class.forName(driver);
            con = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            log.error("-----------mysql get connection has exception , msg = " + Throwables.getStackTraceAsString(e));
        }
        return con;
    }
}
