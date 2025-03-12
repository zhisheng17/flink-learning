package com.zhisheng.sql.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class Config {

    protected static final Logger LOG = LoggerFactory.getLogger(Config.class);

    private static Properties p = new Properties();
    private static String paramsFile = "/conf.properties";

    static {
        try {
            p.load(Config.class.getResourceAsStream(paramsFile));
        } catch (IOException e) {
            LOG.error("Config读取配置出错：" + e.getMessage());
        }
    }

    public static String getString(String key) {
        return p.getProperty(key);
    }

}
