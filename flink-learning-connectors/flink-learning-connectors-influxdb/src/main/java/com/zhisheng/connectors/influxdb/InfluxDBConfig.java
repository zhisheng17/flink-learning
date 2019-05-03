package com.zhisheng.connectors.influxdb;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * Desc: InfluxDB 配置
 * Created by zhisheng on 2019-05-01
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InfluxDBConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 默认每次批处理的数据条数为 2000 条
     */
    private static final int DEFAULT_BATCH_ACTIONS = 2000;

    /**
     * 默认每隔 100
     */
    private static final int DEFAULT_FLUSH_DURATION = 100;

    /**
     * 数据库地址
     */
    private String url;

    /**
     * 数据库用户名
     */
    private String username;

    /**
     * 数据库密码
     */
    private String password;

    /**
     * 数据库名
     */
    private String database;

    /**
     * batch
     */
    private int batchActions = DEFAULT_BATCH_ACTIONS;

    /**
     * flush duration
     */
    private int flushDuration = DEFAULT_FLUSH_DURATION;

    /**
     * 单位
     */
    private TimeUnit flushDurationTimeUnit = TimeUnit.MILLISECONDS;

    /**
     * 是否开启 GZIP 压缩
     */
    private boolean enableGzip = false;

    /**
     * 是否创建数据库
     */
    private boolean createDatabase = false;
}
