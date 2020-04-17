package com.zhisheng.sql.blink.stream.catalog;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * Desc: Catalog API
 * Created by zhisheng on 2020-01-26 21:45
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class CatalogAPI {
    public static void main(String[] args) {
        StreamExecutionEnvironment blinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        blinkStreamEnv.setParallelism(1);
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(blinkStreamEnv, blinkStreamSettings);

        //Changing the Current Catalog And Database
        blinkStreamTableEnv.useCatalog("zhisheng");
        blinkStreamTableEnv.useDatabase("zhisheng");

        blinkStreamTableEnv.scan("not_the_current_catalog", "not_the_current_db", "zhisheng");

        //List Available Catalogs/Databases/Tables
        blinkStreamTableEnv.listCatalogs();
        blinkStreamTableEnv.listDatabases();
        blinkStreamTableEnv.listTables();

    }
}
