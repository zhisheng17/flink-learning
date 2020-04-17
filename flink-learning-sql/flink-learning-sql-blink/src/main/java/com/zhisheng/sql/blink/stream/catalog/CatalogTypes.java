package com.zhisheng.sql.blink.stream.catalog;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * Desc: Catalog Types
 * Created by zhisheng on 2020-01-26 21:30
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class CatalogTypes {
    public static void main(String[] args) {
        StreamExecutionEnvironment blinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        blinkStreamEnv.setParallelism(1);
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(blinkStreamEnv, blinkStreamSettings);

        blinkStreamTableEnv.registerCatalog("zhisheng", new GenericInMemoryCatalog("zhisheng"));
        //GenericInMemoryCatalog，默认的 catalog


        //HiveCatalog，这个需要添加 Hive connector 和 Hive 的依赖
//        blinkStreamTableEnv.registerCatalog("zhisheng", new HiveCatalog("zhisheng", "zhisheng", "~/zhisheng/hive/conf", "2.3.4"));


    }
}
