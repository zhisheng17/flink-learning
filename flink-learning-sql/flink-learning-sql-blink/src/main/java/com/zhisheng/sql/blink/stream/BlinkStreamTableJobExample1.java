package com.zhisheng.sql.blink.stream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;


/**
 * Desc: Blink Stream Table Job
 * Created by zhisheng on 2019/11/3 下午1:14
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class BlinkStreamTableJobExample1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment blinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        blinkStreamEnv.setParallelism(1);
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(blinkStreamEnv, blinkStreamSettings);

        String path = BlinkStreamTableJobExample1.class.getClassLoader().getResource("list.txt").getPath();
        blinkStreamTableEnv
                .connect(new FileSystem().path(path))
                .withFormat(new OldCsv().field("word", Types.STRING).lineDelimiter("\n"))
                .withSchema(new Schema().field("word", Types.STRING))
                .inAppendMode()
                .registerTableSource("FlieSourceTable");

        Table wordWithCount = blinkStreamTableEnv.scan("FlieSourceTable")
                .groupBy("word")
                .select("word,count(word) as _count");
        blinkStreamTableEnv.toRetractStream(wordWithCount, Row.class).print();

        blinkStreamTableEnv.execute("Blink Stream SQL Job");
    }
}
