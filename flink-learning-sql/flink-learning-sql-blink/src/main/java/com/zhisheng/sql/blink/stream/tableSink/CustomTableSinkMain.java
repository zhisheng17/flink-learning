package com.zhisheng.sql.blink.stream.tableSink;

import com.zhisheng.sql.blink.stream.example.SQLExampleWordCount;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

/**
 * Desc: custom table sink
 * Created by zhisheng on 2020-01-10 20:54
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class CustomTableSinkMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment blinkStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        blinkStreamEnv.setParallelism(1);
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(blinkStreamEnv, blinkStreamSettings);

        String path = SQLExampleWordCount.class.getClassLoader().getResource("words.txt").getPath();

        CsvTableSource csvTableSource = CsvTableSource.builder()
                .field("word", Types.STRING)
                .path(path)
                .build();
        blinkStreamTableEnv.registerTableSource("zhisheng", csvTableSource);

        RetractStreamTableSink<Row> retractStreamTableSink = new MyRetractStreamTableSink(new String[]{"c", "word"}, new TypeInformation[]{Types.LONG, Types.STRING});
        //或者
//        RetractStreamTableSink<Row> retractStreamTableSink = new MyRetractStreamTableSink(new String[]{"c", "word"}, new DataType[]{DataTypes.BIGINT(), DataTypes.STRING()});
        blinkStreamTableEnv.registerTableSink("sinkTable", retractStreamTableSink);

        Table wordWithCount = blinkStreamTableEnv.sqlQuery("SELECT count(word) AS c, word FROM zhisheng GROUP BY word");

        wordWithCount.insertInto("sinkTable");
        blinkStreamTableEnv.execute("Blink Custom Table Sink");
    }
}
