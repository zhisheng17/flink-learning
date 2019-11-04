package com.zhisheng.sql.ago.sql;

import com.zhisheng.sql.ago.model.WC;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;


/**
 * Desc: Convert DataSets to Tables(Use Batch SQL API)
 * Created by zhisheng on 2019-06-02
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class WordCountSQL {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSet<WC> input = env.fromElements(
                new WC("Hello", 1),
                new WC("zhisheng", 1),
                new WC("Hello", 1));

        tEnv.registerDataSet("WordCount", input, "word, count");

        Table table = tEnv.sqlQuery(
                "SELECT word, SUM(count) as count FROM WordCount GROUP BY word");

        DataSet<WC> result = tEnv.toDataSet(table, WC.class);

        result.print();
    }
}
