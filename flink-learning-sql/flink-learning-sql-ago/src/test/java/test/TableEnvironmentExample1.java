package test;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;

/**
 * Desc: flink old planner TableEnvironment
 * Created by zhisheng on 2019/11/3 下午2:21
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class TableEnvironmentExample1 {
    public static void main(String[] args) {
        //流作业
        StreamTableEnvironment sEnv = StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());

        Catalog catalog = new GenericInMemoryCatalog("zhisheng");
        sEnv.registerCatalog("InMemCatalog", catalog);


        //批作业
        BatchTableEnvironment bEnv = BatchTableEnvironment.create(ExecutionEnvironment.getExecutionEnvironment());

    }
}
