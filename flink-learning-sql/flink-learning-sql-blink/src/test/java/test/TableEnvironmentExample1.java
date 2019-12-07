package test;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * Desc: blink planner TableEnvironment
 * Created by zhisheng on 2019/11/3 下午2:23
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class TableEnvironmentExample1 {
    public static void main(String[] args) {
        //流作业
        StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment());
        //批作业
        BatchTableEnvironment.create(ExecutionEnvironment.getExecutionEnvironment());
        //use EnvironmentSettings
        StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment(), EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
        StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment(), EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
        //use table config
        StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment(), TableConfig.getDefault());
    }
}
